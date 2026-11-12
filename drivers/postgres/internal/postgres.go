package driver

import (
	"context"
	"crypto/tls"
	"encoding/hex"
	"fmt"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/drivers/postgres/internal/waljs"
	"github.com/datazip-inc/olake/pkg/jdbc"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/datazip-inc/olake/utils/typeutils"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"golang.org/x/crypto/ssh"
)

const (
	// minCDCInitialWaitTime is the minimum wait time in seconds for CDC sync.
	minCDCInitialWaitTime = 120
	// defaultCDCInitialWaitTime is the default wait time in seconds for CDC sync.
	defaultCDCInitialWaitTime = 10800
)

const (
	// get all schemas and table
	getPrivilegedTablesTmpl = `SELECT nspname as table_schema,
		relname as table_name
		FROM pg_class c
		JOIN pg_namespace n ON c.relnamespace = n.oid
		WHERE has_table_privilege(c.oid, 'SELECT')
		AND has_schema_privilege(current_user, nspname, 'USAGE')
		AND relkind IN ('r', 'm', 't', 'f', 'p')
		AND nspname NOT LIKE 'pg_%'  -- Exclude default system schemas
		AND nspname != 'information_schema'` // Exclude information_schema

	// extends getPrivilegedTablesTmpl to restrict to user-specified schemas
	getPrivilegedTablesFilteredTmpl = getPrivilegedTablesTmpl + `
		AND nspname = ANY($1)`
	// get table schema
	getTableSchemaTmpl = `SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2 ORDER BY ordinal_position`
	// get primary key columns, query copied from pgjdbc's PgDatabaseMetaData.getPrimaryKeys() with the always-NULL TABLE_CAT column omitted
	// ref: https://github.com/pgjdbc/pgjdbc/blob/master/pgjdbc/src/main/java/org/postgresql/jdbc/PgDatabaseMetaData.java#L2134
	getTablePrimaryKey = `SELECT result.column_name
		FROM (
			SELECT n.nspname AS table_schema,
				ct.relname AS table_name,
				a.attname  AS column_name,
				(information_schema._pg_expandarray(i.indkey)).n AS key_seq,
				information_schema._pg_expandarray(i.indkey)     AS keys,
				a.attnum AS a_attnum
			FROM pg_catalog.pg_class ct
			JOIN pg_catalog.pg_attribute a ON (ct.oid = a.attrelid)
			JOIN pg_catalog.pg_namespace n ON (ct.relnamespace = n.oid)
			JOIN pg_catalog.pg_index i     ON (a.attrelid = i.indrelid)
			JOIN pg_catalog.pg_class ci    ON (ci.oid = i.indexrelid)
			WHERE i.indisprimary
		) result
		WHERE result.table_schema = $1
		  AND result.table_name   = $2
		  AND result.a_attnum     = (result.keys).x
		ORDER BY result.key_seq`
)

type Postgres struct {
	client      *sqlx.DB
	version     int // server_version_num of the connected server (e.g. 140005), read in Setup
	sizer       ctidSizer
	sshClient   *ssh.Client
	config      *Config // postgres driver connection config
	CDCSupport  bool    // indicates if the Postgres instance supports CDC
	cdcConfig   CDC
	replicator  waljs.Replicator
	state       *types.State // reference to globally present state
	streams     []types.StreamInterface
	typeMapping map[string]types.DataType
	// prerequisites holds the CDC setup checks evaluated in Setup; when the config selects CDC,
	// unmet required checks fail Setup via abstract.RequireCDCPrerequisites.
	prerequisites    types.PrerequisiteResults
	enrollmentIssue  error
	enrollmentTables []pubTable
	// backfillClient serves chunk planning and chunk scans: the BackfillHost reader when one is
	// configured, otherwise the same pool as client.
	backfillClient *sqlx.DB
	// backfillBarrier is set only with a BackfillHost and holds copies until the reader has caught up.
	backfillBarrier *readerBarrier
	backfillMu      sync.Mutex
	backfillInit    *backfillInitialization
	backfillClosed  bool
	poolOpener      func(context.Context, *url.URL, *tls.Config) (*sqlx.DB, error)
}

func (p *Postgres) CDCSupported() bool {
	return p.CDCSupport
}

func (p *Postgres) Setup(ctx context.Context) error {
	return p.setup(ctx, true)
}

func (p *Postgres) setup(ctx context.Context, requireCDCPrerequisites bool) error {
	err := p.config.Validate()
	if err != nil {
		return fmt.Errorf("failed to validate config: %w", err)
	}
	p.typeMapping = pgTypeToDataTypes()

	if p.config.SSHConfig != nil && p.config.SSHConfig.Host != "" {
		logger.Info("Found SSH Configuration")
		p.sshClient, err = p.config.SSHConfig.SetupSSHConnection()
		if err != nil {
			return fmt.Errorf("failed to setup SSH connection: %w", err)
		}
	}

	tlsConfig, err := p.config.buildTLSConfig()
	if err != nil {
		return fmt.Errorf("failed to build tls config: %w", err)
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	pgClient, err := p.openPool(ctx, p.config.Connection, tlsConfig)
	if err != nil {
		return err
	}
	p.client = pgClient
	if p.config.BackfillConnection == nil {
		p.backfillClient = pgClient
	}
	p.version, err = serverVersionNum(ctx, pgClient)
	if err != nil {
		return fmt.Errorf("failed to detect postgres server version: %w", err)
	}
	p.sizer = newCTIDSizer(p.version)
	logger.Infof("Connected to Postgres server_version_num=%d", p.version)
	// TODO: correct cdc setup
	found, _ := utils.IsOfType(p.config.UpdateMethod, "replication_slot")
	if found {
		logger.Info("Found CDC Configuration")
		cdc := &CDC{}
		if err := utils.Unmarshal(p.config.UpdateMethod, cdc); err != nil {
			return err
		}

		if cdc.InitialWaitTime < minCDCInitialWaitTime {
			logger.Warnf("initial_wait_time %d is below the minimum of %d seconds; using default %d", cdc.InitialWaitTime, minCDCInitialWaitTime, defaultCDCInitialWaitTime)
			cdc.InitialWaitTime = defaultCDCInitialWaitTime
		}

		// Normal sync/check require all CDC prerequisites. Enrollment reports missing
		// publication/slot coverage per candidate instead, without starting replication.
		if requireCDCPrerequisites {
			p.prerequisites = abstract.RunPrerequisites(ctx, p.prerequisiteChecks(cdc))
			if err := abstract.RequireCDCPrerequisites(p.Type(), p.prerequisites); err != nil {
				return err
			}
		}
		p.CDCSupport = true
		p.cdcConfig = *cdc
	} else {
		logger.Info("Standard Replication is selected")
	}
	return nil
}

// openPool connects to one source instance with the shared credentials, TLS settings and SSH tunnel.
func (p *Postgres) openPool(ctx context.Context, connection *url.URL, tlsConfig *tls.Config) (*sqlx.DB, error) {
	if p.poolOpener != nil {
		return p.poolOpener(ctx, connection, tlsConfig)
	}
	pgCfg, err := pgx.ParseConfig(connection.String())
	if err != nil {
		return nil, fmt.Errorf("failed to parse postgres connection string: %w", err)
	}
	if tlsConfig != nil {
		pgCfg.TLSConfig = tlsConfig
	}

	if p.sshClient != nil {
		logger.Info("Connecting to Postgres via SSH tunnel")
		// Allows pgx to use the SSH client to connect to the database
		pgCfg.DialFunc = func(_ context.Context, _, addr string) (net.Conn, error) {
			return p.sshClient.Dial("tcp", addr)
		}
	}

	sqlxDB := sqlx.NewDb(stdlib.OpenDB(*pgCfg), "pgx")
	sqlxDB.SetMaxOpenConns(p.config.MaxThreads)
	pgClient := sqlxDB.Unsafe()

	// force a connection and test that it worked
	if err := pgClient.PingContext(ctx); err != nil {
		if closeErr := pgClient.Close(); closeErr != nil {
			logger.Warnf("failed to close unreachable postgres pool: %s", closeErr)
		}
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}
	return pgClient, nil
}

// serverVersionNum returns the server's server_version_num, e.g. 140005 for PG 14.5.
func serverVersionNum(ctx context.Context, db *sqlx.DB) (int, error) {
	var version int
	err := db.QueryRowContext(ctx, jdbc.PostgresServerVersionNum()).Scan(&version)
	return version, err
}

func (p *Postgres) StateType() types.StateType {
	return types.GlobalType
}

func (p *Postgres) SetupState(state *types.State) {
	p.state = state
}

func (p *Postgres) GetConfigRef() abstract.Config {
	p.config = &Config{}

	return p.config
}

func (p *Postgres) Spec() any {
	return Config{}
}

// SupportedUpdateTypes excludes equality when preserve_toast_values is on: unchanged TOAST values
// are read back through the destination's table index, which only pos and dv maintain. Discover
// calls it before Setup, so it reads update_method directly, as Setup does: without a
// replication_slot it is not CDC and nothing is recovered.
func (p *Postgres) SupportedUpdateTypes() []types.UpdateType {
	cdc := &CDC{}
	if err := utils.Unmarshal(p.config.UpdateMethod, cdc); err != nil || cdc.ReplicationSlot == "" || !cdc.PreserveToastValues {
		return nil
	}
	return []types.UpdateType{types.UpdateTypePosition, types.UpdateTypeDeletionVector}
}

func (p *Postgres) CloseConnection() {
	p.backfillMu.Lock()
	p.backfillClosed = true
	reader := p.backfillClient
	if p.backfillInit != nil {
		p.backfillInit.cancel()
	}
	p.backfillMu.Unlock()
	if reader != nil && reader != p.client {
		if err := reader.Close(); err != nil {
			logger.Errorf("failed to close connection with backfill host: %s", err)
		}
	}

	if p.client != nil {
		err := p.client.Close()
		if err != nil {
			logger.Error("failed to close connection with postgres: %s", err)
		}
	}

	if p.sshClient != nil {
		err := p.sshClient.Close()
		if err != nil {
			logger.Error("failed to close SSH connection: %s", err)
		}
	}
}

func (p *Postgres) GetStreamNames(ctx context.Context) ([]types.StreamID, error) {
	logger.Infof("Starting discover for Postgres database %s", p.config.Database)

	var (
		tableNamesOutput []Table
		err              error
	)

	if len(p.config.Schemas) > 0 {
		logger.Infof("Schema filter applied, discovering only schemas: %v", p.config.Schemas)
		err = p.client.SelectContext(ctx, &tableNamesOutput, getPrivilegedTablesFilteredTmpl, pq.Array(p.config.Schemas))
	} else {
		err = p.client.SelectContext(ctx, &tableNamesOutput, getPrivilegedTablesTmpl)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to retrieve table names: %w", err)
	}

	tablesNames := make([]types.StreamID, 0, len(tableNamesOutput))
	for _, table := range tableNamesOutput {
		tablesNames = append(tablesNames, types.StreamID{Namespace: table.Schema, Name: table.Name})
	}
	return tablesNames, nil
}

func (p *Postgres) ProduceSchema(ctx context.Context, streamID types.StreamID) (*types.Stream, error) {
	populateStream := func(streamID types.StreamID) (*types.Stream, error) {
		schemaName, streamName := streamID.Namespace, streamID.Name
		stream := types.NewStream(streamName, schemaName, &p.config.Database)
		var columnSchemaOutput []ColumnDetails
		err := p.client.SelectContext(ctx, &columnSchemaOutput, getTableSchemaTmpl, schemaName, streamName)
		if err != nil {
			return stream, fmt.Errorf("failed to retrieve column details for table %s: %w", streamName, err)
		}

		if len(columnSchemaOutput) == 0 {
			logger.Warnf("no columns found in table [%s.%s]", schemaName, streamName)
			return stream, nil
		}

		var primaryKeyOutput []ColumnDetails
		err = p.client.SelectContext(ctx, &primaryKeyOutput, getTablePrimaryKey, schemaName, streamName)
		if err != nil {
			return stream, fmt.Errorf("failed to retrieve primary key columns for table %s: %w", streamName, err)
		}

		for _, column := range columnSchemaOutput {
			stream.WithCursorField(column.Name)
			var datatype types.DataType
			if val, found := p.typeMapping[*column.DataType]; found {
				datatype = val
			} else {
				logger.Debugf("failed to get respective type in datatypes for column: %s[%s]", column.Name, *column.DataType)
				datatype = types.String
			}

			stream.UpsertField(column.Name, datatype, strings.EqualFold("yes", *column.IsNullable), false)
		}

		// add primary keys for stream
		for _, column := range primaryKeyOutput {
			stream.WithPrimaryKey(column.Name)
		}

		stream.WithSyncMode(types.FULLREFRESH, types.INCREMENTAL)
		if p.CDCSupported() {
			stream.UpsertField(waljs.CDCLSN, types.String, true, true)
			stream.WithSyncMode(types.CDC, types.STRICTCDC)
		}

		return stream, nil
	}

	stream, err := populateStream(streamID)
	if err != nil {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("failed to produce schema context deadline exceeded: %w", ctx.Err())
		}
		return nil, err
	}
	return stream, nil
}

func (p *Postgres) Type() string {
	return string(constants.Postgres)
}

func (p *Postgres) MaxConnections() int {
	return p.config.MaxThreads
}

func (p *Postgres) MaxRetries() int {
	return p.config.RetryCount
}

func (p *Postgres) columnType(columnType string) types.DataType {
	return typeutils.ExtractAndMapColumnType(columnType, p.typeMapping)
}

func (p *Postgres) dataTypeConverter(value interface{}, columnType string) (interface{}, error) {
	if value == nil {
		return nil, typeutils.ErrNullValue
	}
	olakeType := typeutils.ExtractAndMapColumnType(columnType, p.typeMapping)
	if olakeType == types.Binary {
		if value, ok := value.(string); ok {
			decoded, err := hex.DecodeString(strings.TrimPrefix(value, `\x`))
			if err != nil {
				return nil, fmt.Errorf("failed to decode bytea: %w", err)
			}
			return decoded, nil
		}
	}
	return typeutils.ReformatValue(olakeType, value)
}
