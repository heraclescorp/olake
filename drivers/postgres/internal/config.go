package driver

import (
	"crypto/tls"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"

	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/errs"
)

type Config struct {
	Connection       *url.URL          `json:"-"`
	Host             string            `json:"host"`
	Port             int               `json:"port"`
	Database         string            `json:"database"`
	Username         string            `json:"username"`
	Password         string            `json:"password"`
	JDBCURLParams    map[string]string `json:"jdbc_url_params"`
	SSLConfiguration *utils.SSLConfig  `json:"ssl"`
	UpdateMethod     interface{}       `json:"update_method"`
	MaxThreads       int               `json:"max_threads"`
	RetryCount       int               `json:"retry_count"`
	SSHConfig        *utils.SSHConfig  `json:"ssh_config"`
	Schemas          []string          `json:"schemas,omitempty"`
	// BackfillHost is an optional Aurora PostgreSQL reader instance for chunk planning and chunk scans.
	// The replication slot, publication checks, CDC, discovery and incremental change reads use Host.
	BackfillHost string `json:"backfill_host,omitempty"`
	// BackfillConnection is Connection pointed at BackfillHost; nil when no backfill host is set.
	BackfillConnection *url.URL `json:"-"`
}

// Capture Write Ahead Logs
type CDC struct {
	ReplicationSlot string `json:"replication_slot"`
	InitialWaitTime int    `json:"initial_wait_time"`
	// Publications used when OutputPlugin is pgoutput
	Publication string `json:"publication"`
}

func (c *Config) Validate() error {
	if c.Host == "" {
		return errs.Precondition(errs.ConfigInvalid, codeHostMissing, fmt.Errorf("empty host name"))
	} else if strings.Contains(c.Host, "https") || strings.Contains(c.Host, "http") {
		return errs.Precondition(errs.ConfigInvalid, codeHostSchemeIncluded,
			fmt.Errorf("host should not contain http or https"))
	}

	if strings.Contains(c.BackfillHost, "https") || strings.Contains(c.BackfillHost, "http") {
		return errs.Precondition(errs.ConfigInvalid, codeBackfillHostSchemeIncluded,
			fmt.Errorf("backfill host should not contain http or https"))
	}

	// Validate port
	if c.Port <= 0 || c.Port > 65535 {
		return errs.Precondition(errs.ConfigInvalid, codePortInvalid,
			fmt.Errorf("invalid port number: must be between 1 and 65535"))
	}

	if c.Database == "" {
		return errs.Precondition(errs.ConfigInvalid, codeDatabaseMissing,
			fmt.Errorf("database name is required"))
	}

	if err := utils.ApplyMaxThreadsDefault(&c.MaxThreads); err != nil {
		return err
	}

	if err := utils.ApplyRetryCountDefault(&c.RetryCount); err != nil {
		return err
	}

	query := url.Values{}

	// Set additional connection parameters if available
	if len(c.JDBCURLParams) > 0 {
		for k, v := range c.JDBCURLParams {
			query.Add(k, v)
		}
	}

	if c.SSLConfiguration == nil {
		c.SSLConfiguration = &utils.SSLConfig{
			Mode: "disable",
		}
	}

	sslmode := string(c.SSLConfiguration.Mode)
	if sslmode != "" {
		query.Add("sslmode", sslmode)
	}

	err := c.SSLConfiguration.Validate()
	if err != nil {
		return fmt.Errorf("failed to validate ssl config: %w", err)
	}

	c.Connection = c.connectionURL(c.Host, query)
	if c.BackfillHost != "" {
		c.BackfillConnection = c.connectionURL(c.BackfillHost, query)
	}

	for i, s := range c.Schemas {
		if strings.TrimSpace(s) == "" {
			return errs.Precondition(errs.ConfigInvalid, codeSchemaBlank,
				fmt.Errorf("schemas[%d] must not be blank", i))
		}
	}

	return nil
}

// connectionURL shares every connection setting between the writer Host and the BackfillHost reader.
func (c *Config) connectionURL(host string, query url.Values) *url.URL {
	return &url.URL{
		Scheme:   "postgres",
		User:     utils.Ternary(c.Password != "", url.UserPassword(c.Username, c.Password), url.User(c.Username)).(*url.Userinfo),
		Host:     net.JoinHostPort(host, strconv.Itoa(c.Port)),
		Path:     "/" + c.Database,
		RawQuery: query.Encode(),
	}
}

func (c *Config) buildTLSConfig() (*tls.Config, error) {
	return utils.BuildTLSConfig(c.Host, c.SSLConfiguration)
}

// buildBackfillTLSConfig verifies the reader's certificate against BackfillHost, the name actually dialed.
func (c *Config) buildBackfillTLSConfig() (*tls.Config, error) {
	return utils.BuildTLSConfig(c.BackfillHost, c.SSLConfiguration)
}

type Table struct {
	Schema string `db:"table_schema"`
	Name   string `db:"table_name"`
}

type ColumnDetails struct {
	Name       string  `db:"column_name"`
	DataType   *string `db:"data_type"`
	IsNullable *string `db:"is_nullable"`
}
