package driver

import (
	"context"
	"fmt"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/drivers/postgres/internal/waljs"
	"github.com/datazip-inc/olake/types"
)

var _ abstract.TableEnrollmentSource = (*Postgres)(nil)

// SetupTableEnrollment opens only the SQL metadata connection. In particular it
// never calls PreCDC, which may advance the replication slot and initialize state.
func (p *Postgres) SetupTableEnrollment(ctx context.Context) error {
	if err := p.setup(ctx, false); err != nil {
		return err
	}
	p.enrollmentIssue = nil
	p.enrollmentTables = nil
	if !p.CDCSupported() {
		p.enrollmentIssue = fmt.Errorf("automatic enrollment requires configured PostgreSQL CDC")
		return nil
	}
	prerequisites := p.prerequisiteChecks(&p.cdcConfig)
	for i := range prerequisites {
		if prerequisites[i].Name == "replication_slot" {
			prerequisites[i].Check = p.checkEnrollmentSlot
		}
	}
	checks := abstract.RunPrerequisites(ctx, prerequisites)
	for _, check := range checks {
		if check.Required && (!check.Passed || check.Err != nil) {
			if check.Err != nil {
				p.enrollmentIssue = fmt.Errorf("CDC prerequisite %s is unavailable: %w", check.Name, check.Err)
			} else {
				p.enrollmentIssue = fmt.Errorf("CDC prerequisite %s is unavailable", check.Name)
			}
			break
		}
	}
	// A prerequisite failure rejects candidates through ValidateTableEnrollment;
	// it must not prevent discovery from producing their explicit skip results.
	if p.enrollmentIssue == nil && p.cdcConfig.Publication != "" {
		var err error
		p.enrollmentTables, err = fetchPublicationTables(ctx, p.client, p.cdcConfig.Publication)
		if err != nil {
			p.enrollmentIssue = fmt.Errorf("publication table metadata is unavailable")
		}
	}
	return nil
}

// Keep even configuration-derived identifiers bound as data on this read-only path.
func (p *Postgres) checkEnrollmentSlot(ctx context.Context) (string, bool, error) {
	var plugin string
	if err := p.client.GetContext(ctx, &plugin,
		`SELECT plugin FROM pg_replication_slots WHERE slot_name = $1 AND database = current_database() AND slot_type = 'logical'`,
		p.cdcConfig.ReplicationSlot); err != nil {
		return "logical replication slot metadata is unavailable", false, err
	}
	if plugin != "pgoutput" && plugin != "wal2json" {
		return "replication slot output plugin is unsupported", false, nil
	}
	if plugin == "pgoutput" && p.cdcConfig.Publication == "" {
		return "pgoutput requires a configured publication", false, nil
	}
	return "exists", true, nil
}

const enrollmentTableMetadata = `SELECT
	c.relkind::text AS kind, c.relpersistence::text AS persistence,
	c.relreplident::text AS replica_identity, c.relrowsecurity AS row_security,
	has_table_privilege(c.oid, 'SELECT') AND has_schema_privilege(n.oid, 'USAGE') AS readable
	FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
	WHERE n.nspname = $1 AND c.relname = $2`

// PostgreSQL 15 introduced publication row filters and column lists. A filtered
// publication cannot safely follow our unfiltered ordinary initial snapshot.
const enrollmentUnfilteredPublication = `SELECT
	pt.rowfilter IS NULL AND NOT EXISTS (
		SELECT 1 FROM pg_attribute a
		JOIN pg_class c ON c.oid = a.attrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $2 AND c.relname = $3
		AND a.attnum > 0 AND NOT a.attisdropped
		AND NOT (a.attname = ANY(pt.attnames)))
	FROM pg_publication_tables pt
	WHERE pt.pubname = $1 AND pt.schemaname = $2 AND pt.tablename = $3`

func (p *Postgres) ValidateTableEnrollment(ctx context.Context, stream *types.Stream) error {
	if p.enrollmentIssue != nil {
		return p.enrollmentIssue
	}
	if stream.Schema != nil {
		if found, property := stream.Schema.GetProperty(waljs.CDCLSN); found && !property.OlakeColumn {
			return fmt.Errorf("source column collides with reserved PostgreSQL CDC metadata")
		}
	}
	var table struct {
		Kind            string `db:"kind"`
		Persistence     string `db:"persistence"`
		ReplicaIdentity string `db:"replica_identity"`
		RowSecurity     bool   `db:"row_security"`
		Readable        bool   `db:"readable"`
	}
	if err := p.client.GetContext(ctx, &table, enrollmentTableMetadata, stream.Namespace, stream.Name); err != nil {
		return fmt.Errorf("table read/replica identity metadata is unavailable")
	}
	if !table.Readable {
		return fmt.Errorf("table SELECT or schema USAGE privilege is unavailable")
	}
	if (table.Kind != "r" && table.Kind != "p") || table.Persistence != "p" {
		return fmt.Errorf("only permanent ordinary or partitioned tables support automatic CDC enrollment")
	}
	if table.RowSecurity {
		return fmt.Errorf("row-level security needs explicit manual snapshot/CDC review")
	}
	if stream.DefaultStreamProperties != nil && !stream.DefaultStreamProperties.AppendMode && table.ReplicaIdentity != "d" && table.ReplicaIdentity != "f" {
		return fmt.Errorf("upsert requires DEFAULT primary-key or FULL replica identity")
	}
	if p.cdcConfig.Publication == "" {
		return nil // wal2json has no publication; pgoutput was rejected by the slot prerequisite.
	}
	if err := checkStreamsInPublication(p.cdcConfig.Publication, p.enrollmentTables, []types.StreamInterface{stream.Wrap(0)}); err != nil {
		return fmt.Errorf("table is not covered by the configured publication")
	}
	var allChanges bool
	if err := p.client.GetContext(ctx, &allChanges,
		`SELECT pubinsert AND pubupdate AND pubdelete FROM pg_publication WHERE pubname = $1`, p.cdcConfig.Publication); err != nil || !allChanges {
		return fmt.Errorf("publication must publish inserts, updates, and deletes")
	}
	if p.version >= 150000 {
		var unfiltered bool
		if err := p.client.GetContext(ctx, &unfiltered, enrollmentUnfilteredPublication, p.cdcConfig.Publication, stream.Namespace, stream.Name); err != nil || !unfiltered {
			return fmt.Errorf("publication row filters or column restrictions are unsupported or unavailable")
		}
	}
	return nil
}
