package driver

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/datazip-inc/olake/types"
)

func TestValidateTableEnrollmentMetadataOnly(t *testing.T) {
	cases := []struct {
		name        string
		kind        string
		persistence string
		replica     string
		rls         bool
		readable    bool
		publication bool
		actions     bool
		unfiltered  bool
		want        string
	}{
		{name: "eligible", kind: "r", persistence: "p", replica: "d", readable: true, publication: true, actions: true, unfiltered: true},
		{name: "partitioned", kind: "p", persistence: "p", replica: "f", readable: true, publication: true, actions: true, unfiltered: true},
		{name: "read unavailable", kind: "r", persistence: "p", replica: "d", want: "privilege"},
		{name: "materialized view", kind: "m", persistence: "p", replica: "d", readable: true, want: "permanent"},
		{name: "unlogged", kind: "r", persistence: "u", replica: "d", readable: true, want: "permanent"},
		{name: "row security", kind: "r", persistence: "p", replica: "d", rls: true, readable: true, want: "row-level"},
		{name: "replica identity nothing", kind: "r", persistence: "p", replica: "n", readable: true, want: "replica identity"},
		{name: "missing publication table", kind: "r", persistence: "p", replica: "d", readable: true, want: "not covered"},
		{name: "no deletes", kind: "r", persistence: "p", replica: "d", readable: true, publication: true, want: "inserts, updates, and deletes"},
		{name: "row or column filter", kind: "r", persistence: "p", replica: "d", readable: true, publication: true, actions: true, want: "row filters or column restrictions"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, mock := newMockPostgres(t)
			p.version = 150000
			p.cdcConfig.Publication = "olake_pub"
			if tc.publication {
				p.enrollmentTables = []pubTable{{Schema: "public", Table: "candidate"}}
			}
			mock.ExpectQuery(enrollmentTableMetadata).WithArgs("public", "candidate").WillReturnRows(
				sqlmock.NewRows([]string{"kind", "persistence", "replica_identity", "row_security", "readable"}).AddRow(tc.kind, tc.persistence, tc.replica, tc.rls, tc.readable))
			if tc.readable && (tc.kind == "r" || tc.kind == "p") && tc.persistence == "p" && !tc.rls && (tc.replica == "d" || tc.replica == "f") && tc.publication {
				mock.ExpectQuery(`SELECT pubinsert AND pubupdate AND pubdelete FROM pg_publication WHERE pubname = $1`).WithArgs("olake_pub").WillReturnRows(boolRow("all_changes", tc.actions))
				if tc.actions {
					mock.ExpectQuery(enrollmentUnfilteredPublication).WithArgs("olake_pub", "public", "candidate").WillReturnRows(boolRow("unfiltered", tc.unfiltered))
				}
			}
			stream := types.NewStream("candidate", "public", nil)
			stream.DefaultStreamProperties = &types.DefaultStreamProperties{AppendMode: false}
			err := p.ValidateTableEnrollment(context.Background(), stream)
			if tc.want == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("wanted %q, got %v", tc.want, err)
			}
		})
	}
}

func TestValidateTableEnrollmentUnavailableGlobalPrerequisite(t *testing.T) {
	p, _ := newMockPostgres(t)
	p.enrollmentIssue = errors.New("CDC prerequisite publication is unavailable")
	if err := p.ValidateTableEnrollment(context.Background(), types.NewStream("candidate", "public", nil)); err != p.enrollmentIssue {
		t.Fatalf("missing prerequisite must be an explicit skip: %v", err)
	}
	// No SQL expectations: a known unavailable prerequisite must not access rows,
	// create a slot, advance an LSN, or mutate the publication.
}

func TestValidateTableEnrollmentWal2JSONAndAppendIdentity(t *testing.T) {
	p, mock := newMockPostgres(t)
	mock.ExpectQuery(enrollmentTableMetadata).WithArgs("public", "candidate").WillReturnRows(
		sqlmock.NewRows([]string{"kind", "persistence", "replica_identity", "row_security", "readable"}).AddRow("r", "p", "n", false, true))
	stream := types.NewStream("candidate", "public", nil)
	stream.DefaultStreamProperties = &types.DefaultStreamProperties{AppendMode: true}
	if err := p.ValidateTableEnrollment(context.Background(), stream); err != nil {
		t.Fatal(err)
	}
	// wal2json is not constrained by a publication. Append does not use old row
	// identifiers; it can enroll without DEFAULT/FULL identity or a source PK.
}
