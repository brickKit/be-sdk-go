package pg

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/brickKit/be-sdk-go/internal/problem"
	"github.com/jackc/pgx/v5"
)

// Minimum server versions (P10.7, P19.5): declarative partitioning and FOR UPDATE SKIP LOCKED are
// present in every version at or above them.
const (
	MinServerVersion      = 140000
	MinShellServerVersion = 160000
)

// ProbeResult is what the start-up probe found (P10.7, P1.4); the root decides readiness from it.
type ProbeResult struct {
	Err              error    // the probe could not run (connection, deadline): try again later
	Fatal            error    // a missing capability: the process exits (P1.8)
	IdentityProblems []string // each failed identity check; non-empty = /readyz stays 503, be_db_identity_ok 0
	ComponentVersion uint     // schema_migrations_<schema>; 0 when absent
	ComponentDirty   bool
	PlatformVersion  int // besdk_platform_version for this component; 0 when absent
}

// Probe runs the start-up probe in one read-only Store transaction as PG_USER (P10.7): capabilities,
// identity (USAGE but no CREATE on the schema, not a member of owner, every table owned by owner with
// SELECT, INSERT, UPDATE, DELETE for PG_USER) and the schema's migration versions. A role the login
// role cannot switch to is an identity problem.
func Probe(ctx context.Context, s *Store, owner string, shell bool) ProbeResult {
	var r ProbeResult
	err := s.Run(ctx, TxOptions{ReadOnly: true}, func(ctx context.Context, tx *Tx) error {
		r = ProbeResult{}
		if err := probeCapabilities(ctx, tx, shell, &r); err != nil || r.Fatal != nil {
			return err
		}
		ok, err := probeIdentity(ctx, tx, s.Role(), owner, &r)
		if err != nil || !ok {
			return err
		}
		return probeVersions(ctx, tx, s.cfg.ComponentID, &r)
	})
	switch state := SQLState(err); {
	case err == nil:
	case state == "42501" || state == "22023": // permission denied to set role / role does not exist
		r.IdentityProblems = append(r.IdentityProblems, fmt.Sprintf("cannot switch to PG_USER %q: %v", s.Role(), err))
	default:
		r.Err = err
	}
	return r
}

func probeCapabilities(ctx context.Context, tx *Tx, shell bool, r *ProbeResult) error {
	var v int
	if err := tx.QueryRowContext(ctx, `SELECT current_setting('server_version_num')::int`).Scan(&v); err != nil {
		return err
	}
	need, what := MinServerVersion, "PostgreSQL 14"
	if shell {
		need, what = MinShellServerVersion, "PostgreSQL 16 (shell)"
	}
	if v < need {
		r.Fatal = problem.Wrap(fmt.Errorf("server_version_num %d < %d: %s required", v, need, what),
			"CAPABILITY_UNAVAILABLE", map[string]string{"capability": fmt.Sprintf("server_version_num>=%d", need)})
	}
	return nil
}

// probeIdentity appends a problem per failed check; false = the schema is unusable, stop here.
func probeIdentity(ctx context.Context, tx *Tx, role, owner string, r *ProbeResult) (bool, error) {
	var hasSchema, create, ownerExists, member bool
	err := tx.QueryRowContext(ctx, `SELECT current_schema() IS NOT NULL,
	    COALESCE(has_schema_privilege(current_user, current_schema(), 'CREATE'), false),
	    EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1),
	    COALESCE((SELECT pg_has_role(current_user, o.oid, 'MEMBER') FROM pg_roles o WHERE o.rolname = $1), false)`,
		owner).Scan(&hasSchema, &create, &ownerExists, &member)
	if err != nil {
		return false, err
	}
	if !hasSchema {
		r.IdentityProblems = append(r.IdentityProblems,
			fmt.Sprintf("schema %q does not exist or PG_USER %q has no USAGE on it", tx.Schema(), role))
		return false, nil
	}
	if create {
		r.IdentityProblems = append(r.IdentityProblems, fmt.Sprintf("PG_USER %q has CREATE on schema %q", role, tx.Schema()))
	}
	if !ownerExists {
		r.IdentityProblems = append(r.IdentityProblems, fmt.Sprintf("PG_OWNER_USER %q does not exist", owner))
	}
	if member {
		r.IdentityProblems = append(r.IdentityProblems, fmt.Sprintf("PG_USER %q is a member of %q", role, owner))
	}
	return true, probeTables(ctx, tx, role, owner, r)
}

// probeTables checks every relation of the schema, partitions included (P10.7).
func probeTables(ctx context.Context, tx *Tx, role, owner string, r *ProbeResult) error {
	rows, err := tx.QueryContext(ctx, `SELECT c.relname, pg_get_userbyid(c.relowner),
	    has_table_privilege(current_user, c.oid, 'SELECT'), has_table_privilege(current_user, c.oid, 'INSERT'),
	    has_table_privilege(current_user, c.oid, 'UPDATE'), has_table_privilege(current_user, c.oid, 'DELETE')
	  FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
	 WHERE n.nspname = current_schema() AND c.relkind IN ('r', 'p', 'v', 'm', 'f')
	 ORDER BY c.relname`)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var name, relOwner string
		var priv [4]bool
		if err := rows.Scan(&name, &relOwner, &priv[0], &priv[1], &priv[2], &priv[3]); err != nil {
			return err
		}
		if relOwner != owner {
			r.IdentityProblems = append(r.IdentityProblems, fmt.Sprintf("table %q is owned by %q, not %q", name, relOwner, owner))
		}
		var missing []string
		for i, p := range []string{"SELECT", "INSERT", "UPDATE", "DELETE"} {
			if !priv[i] {
				missing = append(missing, p)
			}
		}
		if len(missing) > 0 {
			r.IdentityProblems = append(r.IdentityProblems,
				fmt.Sprintf("PG_USER %q lacks %s on table %q", role, strings.Join(missing, ", "), name))
		}
	}
	return rows.Err()
}

// probeVersions reads the component's migration version and the platform version; an absent or
// unreadable table counts as version 0 (its privilege problem is already reported).
func probeVersions(ctx context.Context, tx *Tx, componentID string, r *ProbeResult) error {
	state := ComponentStateTable(tx.Schema())
	var hasState, hasPlatform bool
	err := tx.QueryRowContext(ctx, `SELECT
	    COALESCE(has_table_privilege(current_user, to_regclass(format('%I.%I', current_schema(), $1::text)), 'SELECT'), false),
	    COALESCE(has_table_privilege(current_user, to_regclass(format('%I.%I', current_schema(), 'besdk_platform_version')), 'SELECT'), false)`,
		state).Scan(&hasState, &hasPlatform)
	if err != nil {
		return err
	}
	if hasState {
		var v int64
		err := tx.QueryRowContext(ctx, `SELECT version, dirty FROM `+pgx.Identifier{state}.Sanitize()+` LIMIT 1`).Scan(&v, &r.ComponentDirty)
		if err != nil && err != sql.ErrNoRows {
			return err
		}
		r.ComponentVersion = uint(max(v, 0))
	}
	if hasPlatform {
		err := tx.QueryRowContext(ctx, `SELECT version FROM besdk_platform_version WHERE component = $1`, componentID).Scan(&r.PlatformVersion)
		if err != nil && err != sql.ErrNoRows {
			return err
		}
	}
	return nil
}

// ComponentStateTable is golang-migrate's state table of the component migrations (P11.3).
func ComponentStateTable(schema string) string { return "schema_migrations_" + schema }

// PlatformStateTable is golang-migrate's state table of the platform migration (P11.3).
func PlatformStateTable(schema string) string { return "besdk_migrations_" + schema }
