package tiering

import (
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// The write barrier (be-protocol P15.13): every insert, update or delete of a class's root or child row checks, in
// the writer's own transaction, whether the row's aggregate is a member of a unit that is no longer open, and fails
// with SQLSTATE BE001 (RECORD_SEALED) if so, unless the transaction is the SDK's own work on that unit (it sets
// be.tier_unit to the unit, transaction-local). It is a trigger, so it holds whichever entry the write comes from. A
// child's trigger first takes FOR SHARE on its root row (a child below another child finds it up the parent chain): a
// seal holds FOR UPDATE on the roots it seals, so the two serialize (a write that commits first is seen by the seal
// under its lock; one that comes after is refused).

// barrierFunction is the one trigger function every barrier calls. Arguments: the class, "root" or "child", the root
// table, the root's key column, the root key's SQL type, the column of this table that holds its parent's key, and for
// a child below another child the query that finds the root's key from the parent's ($1).
//
// The function keeps the search path it was created under (the component's schema, at its migration): a write from a
// session with another search path (an operator naming the schema) finds the catalog all the same, and no session can
// put another be_tier_member in front of it.
const barrierFunction = `CREATE OR REPLACE FUNCTION be_tier_barrier() RETURNS trigger LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE
  cls text := TG_ARGV[0];
  kind text := TG_ARGV[1];
  root_table text := TG_ARGV[2];
  root_key text := TG_ARGV[3];
  key_type text := TG_ARGV[4];
  col text := TG_ARGV[5];
  lookup text := coalesce(TG_ARGV[6], '');  -- a child below another child: its root key up the parent chain
  rid text;
  st text;
  uid text;
BEGIN
  FOREACH rid IN ARRAY ARRAY[
      CASE WHEN TG_OP <> 'INSERT' THEN to_jsonb(OLD) ->> col END,
      CASE WHEN TG_OP <> 'DELETE' THEN to_jsonb(NEW) ->> col END] LOOP
    CONTINUE WHEN rid IS NULL;
    IF lookup <> '' THEN
      EXECUTE lookup INTO rid USING rid;
      CONTINUE WHEN rid IS NULL;  -- no parent row: the foreign key refuses the write
    END IF;
    IF kind = 'child' THEN
      EXECUTE format('SELECT 1 FROM %I WHERE %I = CAST($1 AS ' || key_type || ') FOR SHARE', root_table, root_key) USING rid;
    END IF;
    SELECT u.state, u.id::text INTO st, uid FROM be_tier_member m JOIN be_tier_unit u ON u.id = m.unit
      WHERE m.class = cls AND m.root_id = rid;
    -- the SDK's own work on this unit (release, destruction, erasure) names it in the transaction
    IF st IS NOT NULL AND st <> 'open' AND coalesce(current_setting('be.tier_unit', true), '') <> uid THEN
      RAISE EXCEPTION USING ERRCODE = 'BE001', MESSAGE = 'the record belongs to a sealed archive unit',
        DETAIL = cls || '/' || rid;
    END IF;
  END LOOP;
  RETURN COALESCE(NEW, OLD);
END $$`

// sqlType is the SQL type of a key column's type.
func sqlType(typ string) (string, error) {
	switch typ {
	case TypeUUID:
		return "uuid", nil
	case TypeText:
		return "text", nil
	case TypeInt2:
		return "smallint", nil
	case TypeInt4:
		return "integer", nil
	case TypeInt8:
		return "bigint", nil
	}
	return "", fmt.Errorf("tiering: a root key of type %s", typ)
}

// BarrierSQL is the statements that install the barrier for the classes (idempotent): the function, then for every
// tierable class a trigger on its root and on each child.
func BarrierSQL(classes []Class) ([]string, error) {
	out := []string{barrierFunction}
	for _, c := range classes {
		if !c.Tierable() {
			continue
		}
		keyType, err := sqlType(c.Root.Columns[c.Root.PK])
		if err != nil {
			return nil, fmt.Errorf("%s: %w", c.Name, err)
		}
		name := "be_tier_barrier_" + strings.NewReplacer(".", "_", "-", "_").Replace(c.Name)
		trigger := func(table, kind, col, lookup string) []string {
			args := []string{c.Name, kind, c.Root.Name, c.Root.PK, keyType, col}
			if lookup != "" {
				args = append(args, lookup)
			}
			for i, a := range args {
				args[i] = "'" + strings.ReplaceAll(a, "'", "''") + "'"
			}
			t := pgx.Identifier{table}.Sanitize()
			n := pgx.Identifier{name}.Sanitize()
			return []string{
				"DROP TRIGGER IF EXISTS " + n + " ON " + t,
				"CREATE TRIGGER " + n + " BEFORE INSERT OR UPDATE OR DELETE ON " + t +
					" FOR EACH ROW EXECUTE FUNCTION be_tier_barrier(" + strings.Join(args, ", ") + ")",
			}
		}
		out = append(out, trigger(c.Root.Name, "root", c.Root.PK, "")...)
		for _, ch := range c.Children {
			lookup := ""
			if parent, ok := c.child(ch.Parent); ok {
				lookup = fmt.Sprintf("SELECT %s FROM %s p WHERE p.%s::text = $1", c.rootExpr(parent, "p"), ident(parent.Name),
					ident(parent.PK))
			}
			out = append(out, trigger(ch.Name, "child", ch.FK, lookup)...)
		}
	}
	return out, nil
}
