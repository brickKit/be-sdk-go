package testpg

import "testing"

func TestNewIdentity(t *testing.T) {
	for _, major := range []string{"16", "14"} {
		id := NewOn(t, major)
		db := Open(t, id.DSN(id.User, id.Password))
		var schemaOK bool
		if err := db.QueryRow(`SELECT has_schema_privilege(current_user, $1, 'USAGE') AND NOT has_schema_privilege(current_user, $1, 'CREATE')`, id.Schema).Scan(&schemaOK); err != nil || !schemaOK {
			t.Fatalf("PG%s runtime role privileges: ok=%v err=%v", major, schemaOK, err)
		}
	}
}
