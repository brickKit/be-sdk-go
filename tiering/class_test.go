package tiering

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func orders() Class {
	return Class{Name: "sales.orders", Category: CategoryCompleted,
		Root: Table{Name: "orders", PK: "id", Columns: map[string]string{"id": TypeUUID, "legal_entity": TypeUUID,
			"closed_at": TypeTimestamptz, "created_at": TypeTimestamptz, "email": TypeText}},
		Children: []Child{{Table: Table{Name: "order_lines", PK: "id", Columns: map[string]string{"id": TypeUUID,
			"order_id": TypeUUID}}, FK: "order_id"}},
		Group: []string{"legal_entity"}, Anchor: "closed_at", Created: "created_at", Terminal: "r.status = 'closed'",
		ResidenceDays: 365, Retention: Retention{MinDays: 3650, MaxDays: 3650, Basis: "commercial law"},
		Locate: []string{"legal_entity"}, Erasable: []string{"email"}, Routes: []string{RouteObject}, SchemaVersion: "1",
		HoldOperation: "sales.order.hold", ApproveOperation: "sales.order.approve_destruction"}
}

// P15.1-P15.5: what a declaration may not say.
func TestAClassDeclaration(t *testing.T) {
	assert.NoError(t, orders().Check(90, []string{"doc_numbers"}))

	cases := map[string]func(c *Class){
		"residence below N (P15.3, TR29)":            func(c *Class) { c.ResidenceDays = 30 },
		"a residence on a never-tiered kind (P15.2)": func(c *Class) { c.Category = CategoryOnlineMutable },
		"routes on evidence kept online (P15.2, TR22)": func(c *Class) {
			c.Category, c.ResidenceDays = CategoryEvidence, 0
		},
		"a unique registry as a table (P15.5, TR21)": func(c *Class) { c.Children[0].Name = "doc_numbers" },
		"an unknown route":                           func(c *Class) { c.Routes = []string{"iceberg"} },
		"no routes":                                  func(c *Class) { c.Routes = nil },
		"no terminal condition":                      func(c *Class) { c.Terminal = "" },
		"a retention without a basis (P15.4)":        func(c *Class) { c.Retention.Basis = "" },
		"the longest retention below the shortest":   func(c *Class) { c.Retention.MaxDays = 10 },
		"an anchor that is not a column":             func(c *Class) { c.Anchor = "closed" },
		"an anchor that is not an instant":           func(c *Class) { c.Anchor = "legal_entity" },
		"a locate column the root lacks":             func(c *Class) { c.Locate = []string{"region"} },
		"a child's key without a type":               func(c *Class) { c.Children[0].FK = "parent" },
		"an erasable column in no table":             func(c *Class) { c.Erasable = []string{"phone"} },
		"a column type the encoding lacks":           func(c *Class) { c.Root.Columns["email"] = "citext" },
		"no schema version":                          func(c *Class) { c.SchemaVersion = "" },
		"no hold operation (P15.28)":                 func(c *Class) { c.HoldOperation = "" },
		"no approval operation (P15.29)":             func(c *Class) { c.ApproveOperation = "" },
		"a child under an unknown parent": func(c *Class) {
			c.Children = append(c.Children, Child{Table: Table{Name: "notes", PK: "id", Columns: map[string]string{
				"id": TypeUUID, "line_id": TypeUUID}}, FK: "line_id", Parent: "lines"})
		},
		"a root with a key of two columns": func(c *Class) { c.Root.PK, c.Root.Key = "", []string{"id", "legal_entity"} },
		"a table with both a PK and a Key": func(c *Class) { c.Children[0].Key = []string{"id", "order_id"} },
		"a parent with a key of two columns": func(c *Class) {
			c.Children[0].PK, c.Children[0].Key = "", []string{"id", "order_id"}
			c.Children = append(c.Children, Child{Table: Table{Name: "notes", PK: "id", Columns: map[string]string{
				"id": TypeUUID, "line_id": TypeUUID}}, FK: "line_id", Parent: "order_lines"})
		},
		"a key column without a type": func(c *Class) { c.Children[0].PK, c.Children[0].Key = "", []string{"id", "nope"} },
		"children in a cycle": func(c *Class) {
			c.Children = []Child{
				{Table: Table{Name: "a", PK: "id", Columns: map[string]string{"id": TypeUUID, "b_id": TypeUUID}}, FK: "b_id", Parent: "b"},
				{Table: Table{Name: "b", PK: "id", Columns: map[string]string{"id": TypeUUID, "a_id": TypeUUID}}, FK: "a_id", Parent: "a"}}
		},
		"pending but erasable, no hold operation": func(c *Class) {
			c.Retention, c.HoldOperation, c.ApproveOperation = Retention{Pending: true}, "", ""
		},
	}
	for name, edit := range cases {
		t.Run(name, func(t *testing.T) {
			c := orders()
			c.Children = append([]Child(nil), c.Children...)
			c.Root.Columns = clone(c.Root.Columns)
			edit(&c)
			assert.Error(t, c.Check(90, []string{"doc_numbers"}))
		})
	}

	unknown := orders()
	unknown.Children = append(unknown.Children, Child{Table: Table{Name: "notes", PK: "id", Columns: map[string]string{
		"id": TypeUUID, "line_id": TypeUUID}}, FK: "line_id", Parent: "lines"})
	assert.ErrorContains(t, unknown.Check(90, nil), `its parent "lines" is not the root or another child`)
	nested := orders()
	nested.Children = append(nested.Children, Child{Table: Table{Name: "line_notes", PK: "id", Columns: map[string]string{
		"id": TypeUUID, "line_id": TypeUUID}}, FK: "line_id", Parent: "order_lines"})
	assert.NoError(t, nested.Check(90, nil), "a child under a child (P15.1)")
	leaf := orders()
	leaf.Children[0].PK, leaf.Children[0].Key = "", []string{"order_id", "id"}
	assert.NoError(t, leaf.Check(90, nil), "a leaf with a key of two columns")
	never := orders()
	never.ResidenceDays = Never
	assert.NoError(t, never.Check(90, nil), "a class may stay online")
	pending := orders()
	pending.Retention = Retention{Pending: true}
	assert.NoError(t, pending.Check(90, nil), "a retention without a basis is pending (P15.4, TR24)")
	kept := orders()
	kept.Retention, kept.Erasable, kept.HoldOperation, kept.ApproveOperation = Retention{Pending: true}, nil, "", ""
	assert.NoError(t, kept.Check(90, nil), "never destroyed nor erased: no hold or approval operation")
	evidence := orders()
	evidence.Category, evidence.ResidenceDays, evidence.Routes = CategoryEvidence, 0, nil
	assert.NoError(t, evidence.Check(90, nil), "a never-tiered kind with a retention only")
}

func clone(m map[string]string) map[string]string {
	out := map[string]string{}
	for k, v := range m {
		out[k] = v
	}
	return out
}
