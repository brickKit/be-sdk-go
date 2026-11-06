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
		Locate: []string{"legal_entity"}, Erasable: []string{"email"}, Routes: []string{RouteObject}, SchemaVersion: "1"}
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

	never := orders()
	never.ResidenceDays = Never
	assert.NoError(t, never.Check(90, nil), "a class may stay online")
	pending := orders()
	pending.Retention = Retention{Pending: true}
	assert.NoError(t, pending.Check(90, nil), "a retention without a basis is pending (P15.4, TR24)")
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
