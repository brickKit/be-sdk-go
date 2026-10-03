package lifecycle

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// Class is a table class of P16 ("Table classes").
type Class string

// The eight table classes.
const (
	Master    Class = "master"
	Reference Class = "reference"
	Document  Class = "document"
	Ledger    Class = "ledger"
	Audit     Class = "audit"
	Queue     Class = "queue"
	Snapshot  Class = "snapshot"
	Platform  Class = "platform"
)

// Grain is the length of one range partition.
type Grain string

// The grains of partition.grain.
const (
	Week  Grain = "week"
	Month Grain = "month"
	Year  Grain = "year"
)

// DefaultAhead is partition.ahead when the declaration omits it (lifecycle.schema.json).
const DefaultAhead = 2

// maxPartitionedName keeps `<table>_YYYY_MM_DD` within PostgreSQL's 63-byte identifiers.
const maxPartitionedName = 63 - len("_2006_01_02")

// Partition is a table's partition clause: a range by time ({by, grain, ahead}) or a list opened by a
// command ({by, kind: list, opened_by: command}).
type Partition struct {
	By       string `json:"by"`
	Grain    Grain  `json:"grain,omitempty"`
	Ahead    int    `json:"ahead,omitempty"`
	Kind     string `json:"kind,omitempty"`
	OpenedBy string `json:"opened_by,omitempty"`
}

// IsRange is true for a range-by-time partition clause.
func (p *Partition) IsRange() bool { return p != nil && p.Grain != "" }

// IsList is true for a list partition clause.
func (p *Partition) IsList() bool { return p != nil && p.Kind == "list" }

// Closed says when a row counts as closed: column IN In, closed at the instant in column At.
type Closed struct {
	Column string   `json:"column"`
	In     []string `json:"in"`
	At     string   `json:"at"`
}

// Table is one declared table with its follows resolved: a follower carries its parent's class,
// partition and seal unless it declares its own class (P16.1, "follows").
type Table struct {
	Name         string
	Class        Class
	Follows      string
	Partition    *Partition
	Closed       *Closed
	Seal         Seal
	Cold         string // tiers.cold as declared ("" = none); the 1.0 engine never freezes (G4)
	Hot          string // tiers.hot as declared
	RetentionMin *After // nil = none declared, or `forever` (Forever is then true)
	Forever      bool
	RetentionEnd string
	Guard        string // guard.blocked_by

	inheritsSeal bool // a follower without its own tiers.seal: sealed with its parent
}

// Declaration is a loaded lifecycle.yaml v1 (P16.1).
type Declaration struct {
	Tables map[string]*Table
}

// Names lists the declared tables, sorted.
func (d *Declaration) Names() []string { return sortedKeys(d.Tables) }

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for n := range m {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// Followers lists the tables that follow parent, sorted.
func (d *Declaration) Followers(parent string) []string {
	var out []string
	for _, n := range d.Names() {
		if d.Tables[n].Follows == parent {
			out = append(out, n)
		}
	}
	return out
}

// rawDecl and rawTable mirror the schema for the typed decode.
type rawDecl struct {
	Lifecycle string              `json:"lifecycle"`
	TenantKey string              `json:"tenant_key"`
	Tables    map[string]rawTable `json:"tables"`
}

type rawTable struct {
	Class     Class      `json:"class"`
	Follows   string     `json:"follows"`
	Partition *Partition `json:"partition"`
	Closed    *Closed    `json:"closed"`
	Tiers     struct {
		Hot, Seal, Cold string
	} `json:"tiers"`
	Retention struct {
		Min string `json:"min"`
		End string `json:"end"`
	} `json:"retention"`
	Guard *struct {
		BlockedBy string `json:"blocked_by"`
	} `json:"guard"`
	PII     []string `json:"pii"`
	Erasure *struct {
		Columns map[string]string `json:"columns"`
	} `json:"erasure"`
}

// classInvariant names the P16.1 class rule a table breaks ("" = none); the schema says the same,
// this only gives the message a reason a person can act on.
func classInvariant(rt rawTable) string {
	switch rt.Class {
	case Ledger:
		if len(rt.PII) > 0 {
			return "a ledger table has no pii column (opaque subject ids only)"
		}
		if rt.Erasure != nil && len(rt.Erasure.Columns) > 0 {
			return "a ledger table has no erasure.columns"
		}
	case Queue:
		if rt.Tiers.Cold != "" && rt.Tiers.Cold != "never" {
			return "a queue table is never cold (tiers.cold)"
		}
	case Snapshot:
		if rt.Retention.Min != "" {
			return "a snapshot table declares no retention.min"
		}
	}
	return ""
}

// Error is a fatal problem in lifecycle.yaml; Table is "" when it concerns the whole document.
type Error struct {
	Table string
	Msg   string
}

func (e *Error) Error() string {
	if e.Table == "" {
		return "lifecycle.yaml: " + e.Msg
	}
	return "lifecycle.yaml: table " + e.Table + ": " + e.Msg
}

// Parse reads and checks a lifecycle.yaml v1: the protocol schema, then the invariants P16.1 states
// that need no SQL. Every violation is an *Error naming the table.
func Parse(data []byte) (*Declaration, error) {
	raw, doc, err := yamlToJSON(data)
	if err != nil {
		return nil, &Error{Msg: err.Error()}
	}
	var r rawDecl
	if json.Unmarshal(raw, &r) == nil {
		for _, name := range sortedKeys(r.Tables) {
			if msg := classInvariant(r.Tables[name]); msg != "" {
				return nil, &Error{Table: name, Msg: msg}
			}
		}
	}
	loc, msg, ok, err := validate(declSchemaURL, doc)
	if err != nil {
		return nil, err
	}
	if !ok {
		if len(loc) >= 2 && loc[0] == "tables" {
			return nil, &Error{Table: loc[1], Msg: msg}
		}
		return nil, &Error{Msg: msg}
	}
	r = rawDecl{}
	if err := json.Unmarshal(raw, &r); err != nil {
		return nil, &Error{Msg: err.Error()}
	}
	if r.TenantKey != "" && r.TenantKey != "none" {
		return nil, &Error{Msg: fmt.Sprintf("tenant_key %q: pooled multi-tenancy is reserved; only none is supported", r.TenantKey)}
	}
	d := &Declaration{Tables: map[string]*Table{}}
	for name, rt := range r.Tables {
		t, err := buildTable(name, rt)
		if err != nil {
			return nil, err
		}
		d.Tables[name] = t
	}
	if err := d.resolveFollows(r.Tables); err != nil {
		return nil, err
	}
	return d, d.checkInvariants()
}

func buildTable(name string, rt rawTable) (*Table, error) {
	t := &Table{Name: name, Class: rt.Class, Follows: rt.Follows, Partition: rt.Partition, Closed: rt.Closed,
		Cold: rt.Tiers.Cold, Hot: rt.Tiers.Hot, RetentionEnd: rt.Retention.End}
	if t.Partition.IsRange() && t.Partition.Ahead == 0 {
		t.Partition.Ahead = DefaultAhead
	}
	seal, err := parseSeal(rt.Tiers.Seal)
	if err != nil {
		return nil, &Error{Table: name, Msg: err.Error()}
	}
	t.Seal = seal
	switch m := rt.Retention.Min; {
	case m == "forever":
		t.Forever = true
	case m != "":
		a, _ := parseAfter(m) // the schema admitted it
		t.RetentionMin = &a
	}
	if rt.Guard != nil {
		t.Guard = rt.Guard.BlockedBy
	}
	return t, nil
}

// resolveFollows checks each follows and copies the parent's class, partition and seal into the
// follower where the follower declares none.
func (d *Declaration) resolveFollows(raw map[string]rawTable) error {
	for _, name := range d.Names() {
		t := d.Tables[name]
		if t.Follows == "" {
			continue
		}
		p, ok := d.Tables[t.Follows]
		switch {
		case !ok:
			return &Error{Table: name, Msg: fmt.Sprintf("follows %s, which is not declared", t.Follows)}
		case raw[t.Follows].Follows != "":
			return &Error{Table: name, Msg: fmt.Sprintf("follows %s, which itself follows %s; follow the root table", t.Follows, p.Follows)}
		case t.Partition != nil:
			return &Error{Table: name, Msg: "a follower takes its parent's partition and declares none"}
		}
		t.Partition = p.Partition
		if t.Class == "" {
			t.Class = p.Class
		}
		if raw[name].Tiers.Seal == "" {
			t.Seal, t.inheritsSeal = p.Seal, true
		}
		if t.Closed == nil {
			t.Closed = p.Closed
		}
	}
	return nil
}

// checkInvariants covers what the schema cannot say (P16.1).
func (d *Declaration) checkInvariants() error {
	for _, name := range d.Names() {
		t := d.Tables[name]
		if (t.Partition.IsRange() || t.Partition.IsList()) && len(name) > maxPartitionedName {
			return &Error{Table: name, Msg: fmt.Sprintf("a partitioned table name has at most %d characters, so its partitions' names fit 63", maxPartitionedName)}
		}
		if t.Closed == nil && t.usesClosedAnchor() {
			return &Error{Table: name, Msg: "an anchor `closed` needs a `closed` clause saying when a row is closed"}
		}
		if t.Seal.Kind == SealAfter && t.Seal.After.Anchor == "sealed" {
			return &Error{Table: name, Msg: "tiers.seal cannot be anchored on sealed"}
		}
	}
	return nil
}

func (t *Table) usesClosedAnchor() bool {
	if t.Seal.Kind == SealAfter && t.Seal.After.Anchor == "closed" {
		return true
	}
	if t.RetentionMin != nil && t.RetentionMin.Anchor == "closed" {
		return true
	}
	return strings.HasSuffix(t.Cold, " after closed")
}
