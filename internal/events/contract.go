package events

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/fs"
	"path"
	"sort"

	beprotocol "github.com/brickKit/be-protocol"
	"github.com/brickKit/be-sdk-go/internal/envelope"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

// Protocol schema locations: the events-contract schema validates a contract file, and a payload
// schema may $ref its $defs/claim_check (P12.2). Both resolve from the embedded protocol files.
const (
	contractSchemaURL = "https://github.com/brickKit/be-protocol/schemas/events-contract.schema.json"
	envelopeSchemaURL = "https://github.com/brickKit/be-protocol/schemas/envelope.schema.json"
	contractGlob      = "contracts/events/*.events.json"
)

// Contract is a component's events contract, every contracts/events/*.events.json (P12.2).
// It is immutable once loaded and safe for concurrent use.
type Contract struct {
	events map[string]EventDef
}

// EventDef is one event of the contract (P12.2, P11.8).
type EventDef struct {
	Subject             string // P12.3
	File                string // the contract file name, for ce-dataschema
	AggregateType       string // x-aggregate-type: ce-aggregatetype
	TransactionDocument bool   // x-transaction-document: ce-legalentity required (P11.8)
	schema              *jsonschema.Schema
}

// contractFile is the part of a contract file the runtime reads.
type contractFile struct {
	Events []struct {
		Subject             string          `json:"subject"`
		AggregateType       string          `json:"x-aggregate-type"`
		Consumption         string          `json:"x-consumption"`
		TransactionDocument bool            `json:"x-transaction-document"`
		Payload             json.RawMessage `json:"payload"`
	} `json:"events"`
}

// LoadContract reads every contracts/events/*.events.json of fsys (the component's root),
// validates each file against the protocol's events-contract schema and compiles each payload
// schema (JSON Schema 2020-12, formats asserted) (P12.2). A subject declared twice, an invalid
// subject (P12.3) or the reserved sequence mode is an error.
func LoadContract(fsys fs.FS) (*Contract, error) {
	names, err := fs.Glob(fsys, contractGlob)
	if err != nil {
		return nil, fmt.Errorf("events contract: %w", err)
	}
	sort.Strings(names)
	fileSchema, err := compileContractSchema()
	if err != nil {
		return nil, err
	}
	c := &Contract{events: map[string]EventDef{}}
	for _, name := range names {
		if err := c.addFile(fsys, name, fileSchema); err != nil {
			return nil, fmt.Errorf("events contract %s: %w", name, err)
		}
	}
	return c, nil
}

// addFile validates one contract file and adds its events.
func (c *Contract) addFile(fsys fs.FS, name string, fileSchema *jsonschema.Schema) error {
	raw, err := fs.ReadFile(fsys, name)
	if err != nil {
		return err
	}
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
	if err != nil {
		return fmt.Errorf("not JSON: %w", err)
	}
	if err := fileSchema.Validate(doc); err != nil {
		return fmt.Errorf("does not match events-contract.schema.json: %w", err)
	}
	var f contractFile
	if err := json.Unmarshal(raw, &f); err != nil {
		return err
	}
	base := path.Base(name)
	for _, e := range f.Events {
		if err := envelope.ValidSubject(e.Subject); err != nil {
			return err
		}
		if _, dup := c.events[e.Subject]; dup {
			return fmt.Errorf("subject %s declared twice", e.Subject)
		}
		if e.Consumption == "sequence" {
			return fmt.Errorf("subject %s: x-consumption sequence is reserved, not built in 1.0", e.Subject)
		}
		sch, err := compilePayload(base, e.Subject, e.Payload)
		if err != nil {
			return fmt.Errorf("subject %s: payload schema: %w", e.Subject, err)
		}
		c.events[e.Subject] = EventDef{Subject: e.Subject, File: base, AggregateType: e.AggregateType,
			TransactionDocument: e.TransactionDocument, schema: sch}
	}
	return nil
}

// Lookup returns the contract entry of subject.
func (c *Contract) Lookup(subject string) (EventDef, bool) {
	d, ok := c.events[subject]
	return d, ok
}

// Subjects lists every subject of the contract, sorted.
func (c *Contract) Subjects() []string {
	out := make([]string, 0, len(c.events))
	for s := range c.events {
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}

// Validate checks a payload against the event's payload schema (P12.2): it must be a JSON object
// matching the schema.
func (d EventDef) Validate(payload []byte) error {
	v, err := jsonschema.UnmarshalJSON(bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("payload of %s is not JSON: %w", d.Subject, err)
	}
	if _, ok := v.(map[string]any); !ok {
		return fmt.Errorf("payload of %s is not a JSON object", d.Subject)
	}
	if err := d.schema.Validate(v); err != nil {
		return fmt.Errorf("payload of %s violates the contract: %w", d.Subject, err)
	}
	return nil
}

// newCompiler returns a compiler that knows the protocol schemas and never loads from the network.
func newCompiler() (*jsonschema.Compiler, error) {
	c := jsonschema.NewCompiler()
	c.DefaultDraft(jsonschema.Draft2020)
	c.AssertFormat()
	c.UseLoader(offlineLoader{})
	for url, file := range map[string]string{contractSchemaURL: "schemas/events-contract.schema.json",
		envelopeSchemaURL: "schemas/envelope.schema.json"} {
		raw, err := fs.ReadFile(beprotocol.FS, file)
		if err != nil {
			return nil, fmt.Errorf("events contract: protocol schema %s: %w", file, err)
		}
		doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
		if err != nil {
			return nil, err
		}
		if err := c.AddResource(url, doc); err != nil {
			return nil, err
		}
	}
	return c, nil
}

func compileContractSchema() (*jsonschema.Schema, error) {
	c, err := newCompiler()
	if err != nil {
		return nil, err
	}
	s, err := c.Compile(contractSchemaURL)
	if err != nil {
		return nil, fmt.Errorf("events contract: compile protocol schema: %w", err)
	}
	return s, nil
}

// compilePayload compiles one payload schema as its own resource.
func compilePayload(file, subject string, raw json.RawMessage) (*jsonschema.Schema, error) {
	c, err := newCompiler()
	if err != nil {
		return nil, err
	}
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	url := "mem:///contracts/events/" + file + "/" + subject + ".json"
	if err := c.AddResource(url, doc); err != nil {
		return nil, err
	}
	return c.Compile(url)
}

// offlineLoader refuses every URL that is not a registered resource: a contract never makes the
// runtime fetch anything.
type offlineLoader struct{}

func (offlineLoader) Load(url string) (any, error) {
	return nil, fmt.Errorf("schema %s is not available offline", url)
}
