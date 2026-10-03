package events

import (
	"io/fs"
	"strings"
	"testing"
	"testing/fstest"

	beprotocol "github.com/brickKit/be-protocol"
	"github.com/stretchr/testify/require"
)

// widgetContract is the fixture component's events contract (be-protocol fixtures/widget).
func widgetContract(t *testing.T) *Contract {
	t.Helper()
	sub, err := fs.Sub(beprotocol.FS, "fixtures/widget")
	require.NoError(t, err)
	c, err := LoadContract(sub)
	require.NoError(t, err)
	return c
}

func TestLoadContractReadsTheFixture(t *testing.T) {
	c := widgetContract(t)
	d, ok := c.Lookup("conformance.widget.created.v1")
	require.True(t, ok)
	require.Equal(t, "conformance.widget.widget", d.AggregateType)
	require.True(t, d.TransactionDocument)
	require.Equal(t, "widget.events.json", d.File)
	require.ElementsMatch(t, []string{"conformance.widget.created.v1", "conformance.widget.approved.v1",
		"conformance.widget.reverted.v1"}, c.Subjects())
	_, ok = c.Lookup("conformance.widget.deleted.v1")
	require.False(t, ok)
}

func TestContractValidatesPayloads(t *testing.T) {
	d, _ := widgetContract(t).Lookup("conformance.widget.reverted.v1")
	require.NoError(t, d.Validate([]byte(`{"widget_id":"w","legal_entity_id":"LE01","number":"N1","status":"DRAFT","reason":"RESERVATION_EXPIRED","version":2}`)))
	require.Error(t, d.Validate([]byte(`{"widget_id":"w"}`)))
	require.Error(t, d.Validate([]byte(`{"widget_id":"w","legal_entity_id":"LE01","number":"N1","status":"APPROVED","reason":"RESERVATION_EXPIRED","version":2}`)))
	require.Error(t, d.Validate([]byte(`[1]`)))
	require.Error(t, d.Validate([]byte(`not json`)))
}

func TestContractAssertsFormats(t *testing.T) {
	d, _ := widgetContract(t).Lookup("conformance.widget.created.v1")
	ok := `{"widget_id":"w","legal_entity_id":"LE01","number":"N1","document_date":"2026-10-03","kind_code":"K","region":"R","owner_id":"o","status":"DRAFT","currency":"EUR","quantity":"1","amount":"2.50","version":1}`
	require.NoError(t, d.Validate([]byte(ok)))
	bad := strings.Replace(ok, "2026-10-03", "2026-13-45", 1)
	require.Error(t, d.Validate([]byte(bad)))
}

func contractFS(files map[string]string) fs.FS {
	m := fstest.MapFS{}
	for name, body := range files {
		m["contracts/events/"+name] = &fstest.MapFile{Data: []byte(body)}
	}
	return m
}

const claimCheckContract = `{"events":[{"subject":"erp.docs.file.attached.v1","x-aggregate-type":"erp.docs.file",
 "payload":{"type":"object","required":["content"],"properties":{"content":
 {"$ref":"https://github.com/brickKit/be-protocol/schemas/events-contract.schema.json#/$defs/claim_check"}}}}]}`

func TestContractResolvesTheClaimCheckRefOffline(t *testing.T) {
	c, err := LoadContract(contractFS(map[string]string{"docs.events.json": claimCheckContract}))
	require.NoError(t, err)
	d, ok := c.Lookup("erp.docs.file.attached.v1")
	require.True(t, ok)
	require.False(t, d.TransactionDocument)
	sum := strings.Repeat("a", 64)
	require.NoError(t, d.Validate([]byte(`{"content":{"key":"k","sha256":"`+sum+`","size":3}}`)))
	require.Error(t, d.Validate([]byte(`{"content":{"key":"k","sha256":"XYZ","size":3}}`)))
}

func TestLoadContractRejects(t *testing.T) {
	cases := map[string]map[string]string{
		"not json":            {"a.events.json": `{`},
		"schema violation":    {"a.events.json": `{"events":[{"subject":"erp.a.b.v1","payload":{"type":"object"}}]}`},
		"bad subject":         {"a.events.json": `{"events":[{"subject":"erp.a.v1","x-aggregate-type":"erp.a","payload":{"type":"object"}}]}`},
		"sequence mode":       {"a.events.json": `{"events":[{"subject":"erp.a.b.v1","x-aggregate-type":"erp.a","x-consumption":"sequence","payload":{"type":"object"}}]}`},
		"tx doc without le":   {"a.events.json": `{"events":[{"subject":"erp.a.b.v1","x-aggregate-type":"erp.a","x-transaction-document":true,"payload":{"type":"object"}}]}`},
		"bad payload schema":  {"a.events.json": `{"events":[{"subject":"erp.a.b.v1","x-aggregate-type":"erp.a","payload":{"type":"object","minProperties":"x"}}]}`},
		"duplicate subject":   {"a.events.json": `{"events":[{"subject":"erp.a.b.v1","x-aggregate-type":"erp.a","payload":{"type":"object"}}]}`, "b.events.json": `{"events":[{"subject":"erp.a.b.v1","x-aggregate-type":"erp.a","payload":{"type":"object"}}]}`},
		"duplicate same file": {"a.events.json": `{"events":[{"subject":"erp.a.b.v1","x-aggregate-type":"erp.a","payload":{"type":"object"}},{"subject":"erp.a.b.v1","x-aggregate-type":"erp.a","payload":{"type":"object"}}]}`},
	}
	for name, files := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := LoadContract(contractFS(files))
			require.Error(t, err)
		})
	}
}

func TestLoadContractWithoutFilesIsEmpty(t *testing.T) {
	c, err := LoadContract(fstest.MapFS{})
	require.NoError(t, err)
	require.Empty(t, c.Subjects())
}
