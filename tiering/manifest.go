package tiering

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/brickKit/be-sdk-go/v3/canon"
)

// The object key and the manifest (be-protocol P15.15, P15.16).

// ObjectKey is where one part of one table of a unit's copy goes: under the component's prefix, the class, the unit,
// "<generation>.<manifest version>"; written once, never overwritten.
func ObjectKey(prefix, class, unit string, generation int64, version int, table string, part int) string {
	return fmt.Sprintf("%s/%s/%s/%d.%d/%s-%d.parquet", prefix, class, unit, generation, version, table, part)
}

// ManifestKey is where an object-parquet manifest's copy goes, beside its parts (P15.16).
func ManifestKey(prefix, class, unit string, generation int64, version int) string {
	return fmt.Sprintf("%s/%s/%s/%d.%d/manifest.json", prefix, class, unit, generation, version)
}

// Formats.
const (
	FormatParquet = "parquet-1"
	FormatPgRows  = "pg-rows-1"
)

// Manifest is be.tier.manifest (P15.16). Counts are decimal strings, as the canonical encoding keeps every integer
// that may pass 2^53.
type Manifest struct {
	RecordType    string          `json:"record_type"`
	Schema        string          `json:"schema"`
	Unit          string          `json:"unit"`
	Class         string          `json:"class"`
	Version       int             `json:"version"`
	Route         string          `json:"route"`
	Generation    int64           `json:"generation"`
	Epoch         string          `json:"epoch"`
	Format        string          `json:"format"`
	SchemaVersion string          `json:"schema_version"`
	DigestVersion int             `json:"digest_version"`
	Digest        string          `json:"digest"`
	Tables        []ManifestTable `json:"tables"`
	CreatedAt     string          `json:"created_at"`
}

// ManifestTable is one table in a manifest.
type ManifestTable struct {
	Name    string           `json:"name"`
	PK      []string         `json:"pk"`
	Columns []ManifestColumn `json:"columns"`
	Rows    string           `json:"rows"`
	Objects []ManifestObject `json:"objects,omitempty"`
}

// ManifestColumn is one column's name, type and nullability.
type ManifestColumn struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable bool   `json:"nullable"`
}

// ManifestObject is one object of a table: its key, rows, bytes and SHA-256 (hex).
type ManifestObject struct {
	Key    string `json:"key"`
	Rows   string `json:"rows"`
	Bytes  string `json:"bytes"`
	SHA256 string `json:"sha256"`
}

// ManifestRecordType is the manifest's record type.
const ManifestRecordType = "be.tier.manifest"

// Canonical is the manifest's canonical bytes.
func (m Manifest) Canonical() ([]byte, error) {
	raw, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	var v any
	if err := d.Decode(&v); err != nil {
		return nil, err
	}
	return canon.Encode(v)
}
