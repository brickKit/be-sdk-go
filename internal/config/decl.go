package config

// Protocol value formats (P2.3, schemas/config-keys.yaml "format").
const (
	FormatString    = "string"
	FormatInt       = "int"
	FormatBool      = "bool"
	FormatDuration  = "duration"
	FormatDurations = "durations"
	FormatURL       = "url"
	FormatJSON      = "json"
	FormatEnum      = "enum"
)

// Decl is one declared configuration key: a configSchema item merged with the catalogue entry when the
// key is a protocol key (P2.8). It drives how Load parses the key's value (P2.3).
type Decl struct {
	Name         string   // the environment variable name
	Type         string   // brickKit configSchema type: string, integer, boolean, object, array
	Format       string   // protocol format, one of the Format* constants
	Required     bool     // absent or empty is CONFIG_MISSING (P2.3)
	Default      *string  // nil = no default; injected as written, parsed like a value
	DefaultFrom  string   // when absent or empty: take this other declared key's value
	ShellDefault *string  // the catalogue's default in a shell's own configuration (PG_POOL_MAX)
	Secret       bool     // a file-delivered secret: the value is the file's absolute path (P2.7)
	Enum         []string // allowed values of an enum key
	Schemes      []string // allowed URL schemes of a url key; empty = any
	JSONKind     string   // JSONObject, JSONArray or "" for a json key
	JSONSchema   string   // file under be-protocol schemas/ the json value must satisfy; "" = none
	OneOf        []string // a group of keys of which at least one must be set
	Minimum      *int64   // lowest allowed value of an int key
	Protocol     bool     // a catalogue key (P2.8) rather than the component's own
}

// defaultText returns the default to inject, if any. An empty default means "no value", for every
// format (P2.3: empty == not set; OTEL_BASE_URL, JOBS_OVERRIDES).
func (d Decl) defaultText() (string, bool) {
	if d.Default == nil || *d.Default == "" {
		return "", false
	}
	return *d.Default, true
}
