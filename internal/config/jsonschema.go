package config

import (
	"bytes"
	"io/fs"
	"strings"

	beprotocol "github.com/brickKit/be-protocol"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

// schemaBaseURL is the $id prefix of be-protocol's schemas; it maps onto schemas/ in beprotocol.FS.
const schemaBaseURL = "https://github.com/brickKit/be-protocol/schemas/"

// protocolLoader resolves be-protocol schema URLs from the embedded FS, never from the network.
type protocolLoader struct{}

// Load implements jsonschema.URLLoader.
func (protocolLoader) Load(url string) (any, error) {
	name, ok := strings.CutPrefix(url, schemaBaseURL)
	if !ok {
		return nil, errText("schema outside be-protocol: " + url)
	}
	b, err := fs.ReadFile(beprotocol.FS, "schemas/"+name)
	if err != nil {
		return nil, err
	}
	return jsonschema.UnmarshalJSON(bytes.NewReader(b))
}

// jsonValidator validates json values against the be-protocol schema the catalogue names
// (json_schema: JOBS_OVERRIDES, DATA_LIFECYCLE). One per Load; compiled schemas are cached in it.
type jsonValidator struct {
	compiler *jsonschema.Compiler
	compiled map[string]*jsonschema.Schema
}

func newJSONValidator() *jsonValidator {
	c := jsonschema.NewCompiler()
	c.UseLoader(jsonschema.SchemeURLLoader{"https": protocolLoader{}})
	return &jsonValidator{compiler: c, compiled: map[string]*jsonschema.Schema{}}
}

// validate checks doc (compact JSON) against schemas/<file>; the error names only the key.
func (jv *jsonValidator) validate(key, file string, doc []byte) *Error {
	sch, ok := jv.compiled[file]
	if !ok {
		var err error
		sch, err = jv.compiler.Compile(schemaBaseURL + file)
		if err != nil {
			return newErr(ReasonInvalid, key, "schema "+file+" does not compile")
		}
		jv.compiled[file] = sch
	}
	inst, err := jsonschema.UnmarshalJSON(bytes.NewReader(doc))
	if err != nil {
		return newErr(ReasonInvalid, key, "not JSON")
	}
	if err := sch.Validate(inst); err != nil {
		return newErr(ReasonInvalid, key, "does not satisfy "+file)
	}
	return nil
}
