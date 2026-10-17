// Package mergesafety finds the code that breaks when a module shares a process with other members of a shell
// (component implementation conventions, "merge safety"): reading the process environment, initialising process-wide
// state, exiting the process, and building engines, listeners or connections behind the SDK's back.
//
// It parses Go files and resolves import paths, so `env := os.Getenv` style aliases of the package are caught too;
// it is a lint, not a proof (reflection or a call through a function value escape it).
package mergesafety

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// Rule is one forbidden call.
type Rule struct {
	Import string // import path
	Name   string // selector; "*" for any member of the package
	Why    string
}

// Rules is the list the check applies.
var Rules = []Rule{
	{"os", "Getenv", "reads the process environment, shared by every member of a shell"},
	{"os", "LookupEnv", "reads the process environment, shared by every member of a shell"},
	{"os", "Environ", "reads the process environment, shared by every member of a shell"},
	{"os", "Setenv", "changes the process environment of every member"},
	{"os", "Exit", "exits the whole shell; return an error instead"},
	{"log", "Fatal", "exits the whole shell; return an error instead"},
	{"log", "Fatalf", "exits the whole shell; return an error instead"},
	{"log", "Fatalln", "exits the whole shell; return an error instead"},
	{"log", "Panic", "use errors; the SDK recovers panics but this one is deliberate"},
	{"log/slog", "SetDefault", "sets the process-wide logger; use the runtime's logger"},
	{"go.opentelemetry.io/otel", "SetTracerProvider", "sets the process-wide tracer provider; use the runtime's tracer"},
	{"go.opentelemetry.io/otel", "SetTextMapPropagator", "sets the process-wide propagator"},
	{"go.opentelemetry.io/otel", "SetMeterProvider", "sets the process-wide meter provider"},
	{"github.com/prometheus/client_golang/prometheus", "MustRegister", "registers on the default registry; the second member crashes"},
	{"github.com/prometheus/client_golang/prometheus", "Register", "registers on the default registry; the second member crashes"},
	{"github.com/prometheus/client_golang/prometheus", "DefaultRegisterer", "the default registry is process-wide"},
	{"github.com/prometheus/client_golang/prometheus/promauto", "*", "promauto registers on the default registry"},
	{"github.com/gin-gonic/gin", "SetMode", "Gin's mode is process-wide; the SDK sets it"},
	{"github.com/gin-gonic/gin", "Default", "builds an engine outside the SDK's middleware chain"},
	{"github.com/gin-gonic/gin", "New", "builds an engine outside the SDK's middleware chain"},
	{"os/signal", "Notify", "signal handling belongs to the launcher"},
	{"os/signal", "NotifyContext", "signal handling belongs to the launcher"},
	{"net/http", "ListenAndServe", "a module never listens; the launcher or the shell does"},
	{"net/http", "ListenAndServeTLS", "a module never listens; the launcher or the shell does"},
	{"net", "Listen", "a module never listens; the launcher or the shell does"},
	{"database/sql", "Open", "opens a pool outside the SDK; use the runtime's database"},
	{"database/sql", "OpenDB", "opens a pool outside the SDK; use the runtime's database"},
	{"github.com/jackc/pgx/v5/pgxpool", "New", "opens a pool outside the SDK; use the runtime's database"},
	{"github.com/jackc/pgx/v5/pgxpool", "NewWithConfig", "opens a pool outside the SDK; use the runtime's database"},
	{"github.com/jackc/pgx/v5", "Connect", "opens a connection outside the SDK; use the runtime's database"},
	{"github.com/jackc/pgx/v5", "ConnectConfig", "opens a connection outside the SDK; use the runtime's database"},
	{"github.com/jackc/pgx/v5", "ConnectWithOptions", "opens a connection outside the SDK; use the runtime's database"},
	{"github.com/jackc/pgx/v5/pgconn", "Connect", "opens a connection outside the SDK; use the runtime's database"},
	{"github.com/jackc/pgx/v5/pgconn", "ConnectConfig", "opens a connection outside the SDK; use the runtime's database"},
	{"github.com/jackc/pgx/v5/stdlib", "OpenDB", "opens a pool outside the SDK; use the runtime's database"},
	{"github.com/jackc/pgx/v5/stdlib", "OpenDBFromPool", "opens a pool outside the SDK; use the runtime's database"},
	{"github.com/nats-io/nats.go", "Connect", "opens a bus connection outside the SDK; use the runtime's bus"},
}

// Finding is one violation.
type Finding struct {
	Pos  token.Position
	Call string
	Why  string
}

func (f Finding) String() string { return fmt.Sprintf("%s: %s — %s", f.Pos, f.Call, f.Why) }

// CheckDir checks the non-test Go files of one directory (not recursive).
func CheckDir(dir string) ([]Finding, error) {
	fset := token.NewFileSet()
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var out []Finding
	for _, e := range entries {
		n := e.Name()
		if e.IsDir() || !strings.HasSuffix(n, ".go") || strings.HasSuffix(n, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, filepath.Join(dir, n), nil, parser.SkipObjectResolution)
		if err != nil {
			return nil, err
		}
		out = append(out, checkFile(fset, f)...)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Pos.String() < out[j].Pos.String() })
	return out, nil
}

// CheckTree checks every package under root, skipping directories named in skip (relative to root) and testdata.
func CheckTree(root string, skip ...string) ([]Finding, error) {
	var out []Finding
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil || !d.IsDir() {
			return err
		}
		rel, _ := filepath.Rel(root, path)
		if d.Name() == "testdata" || strings.HasPrefix(d.Name(), ".") && rel != "." {
			return filepath.SkipDir
		}
		for _, s := range skip {
			if rel == s {
				return filepath.SkipDir
			}
		}
		f, err := CheckDir(path)
		out = append(out, f...)
		return err
	})
	return out, err
}

func checkFile(fset *token.FileSet, f *ast.File) []Finding {
	local := map[string]string{} // local name -> import path
	for _, im := range f.Imports {
		p, _ := strconv.Unquote(im.Path.Value)
		name := p[strings.LastIndex(p, "/")+1:]
		if strings.HasPrefix(name, "v") && strings.Count(p, "/") > 0 && isMajor(name) {
			q := strings.TrimSuffix(p, "/"+name)
			name = q[strings.LastIndex(q, "/")+1:]
		}
		if p == "github.com/nats-io/nats.go" {
			name = "nats"
		}
		if im.Name != nil {
			name = im.Name.Name
		}
		local[name] = p
	}
	var out []Finding
	ast.Inspect(f, func(n ast.Node) bool {
		sel, ok := n.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		id, ok := sel.X.(*ast.Ident)
		if !ok {
			return true
		}
		path, ok := local[id.Name]
		if !ok {
			return true
		}
		for _, r := range Rules {
			if r.Import == path && (r.Name == "*" || r.Name == sel.Sel.Name) {
				out = append(out, Finding{Pos: fset.Position(sel.Pos()), Call: path + "." + sel.Sel.Name, Why: r.Why})
			}
		}
		return true
	})
	return out
}

func isMajor(s string) bool {
	if len(s) < 2 {
		return false
	}
	_, err := strconv.Atoi(s[1:])
	return err == nil
}
