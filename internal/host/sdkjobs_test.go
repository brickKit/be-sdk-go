package host

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Every SDK function that makes a work.Job is registered by the host: a job the host never adds never runs, and
// nothing else fails (foundations item 5 found a tiering job made and tested but not registered). The check is by
// name: each maker is called in this package's non-test code, and a method name several types share (audit's and
// extcmd's Job) is called at least as many times as there are such makers.

// jobMaker is one function or method of the SDK whose results include a work.Job.
type jobMaker struct {
	pkg, name, recv string // recv is empty for a function
}

func (j jobMaker) String() string {
	if j.recv != "" {
		return j.pkg + ".(" + j.recv + ")." + j.name
	}
	return j.pkg + "." + j.name
}

// returnsJob reports whether a result list names work.Job (Job inside package work).
func returnsJob(pkg string, results *ast.FieldList) bool {
	if results == nil {
		return false
	}
	for _, f := range results.List {
		switch t := f.Type.(type) {
		case *ast.SelectorExpr:
			if x, ok := t.X.(*ast.Ident); ok && x.Name == "work" && t.Sel.Name == "Job" {
				return true
			}
		case *ast.Ident:
			if pkg == "work" && t.Name == "Job" {
				return true
			}
		}
	}
	return false
}

func recvName(fd *ast.FuncDecl) string {
	if fd.Recv == nil || len(fd.Recv.List) == 0 {
		return ""
	}
	t := fd.Recv.List[0].Type
	if s, ok := t.(*ast.StarExpr); ok {
		t = s.X
	}
	if id, ok := t.(*ast.Ident); ok {
		return id.Name
	}
	return "?"
}

// sdkJobMakers lists the job makers in the SDK's non-test code, outside the test helpers.
func sdkJobMakers(t *testing.T) []jobMaker {
	root, err := filepath.Abs("../..")
	require.NoError(t, err)
	var out []jobMaker
	require.NoError(t, filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if n := d.Name(); n == "testdata" || n == "sectest" || n == "conformance" || strings.HasPrefix(n, ".") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		f, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.SkipObjectResolution)
		if err != nil {
			return err
		}
		for _, decl := range f.Decls {
			if fd, ok := decl.(*ast.FuncDecl); ok && returnsJob(f.Name.Name, fd.Type.Results) {
				out = append(out, jobMaker{pkg: f.Name.Name, name: fd.Name.Name, recv: recvName(fd)})
			}
		}
		return nil
	}))
	return out
}

// hostCalls counts the calls in this package's non-test code: a package's function as "pkg.Name", a method as
// ".Name", this package's own function as "host.Name".
func hostCalls(t *testing.T) map[string]int {
	calls := map[string]int{}
	entries, err := os.ReadDir(".")
	require.NoError(t, err)
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		f, err := parser.ParseFile(token.NewFileSet(), e.Name(), nil, parser.SkipObjectResolution)
		require.NoError(t, err)
		imported := map[string]bool{} // the file's package names: pkg.Name is a function, not a method
		for _, im := range f.Imports {
			name := strings.Trim(im.Path.Value, `"`)
			name = name[strings.LastIndex(name, "/")+1:]
			if im.Name != nil {
				name = im.Name.Name
			}
			imported[name] = true
		}
		ast.Inspect(f, func(n ast.Node) bool {
			c, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			switch fn := c.Fun.(type) {
			case *ast.Ident:
				calls["host."+fn.Name]++
			case *ast.SelectorExpr:
				if x, ok := fn.X.(*ast.Ident); ok && imported[x.Name] {
					calls[x.Name+"."+fn.Sel.Name]++
				} else {
					calls["."+fn.Sel.Name]++
				}
			}
			return true
		})
	}
	return calls
}

func TestTheHostRegistersEverySDKJob(t *testing.T) {
	makers := sdkJobMakers(t)
	require.GreaterOrEqual(t, len(makers), 10, "the check finds the SDK's job makers (else it checks nothing)")
	calls := hostCalls(t)
	shared := map[string]int{} // a method name -> how many types have a job-making method of that name
	for _, m := range makers {
		if m.recv != "" {
			shared[m.name]++
		}
	}
	var missing []string
	for _, m := range makers {
		switch {
		case m.recv == "" && calls[m.pkg+"."+m.name] == 0:
			missing = append(missing, m.String())
		case m.recv != "" && calls["."+m.name] < shared[m.name]:
			missing = append(missing, m.String())
		}
	}
	sort.Strings(missing)
	assert.Empty(t, missing, "job makers the host does not register (internal/host): add them where the host adds the "+
		"SDK's jobs (addSDKJob), with their recovery rule")
}
