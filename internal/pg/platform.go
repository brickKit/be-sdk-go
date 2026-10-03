package pg

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"sort"
	"strings"

	beprotocol "github.com/brickKit/be-protocol"
	"github.com/golang-migrate/migrate/v4/source"
)

// PlatformVersion is the version of the platform migration this SDK applies (P11.3): version 1 is
// be-protocol 1.0's reference DDL.
const PlatformVersion = 1

// platformSQL is platform migration version 1: every ddl/[0-9]*.sql of the pinned be-protocol in name
// order (be_bus.sql is the project's, never a component's), then the component's row in
// besdk_platform_version (P11.3).
func platformSQL(componentID string) (string, error) {
	names, err := fs.Glob(beprotocol.FS, "ddl/[0-9]*.sql")
	if err != nil {
		return "", err
	}
	if len(names) == 0 {
		return "", errors.New("be-protocol ddl/ holds no reference DDL")
	}
	sort.Strings(names)
	var b strings.Builder
	for _, n := range names {
		body, err := fs.ReadFile(beprotocol.FS, n)
		if err != nil {
			return "", err
		}
		fmt.Fprintf(&b, "-- be-sdk-go platform migration: %s\n%s\n", n, strings.TrimRight(string(body), "\n"))
		b.WriteString("\n")
	}
	fmt.Fprintf(&b, "INSERT INTO besdk_platform_version (component, version) VALUES (%s, %d)"+
		" ON CONFLICT (component) DO UPDATE SET version = EXCLUDED.version, applied_at = now();\n",
		quoteLiteral(componentID), PlatformVersion)
	return b.String(), nil
}

// platformSource is a golang-migrate source with the platform migration as its only, up-only version.
type platformSource struct{ body string }

func newPlatformSource(componentID string) (*platformSource, error) {
	body, err := platformSQL(componentID)
	if err != nil {
		return nil, err
	}
	return &platformSource{body: body}, nil
}

var _ source.Driver = (*platformSource)(nil)

func notExist(op string) error {
	return &os.PathError{Op: op, Path: "besdk_platform", Err: os.ErrNotExist}
}

// Open is unused: the source is built with newPlatformSource.
func (p *platformSource) Open(string) (source.Driver, error) {
	return nil, errors.New("platform source: Open is not supported")
}

// Close releases nothing.
func (p *platformSource) Close() error { return nil }

// First is the one platform version.
func (p *platformSource) First() (uint, error) { return PlatformVersion, nil }

// Prev: there is no version before the first.
func (p *platformSource) Prev(uint) (uint, error) { return 0, notExist("prev") }

// Next: there is no version after the last.
func (p *platformSource) Next(uint) (uint, error) { return 0, notExist("next") }

// ReadUp returns the platform migration's SQL.
func (p *platformSource) ReadUp(v uint) (io.ReadCloser, string, error) {
	if v != PlatformVersion {
		return nil, "", notExist("read up")
	}
	return io.NopCloser(strings.NewReader(p.body)), "besdk_platform", nil
}

// ReadDown: the platform migration never goes down.
func (p *platformSource) ReadDown(uint) (io.ReadCloser, string, error) {
	return nil, "", notExist("read down")
}
