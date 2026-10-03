package pg

import (
	"errors"
	"io/fs"
	"os"

	"github.com/brickKit/be-sdk-go/internal/problem"
	"github.com/golang-migrate/migrate/v4/source"
	"github.com/golang-migrate/migrate/v4/source/iofs"
)

// LatestVersion is the highest component migration version in component (files
// `<version>_<name>.up.sql` / `.down.sql` at its root; lifecycle.yaml and every other file are
// ignored); 0 when there is none (P1.4: the image's migration version).
func LatestVersion(component fs.FS) (uint, error) {
	src, err := iofs.New(component, ".")
	if err != nil {
		return 0, problem.Wrap(err, "INTERNAL", nil)
	}
	defer func() { _ = src.Close() }()
	return lastVersion(src)
}

// lastVersion walks a source to its last version.
func lastVersion(src source.Driver) (uint, error) {
	v, err := src.First()
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, problem.Wrap(err, "INTERNAL", nil)
	}
	for {
		next, err := src.Next(v)
		if errors.Is(err, os.ErrNotExist) {
			return v, nil
		}
		if err != nil {
			return 0, problem.Wrap(err, "INTERNAL", nil)
		}
		v = next
	}
}
