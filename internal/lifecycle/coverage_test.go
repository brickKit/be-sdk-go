package lifecycle

import (
	"io/fs"

	beprotocol "github.com/brickKit/be-protocol"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/require"
)

func TestCreatedTables(t *testing.T) {
	got, err := createdTables(fstest.MapFS{
		"1_init.up.sql": {Data: []byte(`-- CREATE TABLE commented_out (id int);
CREATE TABLE orders (id uuid PRIMARY KEY);
/* CREATE TABLE in_block_comment (id int); */
create table if not exists "order_items" (id uuid);
CREATE TABLE "MixedCase" (id int);
CREATE UNLOGGED TABLE scratch (id int);
CREATE TABLE besdk_private (id int);
CREATE TEMP TABLE tmp (id int);
CREATE TABLE public_x AS SELECT 1;`)},
		"1_init.down.sql": {Data: []byte(`CREATE TABLE down_only (id int);`)},
		"2_drop.up.sql":   {Data: []byte(`DROP TABLE IF EXISTS scratch; DROP TABLE public_x CASCADE;`)},
		"10_more.up.sql":  {Data: []byte(`CREATE TABLE later (id int) PARTITION BY RANGE (id);`)},
		"lifecycle.yaml":  {Data: []byte(`CREATE TABLE not_sql (id int);`)},
	})
	require.NoError(t, err)
	require.Equal(t, []string{"MixedCase", "later", "order_items", "orders"}, got)
}

func TestLoadChecksEveryCreatedTableIsDeclared(t *testing.T) {
	fsys := fstest.MapFS{
		"1_init.up.sql":   {Data: []byte(`CREATE TABLE orders (id uuid PRIMARY KEY); CREATE TABLE notes (id uuid PRIMARY KEY);`)},
		"1_init.down.sql": {Data: []byte(`DROP TABLE notes; DROP TABLE orders;`)},
		"lifecycle.yaml":  {Data: []byte("lifecycle: v1\ntables:\n  orders: {class: master}\n")},
	}
	_, err := Load(fsys)
	require.ErrorContains(t, err, "table notes: created by 1_init.up.sql but not declared")

	fsys["lifecycle.yaml"] = &fstest.MapFile{Data: []byte("lifecycle: v1\ntables:\n  orders: {class: master}\n  notes: {class: master}\n  ghost: {class: master}\n")}
	_, err = Load(fsys)
	require.ErrorContains(t, err, "table ghost: declared but no migration creates it")

	fsys["lifecycle.yaml"] = &fstest.MapFile{Data: []byte("lifecycle: v1\ntables:\n  orders: {class: master}\n  notes: {class: master}\n")}
	d, err := Load(fsys)
	require.NoError(t, err)
	require.Equal(t, []string{"notes", "orders"}, d.Names())
}

func TestLoadNeedsTheDeclaration(t *testing.T) {
	_, err := Load(fstest.MapFS{"1_x.up.sql": {Data: []byte(`SELECT 1;`)}})
	require.ErrorContains(t, err, "lifecycle.yaml")
}

// The widget fixture's reference schema, in golang-migrate's layout, is exactly its declaration.
func TestLoadWidgetFixture(t *testing.T) {
	d, err := Load(widgetFS(t))
	require.NoError(t, err)
	require.Len(t, d.Names(), 8)
}

func widgetFS(t *testing.T) fstest.MapFS {
	sql, err := fs.ReadFile(beprotocol.FS, "fixtures/widget/migrations/0001_widget.sql")
	require.NoError(t, err)
	return fstest.MapFS{
		"0001_widget.up.sql":   {Data: sql},
		"0001_widget.down.sql": {Data: []byte("SELECT 1;")},
		"lifecycle.yaml":       {Data: widgetYAML(t)},
	}
}
