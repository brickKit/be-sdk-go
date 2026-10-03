package lifecycle

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseConfigDefaults(t *testing.T) {
	for _, v := range []string{"", `{"mode":"on"}`, "mode: on\n", "{}"} {
		c, err := ParseConfig(v)
		require.NoError(t, err, v)
		require.Equal(t, ModeOn, c.Mode, v)
		require.Equal(t, "none", c.ColdStore)
		require.Equal(t, "none", c.ColdQuery)
		require.Equal(t, "none", c.Publisher)
		require.Equal(t, "plain", c.PII)
	}
}

// YAML 1.2: unquoted on / off are the strings "on" / "off" (P16.9).
func TestParseConfigModes(t *testing.T) {
	for v, want := range map[string]Mode{"mode: off": ModeOff, "mode: dry-run": ModeDryRun, `{"mode": "off"}`: ModeOff, "mode: \"on\"": ModeOn} {
		c, err := ParseConfig(v)
		require.NoError(t, err, v)
		require.Equal(t, want, c.Mode, v)
	}
	_, err := ParseConfig("mode: maybe")
	require.ErrorContains(t, err, "DATA_LIFECYCLE")
	_, err = ParseConfig("mode: [")
	require.ErrorContains(t, err, "DATA_LIFECYCLE")
}

// An adapter this SDK version does not have fails the start naming it (P16.9).
func TestParseConfigRefusesMissingAdapters(t *testing.T) {
	for v, name := range map[string]string{
		"cold_store: s3-parquet":      "cold_store s3-parquet",
		"cold_store: iceberg":         "cold_store iceberg",
		"cold_query: scan":            "cold_query scan",
		"publisher: watermark":        "publisher watermark",
		"pii: crypto-shred":           "pii crypto-shred",
		"tenants: {t1: {tables: {}}}": "tenants",
	} {
		_, err := ParseConfig(v)
		require.ErrorContains(t, err, name, v)
		require.ErrorContains(t, err, "not supported by this SDK version", v)
	}
}

func applied(t *testing.T, overrides string) (*Declaration, error) {
	t.Helper()
	d, err := Parse(widgetYAML(t))
	require.NoError(t, err)
	c, err := ParseConfig(overrides)
	require.NoError(t, err)
	return c.Apply(d)
}

func TestOverrideLengthensRetention(t *testing.T) {
	d, err := applied(t, "tables: {widgets: {retention: {min: 15y after closed}}, widget_jobs: {retention: {min: forever}}}")
	require.NoError(t, err)
	require.Equal(t, &After{Span{15, "y"}, "closed"}, d.Tables["widgets"].RetentionMin)
	require.True(t, d.Tables["widget_jobs"].Forever)
	require.Nil(t, d.Tables["widget_jobs"].RetentionMin)

	same, err := applied(t, "tables: {widgets: {retention: {min: 120mo after closed}}}")
	require.NoError(t, err, "equal is not shorter")
	require.Equal(t, &After{Span{120, "mo"}, "closed"}, same.Tables["widgets"].RetentionMin)

	orig, err := Parse(widgetYAML(t))
	require.NoError(t, err)
	require.Equal(t, &After{Span{10, "y"}, "closed"}, orig.Tables["widgets"].RetentionMin, "Apply never changes its input")
}

func TestOverrideNeverShortens(t *testing.T) {
	for v, table := range map[string]string{
		"tables: {widgets: {retention: {min: 5y after closed}}}":       "widgets",
		"tables: {widgets: {retention: {min: 10y after created}}}":     "widgets",
		"tables: {widget_jobs: {retention: {min: 29d after created}}}": "widget_jobs",
		"tables: {widget_ledger: {tiers: {seal: on_signal}}}":          "widget_ledger",
		"tables: {widgets: {tiers: {seal: 12mo after closed}}}":        "widgets",
		"tables: {widgets: {tiers: {cold: 4y after closed}}}":          "widgets",
		"tables: {nowhere: {tiers: {hot: 30d}}}":                       "nowhere",
	} {
		_, err := applied(t, v)
		require.ErrorContains(t, err, "table "+table+":", v)
	}
}

func TestOverridePostponesSealAndColdAndSetsHot(t *testing.T) {
	d, err := applied(t, "tables: {widgets: {tiers: {seal: 24mo after closed, cold: never, hot: 30d}}}")
	require.NoError(t, err)
	w := d.Tables["widgets"]
	require.Equal(t, Seal{Kind: SealAfter, After: After{Span{24, "mo"}, "closed"}}, w.Seal)
	require.Equal(t, "never", w.Cold)
	require.Equal(t, "30d", w.Hot)
	require.Equal(t, w.Seal, d.Tables["widget_lines"].Seal, "a follower is sealed with its parent")
}
