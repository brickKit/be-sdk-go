// Package db is the only way a module reaches its database (protocol 14, "database"): one transaction entry that
// switches to the member's role and schema with SET LOCAL and sets per-transaction timeouts; pools per database
// target, owned by the launcher or the shell; startup checks of the durable settings; the migrate entry.
package db

import (
	"fmt"
	"time"

	"github.com/brickKit/be-sdk-go/v3/config"
)

// ServeKeys are the shared connection keys of protocol chapter 02 that serve reads. The owner role and the direct
// migration address are not among them: the service process never parses the owner's credentials (P5.11).
func ServeKeys() []config.Key {
	return []config.Key{
		{Name: "PG_HOST", Type: config.String, Required: true},
		{Name: "PG_PORT", Type: config.Integer, Default: 5432},
		{Name: "PG_DATABASE", Type: config.String, Required: true},
		{Name: "PG_USER", Type: config.String, Required: true},
		{Name: "PG_PASSWORD", Type: config.String, Required: true, Secret: true},
		{Name: "PG_SCHEMA", Type: config.String, Required: true},
		{Name: "PG_POOLER_PREPARED", Type: config.Boolean, Default: true},
		{Name: "PG_STATEMENT_TIMEOUT", Type: config.Duration, Default: 5 * time.Second},
		{Name: "PG_LOCK_TIMEOUT", Type: config.Duration, Default: 2 * time.Second},
		{Name: "PG_IDLE_IN_TX_TIMEOUT", Type: config.Duration, Default: 10 * time.Second},
		{Name: "PG_POOL_FOREGROUND", Type: config.Integer, Default: 8},
		{Name: "PG_POOL_BACKGROUND", Type: config.Integer, Default: 4},
	}
}

// MigrateKeys are what the migrate entry reads: the serve keys plus the direct address and the owner role. It is the
// full set a component with a database declares in its manifest.
func MigrateKeys() []config.Key {
	return append(ServeKeys(),
		config.Key{Name: "PG_MIGRATION_HOST", Type: config.String},
		config.Key{Name: "PG_MIGRATION_PORT", Type: config.Integer},
		config.Key{Name: "PG_OWNER_USER", Type: config.String},
		config.Key{Name: "PG_OWNER_PASSWORD", Type: config.String, Secret: true})
}

// Target identifies a database: members with the same target share one physical pool in a shell (protocol 14).
type Target struct {
	Host     string
	Port     int
	Database string
}

func (t Target) String() string { return fmt.Sprintf("%s:%d/%s", t.Host, t.Port, t.Database) }

// Timeouts are set in every transaction (never per role: after SET ROLE PostgreSQL ignores ALTER ROLE ... SET).
type Timeouts struct {
	Statement, Lock, IdleInTx time.Duration
}

// Settings are one member's database settings, read from its configuration.
type Settings struct {
	Target   Target
	User     string         // the member's runtime role (the login role when running alone)
	Password *config.Secret // re-read from its file when rotated
	Schema   string
	Prepared bool // false behind a transaction-mode pooler without protocol-level prepared statements
	Timeouts Timeouts
	// Foreground and Background are the member's connection budget: the most connections each side may hold.
	Foreground, Background int
}

// SettingsFrom reads the member's settings from its configuration values.
func SettingsFrom(v *config.Values) Settings {
	return Settings{
		Target:   Target{Host: v.String("PG_HOST"), Port: int(v.Int("PG_PORT")), Database: v.String("PG_DATABASE")},
		User:     v.String("PG_USER"),
		Password: v.Secret("PG_PASSWORD"),
		Schema:   v.String("PG_SCHEMA"),
		Prepared: v.Bool("PG_POOLER_PREPARED"),
		Timeouts: Timeouts{Statement: v.Duration("PG_STATEMENT_TIMEOUT"), Lock: v.Duration("PG_LOCK_TIMEOUT"),
			IdleInTx: v.Duration("PG_IDLE_IN_TX_TIMEOUT")},
		Foreground: int(v.Int("PG_POOL_FOREGROUND")), Background: int(v.Int("PG_POOL_BACKGROUND")),
	}
}
