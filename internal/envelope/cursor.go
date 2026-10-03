package envelope

// Cursor is the state-mode position of one (consumer, aggregate type, aggregate id) stream in
// besdk_event_cursor (P12.6). The zero value is "no row yet".
type Cursor struct {
	Version int64 // the last applied aggregate version
	Exists  bool  // false while no event of this aggregate has been applied
}

// Advance applies the state-mode rule (P12.6, vectors envelope/cursor): a version is applied only
// when it is greater than the cursor; an equal or older one is a duplicate or stale and is skipped
// (acknowledged without running the handler). It returns the cursor after the delivery and whether
// the handler runs. The real cursor is the conditional upsert of ddl/03-event-cursor.sql
// (… DO UPDATE … WHERE besdk_event_cursor.version < EXCLUDED.version RETURNING 1), in the handler's
// transaction; this function states the same rule without a database. Shuffled and duplicated
// deliveries end in the same state as one in-order delivery.
func (c Cursor) Advance(version int64) (Cursor, bool) {
	if c.Exists && version <= c.Version {
		return c, false
	}
	return Cursor{Version: version, Exists: true}, true
}
