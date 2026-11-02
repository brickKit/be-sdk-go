package db

import (
	"context"
	"log/slog"
	"time"
)

// StaleCheckEvery spaces the calls of AnalyzeStale (P5.33): the window in which a cached plan settled on a small
// table still runs after a backlog arrived is at most this, against up to a naptime (60 s) waiting for autovacuum.
const StaleCheckEvery = 2 * time.Second

// AnalyzeStale analyzes the SDK's tables whose statistics lag behind their changes (chapter 05, P5.33), as the schema
// owner through the SDK's function; it returns the tables it analyzed.
func AnalyzeStale(ctx context.Context, d *DB) ([]string, error) {
	var done []string
	err := d.BackgroundTx(ctx, func(ctx context.Context, tx Tx) error {
		done = done[:0]
		rows, err := tx.Query(ctx, "SELECT be_analyze_stale()")
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				return err
			}
			done = append(done, name)
		}
		return rows.Err()
	}, StatementTimeout(time.Minute)) // an analyze samples a bounded number of rows; a large table may take seconds
	return done, err
}

// KeepStatistics calls AnalyzeStale every StaleCheckEvery until ctx ends (P5.33). A failed call is logged and tried
// again at the next tick.
func KeepStatistics(ctx context.Context, d *DB, log *slog.Logger) {
	tick := time.NewTicker(StaleCheckEvery)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			analyzed, err := AnalyzeStale(ctx, d)
			switch {
			case ctx.Err() != nil:
				return
			case err != nil:
				log.Warn("the statistics of the SDK's tables could not be refreshed", "error", err.Error())
			case len(analyzed) > 0:
				log.Debug("analyzed tables whose statistics lagged", "tables", analyzed)
			}
		}
	}
}
