package rpc

// DefaultMaxItems is the batch limit of a repeated field without (be.v1.max_items) (P7.10).
const DefaultMaxItems = 500

// Chunk splits items into consecutive chunks of at most max items, so a caller never sends a batch
// over a dependency's limit (P7.10). A max of zero or less means DefaultMaxItems. Each chunk has its
// own capacity, so appending to one never overwrites the next.
func Chunk[T any](items []T, max int) [][]T {
	if max <= 0 {
		max = DefaultMaxItems
	}
	var out [][]T
	for start := 0; start < len(items); start += max {
		end := min(start+max, len(items))
		out = append(out, items[start:end:end])
	}
	return out
}
