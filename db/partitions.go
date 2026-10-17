package db

import "context"

// MaintainPartitions creates the declared tables' missing monthly partitions inside tx (chapter 05, P5.22): the
// current month through the declared months ahead, as the schema owner through the SDK's restricted function. A
// mismatched, detached or overlapping partition fails it. It returns the partitions it created.
func MaintainPartitions(ctx context.Context, tx Tx) ([]string, error) {
	rows, err := tx.Query(ctx, "SELECT be_maintain_partitions()")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var created []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		created = append(created, name)
	}
	return created, rows.Err()
}
