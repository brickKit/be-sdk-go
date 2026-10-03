package rpc

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestChunkSplitsIntoPiecesOfAtMostMax(t *testing.T) {
	got := Chunk([]int{1, 2, 3, 4, 5, 6, 7}, 3)
	require.Equal(t, [][]int{{1, 2, 3}, {4, 5, 6}, {7}}, got)
}

func TestChunkExactMultiple(t *testing.T) {
	require.Equal(t, [][]string{{"a", "b"}, {"c", "d"}}, Chunk([]string{"a", "b", "c", "d"}, 2))
}

func TestChunkEmptyInputHasNoChunks(t *testing.T) {
	require.Empty(t, Chunk([]int{}, 5))
	require.Empty(t, Chunk[int](nil, 5))
}

func TestChunkNonPositiveMaxUsesDefaultLimit(t *testing.T) {
	items := make([]int, 1001)
	got := Chunk(items, 0)
	require.Len(t, got, 3)
	require.Len(t, got[0], DefaultMaxItems)
	require.Len(t, got[2], 1)
}

func TestChunkDoesNotAliasBeyondChunk(t *testing.T) {
	items := []int{1, 2, 3}
	got := Chunk(items, 2)
	got[0] = append(got[0], 99) // must not overwrite items[2]
	require.Equal(t, []int{1, 2, 3}, items)
}
