package authz

// Level is a data-scope level (P6.3, E6): own < dept < subtree < all; LevelNone means no level.
type Level int8

// The levels in their order (E6).
const (
	LevelNone Level = iota
	LevelOwn
	LevelDept
	LevelSubtree
	LevelAll
)

var levelNames = [...]string{"none", "own", "dept", "subtree", "all"}

// String is the level's name as the bundle and the vectors spell it ("none" for LevelNone).
func (l Level) String() string {
	if l < LevelNone || l > LevelAll {
		return "none"
	}
	return levelNames[l]
}

// ParseLevel reads a bundle level name (own, dept, subtree, all). Anything else is not a level.
func ParseLevel(s string) (Level, bool) {
	for i, n := range levelNames[1:] {
		if n == s {
			return Level(i + 1), true
		}
	}
	return LevelNone, false
}
