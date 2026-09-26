package locache

// ByteView is an immutable view of cached bytes. ByteSlice returns a defensive
// copy so callers cannot mutate the value stored in cache.
type ByteView struct{ b []byte }

func (b ByteView) Len() int          { return len(b.b) }
func (b ByteView) String() string    { return string(b.b) }
func (b ByteView) ByteSlice() []byte { return cloneBytes(b.b) }

// ByteSLice is kept for source compatibility with older versions.
// Deprecated: use ByteSlice.
func (b ByteView) ByteSLice() []byte { return b.ByteSlice() }

func cloneBytes(b []byte) []byte {
	c := make([]byte, len(b))
	copy(c, b)
	return c
}
