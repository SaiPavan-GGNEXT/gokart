package coupon

import "strings"

// Code length bounds (in bytes) from the challenge rules.
const (
	MinLen = 8
	MaxLen = 10
)

// Normalize trims surrounding whitespace and reports whether the result is a
// well-formed coupon code: 8–10 bytes, every byte in [A-Z0-9].
//
// This single definition is used by the offline index builder AND the API
// request path, so the two can never disagree about what a code is. Length is
// defined in bytes (the alphabet is ASCII, so bytes == characters); anything
// outside the alphabet — lowercase, unicode, control bytes — is rejected here,
// which also guarantees a code can never contain the NUL byte the index file
// uses for record padding.
func Normalize(s string) (string, bool) {
	s = strings.TrimSpace(s)
	if len(s) < MinLen || len(s) > MaxLen {
		return "", false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < 'A' || c > 'Z') && (c < '0' || c > '9') {
			return "", false
		}
	}
	return s, true
}

// fnv1a64 is FNV-1a over the code bytes, inlined (the stdlib hash/fnv forces
// an allocation per use). Used only to partition and pre-filter during index
// builds; final validity is always decided on exact strings, so hash quality
// affects speed, never correctness.
func fnv1a64(s string) uint64 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	h := uint64(offset64)
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= prime64
	}
	return h
}
