package coupon

import (
	"context"
	"fmt"
)

// Validator answers whether a raw (client-supplied) coupon code is valid.
//
// Implementations must apply Normalize themselves so every backend enforces
// identical shape rules. Returning an error means the backend could not
// answer (e.g. Redis unreachable) — callers must treat that as "unknown",
// never as valid or invalid.
type Validator interface {
	Validate(ctx context.Context, raw string) (bool, error)
	// Info describes the live backend for logs and the /readyz payload.
	Info() map[string]any
	// Healthy reports whether the backend can currently answer.
	Healthy(ctx context.Context) error
}

// IndexValidator validates against an in-memory Index. Lookups are exact,
// lock-free (the index is immutable), and allocation-free.
type IndexValidator struct {
	idx  *Index
	path string
}

// NewIndexValidator loads and verifies the index at path.
func NewIndexValidator(path string) (*IndexValidator, error) {
	idx, err := LoadIndex(path)
	if err != nil {
		return nil, err
	}
	return &IndexValidator{idx: idx, path: path}, nil
}

func (v *IndexValidator) Validate(_ context.Context, raw string) (bool, error) {
	code, ok := Normalize(raw)
	if !ok {
		return false, nil
	}
	return v.idx.Contains(code), nil
}

func (v *IndexValidator) Info() map[string]any {
	return map[string]any{
		"mode":       "index",
		"path":       v.path,
		"code_count": v.idx.Count(),
		"built_at":   v.idx.Meta.BuiltAt,
		"sources":    v.idx.Meta.Sources,
	}
}

func (v *IndexValidator) Healthy(context.Context) error { return nil }

// Index exposes the underlying index (used by the Redis seeder).
func (v *IndexValidator) Index() *Index { return v.idx }

// StaticValidator is a fixture for automated tests ONLY. It must never be
// reachable without an explicit VALIDATOR=static override: a hardcoded code
// list is behaviorally indistinguishable from a real index at first glance,
// which would silently mask a missing index in any real environment.
type StaticValidator struct{ codes map[string]struct{} }

func NewStaticValidator(codes ...string) (*StaticValidator, error) {
	v := &StaticValidator{codes: make(map[string]struct{}, len(codes))}
	for _, c := range codes {
		n, ok := Normalize(c)
		if !ok {
			return nil, fmt.Errorf("static validator: malformed code %q", c)
		}
		v.codes[n] = struct{}{}
	}
	return v, nil
}

func (v *StaticValidator) Validate(_ context.Context, raw string) (bool, error) {
	code, ok := Normalize(raw)
	if !ok {
		return false, nil
	}
	_, hit := v.codes[code]
	return hit, nil
}

func (v *StaticValidator) Info() map[string]any {
	return map[string]any{"mode": "static (TEST ONLY)", "code_count": len(v.codes)}
}

func (v *StaticValidator) Healthy(context.Context) error { return nil }
