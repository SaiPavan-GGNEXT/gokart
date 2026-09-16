package cached

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/SaiPavan-GGNEXT/gokart/internal/domain"
	"github.com/SaiPavan-GGNEXT/gokart/internal/store"
)

// fakeStore is a controllable inner store implementing ProductStore,
// CreateWithID, and Fingerprint.
type fakeStore struct {
	mu       sync.Mutex
	rows     []domain.Product
	fail     bool
	listCall int
	getCall  int
}

func (f *fakeStore) List(context.Context) ([]domain.Product, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.listCall++
	if f.fail {
		return nil, errors.New("db down")
	}
	return append([]domain.Product(nil), f.rows...), nil
}

func (f *fakeStore) Get(_ context.Context, id string) (*domain.Product, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.getCall++
	if f.fail {
		return nil, errors.New("db down")
	}
	for _, p := range f.rows {
		if p.ID == id {
			return &p, nil
		}
	}
	return nil, store.ErrNotFound
}

func (f *fakeStore) GetMany(_ context.Context, ids []string) (map[string]domain.Product, error) {
	out := map[string]domain.Product{}
	for _, id := range ids {
		if p, err := f.Get(context.Background(), id); err == nil {
			out[id] = *p
		}
	}
	return out, nil
}

func (f *fakeStore) Create(_ context.Context, np domain.NewProduct) (*domain.Product, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail {
		return nil, errors.New("db down")
	}
	p := domain.Product{ID: strconv.Itoa(len(f.rows) + 1), Name: np.Name, Price: np.Price, Category: np.Category}
	f.rows = append(f.rows, p)
	return &p, nil
}

func (f *fakeStore) CreateWithID(_ context.Context, id string, np domain.NewProduct) (*domain.Product, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	p := domain.Product{ID: id, Name: np.Name, Price: np.Price, Category: np.Category}
	f.rows = append(f.rows, p)
	return &p, nil
}

func (f *fakeStore) Count(context.Context) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.rows), nil
}

func (f *fakeStore) Healthy(context.Context) error { return nil }

func (f *fakeStore) Fingerprint(context.Context) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail {
		return "", errors.New("db down")
	}
	return strconv.Itoa(len(f.rows)), nil
}

func (f *fakeStore) addRow(p domain.Product) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rows = append(f.rows, p)
}

func (f *fakeStore) setFail(v bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fail = v
}

func (f *fakeStore) listCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.listCall
}

func (f *fakeStore) getCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.getCall
}

func newCached(t *testing.T, inner *fakeStore) *ProductStore {
	t.Helper()
	c, err := New(context.Background(), inner, 0) // no ticker; ticks driven manually
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestListIsBoundedStaleButRefreshPicksUpChanges(t *testing.T) {
	ctx := context.Background()
	inner := &fakeStore{rows: []domain.Product{{ID: "1", Name: "Waffle", Price: 5, Category: "W"}}}
	c := newCached(t, inner)

	// Another instance writes to the store: the LIST stays bounded-stale...
	inner.addRow(domain.Product{ID: "2", Name: "Latte", Price: 4, Category: "D"})
	if list, _ := c.List(ctx); len(list) != 1 {
		t.Fatalf("list must be snapshot-stale before refresh, got %d rows", len(list))
	}
	// ...until one tick reconciles it.
	if err := c.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	if list, _ := c.List(ctx); len(list) != 2 {
		t.Fatalf("after refresh list = %d rows", len(list))
	}
}

func TestGetReadsThroughOnMissAndCachesTheHit(t *testing.T) {
	ctx := context.Background()
	inner := &fakeStore{rows: []domain.Product{{ID: "1", Name: "Waffle", Price: 5, Category: "W"}}}
	c := newCached(t, inner)

	// Row created on "another instance" after our snapshot loaded:
	inner.addRow(domain.Product{ID: "2", Name: "Latte", Price: 4, Category: "D"})

	// Point lookup finds it via read-through (PK-indexed query)...
	p, err := c.Get(ctx, "2")
	if err != nil || p.Name != "Latte" {
		t.Fatalf("read-through failed: %v %v", p, err)
	}
	// ...and the hit is now cached: even with the DB down, repeats serve.
	inner.setFail(true)
	if p, err := c.Get(ctx, "2"); err != nil || p.Name != "Latte" {
		t.Fatalf("read-through result was not cached: %v %v", p, err)
	}
	// No duplicate list entries from the upsert.
	inner.setFail(false)
	if list, _ := c.List(ctx); len(list) != 2 {
		t.Fatalf("upsert duplicated rows: %d", len(list))
	}
}

func TestGetManyReadsThroughOnlyForMissingIDs(t *testing.T) {
	ctx := context.Background()
	inner := &fakeStore{rows: []domain.Product{{ID: "1", Name: "Waffle", Price: 5, Category: "W"}}}
	c := newCached(t, inner)
	inner.addRow(domain.Product{ID: "2", Name: "Latte", Price: 4, Category: "D"})

	got, err := c.GetMany(ctx, []string{"1", "2", "999"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got["2"].Name != "Latte" {
		t.Fatalf("GetMany read-through: %v", got)
	}
	if _, ok := got["999"]; ok {
		t.Fatal("unknown id must stay missing")
	}
}

func TestTrueMissesAreNotNegativelyCached(t *testing.T) {
	ctx := context.Background()
	inner := &fakeStore{rows: []domain.Product{{ID: "1", Name: "Waffle", Price: 5, Category: "W"}}}
	c := newCached(t, inner)

	before := inner.getCalls()
	for i := 0; i < 3; i++ {
		if _, err := c.Get(ctx, "404404"); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("want ErrNotFound, got %v", err)
		}
	}
	// Each probe hits the store once (documented: rate limiter bounds this;
	// negative caching is the next step if it ever matters).
	if got := inner.getCalls() - before; got != 3 {
		t.Fatalf("expected 3 store hits for 3 negative probes, got %d", got)
	}
}

func TestFingerprintSkipsUnchangedReloads(t *testing.T) {
	ctx := context.Background()
	inner := &fakeStore{rows: []domain.Product{{ID: "1", Name: "Waffle", Price: 5, Category: "W"}}}
	c := newCached(t, inner)

	calls := inner.listCalls()
	for i := 0; i < 5; i++ {
		if err := c.Refresh(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if got := inner.listCalls(); got != calls {
		t.Fatalf("unchanged fingerprint must skip List: %d extra calls", got-calls)
	}
}

func TestWriteThroughIsImmediatelyVisible(t *testing.T) {
	ctx := context.Background()
	inner := &fakeStore{}
	c := newCached(t, inner)

	p, err := c.Create(ctx, domain.NewProduct{Name: "Mocha", Price: 5, Category: "D"})
	if err != nil {
		t.Fatal(err)
	}
	// Read-your-writes with NO refresh in between:
	if got, err := c.Get(ctx, p.ID); err != nil || got.Name != "Mocha" {
		t.Fatalf("write-through not visible: %v %v", got, err)
	}
	if n, _ := c.Count(ctx); n != 1 {
		t.Fatalf("count after write-through = %d", n)
	}
}

func TestFailStaticServesOldSnapshotDuringOutage(t *testing.T) {
	ctx := context.Background()
	inner := &fakeStore{rows: []domain.Product{{ID: "1", Name: "Waffle", Price: 5, Category: "W"}}}
	c := newCached(t, inner)

	inner.setFail(true) // database goes down
	if err := c.Refresh(ctx); err == nil {
		t.Fatal("refresh must report the outage")
	}
	// The menu must still serve.
	if list, _ := c.List(ctx); len(list) != 1 || list[0].Name != "Waffle" {
		t.Fatalf("fail-static violated: %v", list)
	}
	if err := c.Healthy(ctx); err != nil {
		t.Fatal("a loaded snapshot can serve reads during a store outage")
	}
}

func TestSeedingForwardsThroughDecorator(t *testing.T) {
	ctx := context.Background()
	inner := &fakeStore{}
	c := newCached(t, inner)

	if _, err := c.CreateWithID(ctx, "10", domain.NewProduct{Name: "Chicken Waffle", Price: 12.99, Category: "Waffle"}); err != nil {
		t.Fatal(err)
	}
	if p, err := c.Get(ctx, "10"); err != nil || p.Name != "Chicken Waffle" {
		t.Fatalf("seeded row not visible: %v %v", p, err)
	}
}

func TestSnapshotInfoShape(t *testing.T) {
	inner := &fakeStore{rows: []domain.Product{{ID: "1", Name: "W", Price: 1, Category: "C"}}}
	c := newCached(t, inner)
	info := c.SnapshotInfo()
	if info["products"] != 1 || info["mode"] != "snapshot-cache" {
		t.Fatalf("bad snapshot info: %v", info)
	}
	if _, ok := info["loaded_at"].(time.Time); !ok {
		t.Fatalf("loaded_at missing: %v", info)
	}
}
