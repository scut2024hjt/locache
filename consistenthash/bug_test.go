package consistenthash

import (
	"fmt"
	"sync"
	"testing"
)

func TestConcurrentGet(t *testing.T) {
	ring := New()
	if err := ring.Add("A", "B", "C"); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				if got := ring.Get(fmt.Sprintf("key-%d", j%100)); got == "" {
					t.Errorf("goroutine %d: empty owner", id)
					return
				}
			}
		}(i)
	}
	wg.Wait()
}

func TestIdenticalMembershipBuildsIdenticalRing(t *testing.T) {
	a := New()
	b := New()
	_ = a.Add("node-c", "node-a", "node-b")
	_ = b.Add("node-a", "node-b", "node-c")

	for i := 0; i < 5000; i++ {
		key := fmt.Sprintf("key-%d", i)
		if a.Get(key) != b.Get(key) {
			t.Fatalf("different owner for %q: %s vs %s", key, a.Get(key), b.Get(key))
		}
	}
}

func TestAddIsIdempotentAndRemoveWorks(t *testing.T) {
	ring := New(WithReplicas(32))
	if err := ring.Add("A", "B"); err != nil {
		t.Fatal(err)
	}
	if err := ring.Add("A"); err != nil {
		t.Fatal(err)
	}
	if got := ring.Len(); got != 2 {
		t.Fatalf("members=%d, want 2", got)
	}
	if err := ring.Remove("A"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 1000; i++ {
		if got := ring.Get(fmt.Sprintf("k-%d", i)); got == "A" {
			t.Fatalf("removed node selected for k-%d", i)
		}
	}
}

func TestAddingNodeOnlyRemapsSubset(t *testing.T) {
	ring := New(WithReplicas(128))
	_ = ring.Add("A", "B", "C")
	before := make(map[string]string, 10000)
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key-%d", i)
		before[key] = ring.Get(key)
	}
	_ = ring.Add("D")
	changed := 0
	for key, old := range before {
		if ring.Get(key) != old {
			changed++
		}
	}
	if changed == 0 || changed == len(before) {
		t.Fatalf("unexpected remap count: %d/%d", changed, len(before))
	}
}

func TestHashCollisionsRemainDeterministicAcrossInsertionOrder(t *testing.T) {
	constantHash := func([]byte) uint32 { return 7 }
	cfg := Config{Replicas: 4, HashFunc: constantHash}
	a := New(WithConfig(cfg))
	b := New(WithConfig(cfg))
	_ = a.Add("C", "A", "B")
	_ = b.Add("B", "C", "A")
	if gotA, gotB := a.Get("key"), b.Get("key"); gotA != gotB {
		t.Fatalf("collision produced inconsistent owners: %q vs %q", gotA, gotB)
	}
}
