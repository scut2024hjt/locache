package store

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

type testValue string

func (v testValue) Len() int { return len(v) }

func TestShardedLRUBasicAndTTL(t *testing.T) {
	s := newShardedLRU(Options{MaxBytes: 1 << 20, ShardCount: 8, CleanupInterval: 5 * time.Millisecond})
	defer s.Close()

	if err := s.SetWithExpiration("k", testValue("v"), 20*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if got, ok := s.Get("k"); !ok || got.(testValue) != "v" {
		t.Fatalf("initial get = %v, %v", got, ok)
	}
	time.Sleep(30 * time.Millisecond)
	if _, ok := s.Get("k"); ok {
		t.Fatal("expired entry still visible")
	}
}

func TestShardedLRUConcurrent(t *testing.T) {
	s := newShardedLRU(Options{MaxBytes: 8 << 20, ShardCount: 16, CleanupInterval: time.Minute})
	defer s.Close()

	var wg sync.WaitGroup
	for g := 0; g < 64; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				key := fmt.Sprintf("k-%d", (id+i)%256)
				_ = s.Set(key, testValue("value"))
				_, _ = s.Get(key)
			}
		}(g)
	}
	wg.Wait()
	if s.Len() == 0 {
		t.Fatal("cache unexpectedly empty")
	}
}

func TestTinyTotalBudgetReducesShardCount(t *testing.T) {
	s := newShardedLRU(Options{MaxBytes: 16, ShardCount: 16, CleanupInterval: time.Minute})
	defer s.Close()
	if len(s.shards) != 1 {
		t.Fatalf("tiny budget created %d shards, want 1", len(s.shards))
	}
	if err := s.Set("k", testValue("hello")); err != nil {
		t.Fatal(err)
	}
	v, ok := s.Get("k")
	if !ok || string(v.(testValue)) != "hello" {
		t.Fatalf("tiny-budget get=%v ok=%v", v, ok)
	}
}
