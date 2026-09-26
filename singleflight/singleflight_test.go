package singleflight

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSameKeyRunsOnce(t *testing.T) {
	var g Group
	var calls atomic.Int64
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			v, err := g.Do("k", func() (interface{}, error) {
				calls.Add(1)
				time.Sleep(20 * time.Millisecond)
				return "v", nil
			})
			if err != nil || v.(string) != "v" {
				t.Errorf("v=%v err=%v", v, err)
			}
		}()
	}
	close(start)
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatalf("calls=%d, want 1", calls.Load())
	}
}

func TestDifferentKeysDoNotBlockEachOther(t *testing.T) {
	var g Group
	started := make(chan string, 2)
	release := make(chan struct{})
	var wg sync.WaitGroup
	for _, key := range []string{"a", "b"} {
		key := key
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = g.Do(key, func() (interface{}, error) {
				started <- key
				<-release
				return key, nil
			})
		}()
	}
	for i := 0; i < 2; i++ {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("different keys serialized unexpectedly")
		}
	}
	close(release)
	wg.Wait()
}
