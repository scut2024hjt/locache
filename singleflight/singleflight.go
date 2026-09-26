package singleflight

import "sync"

type call struct {
	wg       sync.WaitGroup
	val      interface{}
	err      error
	panicVal interface{}
}

// Group suppresses duplicate in-flight work for the same key. Completed calls
// are removed immediately; this type does not cache results by itself.
type Group struct {
	mu sync.Mutex
	m  map[string]*call
}

func (g *Group) Do(key string, fn func() (interface{}, error)) (interface{}, error) {
	g.mu.Lock()
	if g.m == nil {
		g.m = make(map[string]*call)
	}
	if c, ok := g.m[key]; ok {
		g.mu.Unlock()
		c.wg.Wait()
		if c.panicVal != nil {
			panic(c.panicVal)
		}
		return c.val, c.err
	}
	c := new(call)
	c.wg.Add(1)
	g.m[key] = c
	g.mu.Unlock()

	func() {
		defer func() {
			if r := recover(); r != nil {
				c.panicVal = r
			}
		}()
		c.val, c.err = fn()
	}()

	// Remove the completed call before waking waiters. A new caller arriving
	// after fn has completed should start a new call rather than accidentally
	// reusing the just-finished result during a tiny cleanup window.
	g.mu.Lock()
	if g.m[key] == c {
		delete(g.m, key)
	}
	g.mu.Unlock()
	c.wg.Done()

	if c.panicVal != nil {
		panic(c.panicVal)
	}
	return c.val, c.err
}
