package consistenthash

import (
	"errors"
	"fmt"
	"sort"
	"sync"
)

type point struct {
	hash    uint32
	node    string
	replica int
}

// Map is a thread-safe consistent hash ring with a fixed number of virtual
// nodes per member. Virtual points are sorted by (hash,node,replica), making
// ring construction deterministic even in the extremely rare case of a hash
// collision and regardless of member insertion order.
type Map struct {
	mu     sync.RWMutex
	config Config
	points []point
	nodes  map[string]struct{}
}

type Option func(*Map)

func WithConfig(config Config) Option {
	return func(m *Map) {
		if config.Replicas > 0 {
			m.config.Replicas = config.Replicas
		}
		if config.HashFunc != nil {
			m.config.HashFunc = config.HashFunc
		}
	}
}

func WithReplicas(replicas int) Option {
	return func(m *Map) {
		if replicas > 0 {
			m.config.Replicas = replicas
		}
	}
}

func New(opts ...Option) *Map {
	m := &Map{config: DefaultConfig, nodes: make(map[string]struct{})}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

// Add adds members idempotently.
func (m *Map) Add(nodes ...string) error {
	if len(nodes) == 0 {
		return errors.New("no nodes provided")
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	changed := false
	for _, node := range nodes {
		if node == "" {
			continue
		}
		if _, exists := m.nodes[node]; exists {
			continue
		}
		m.nodes[node] = struct{}{}
		for i := 0; i < m.config.Replicas; i++ {
			m.points = append(m.points, point{
				hash:    m.config.HashFunc([]byte(fmt.Sprintf("%s#%d", node, i))),
				node:    node,
				replica: i,
			})
		}
		changed = true
	}
	if changed {
		sortPoints(m.points)
	}
	return nil
}

func (m *Map) Remove(node string) error {
	if node == "" {
		return errors.New("invalid node")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.nodes[node]; !exists {
		return fmt.Errorf("node %s not found", node)
	}
	filtered := m.points[:0]
	for _, p := range m.points {
		if p.node != node {
			filtered = append(filtered, p)
		}
	}
	m.points = filtered
	delete(m.nodes, node)
	return nil
}

func (m *Map) Get(key string) string {
	if key == "" {
		return ""
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if len(m.points) == 0 {
		return ""
	}
	h := m.config.HashFunc([]byte(key))
	idx := sort.Search(len(m.points), func(i int) bool { return m.points[i].hash >= h })
	if idx == len(m.points) {
		idx = 0
	}
	return m.points[idx].node
}

func (m *Map) Members() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	members := make([]string, 0, len(m.nodes))
	for node := range m.nodes {
		members = append(members, node)
	}
	sort.Strings(members)
	return members
}

func (m *Map) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.nodes)
}

func sortPoints(points []point) {
	sort.Slice(points, func(i, j int) bool {
		if points[i].hash != points[j].hash {
			return points[i].hash < points[j].hash
		}
		if points[i].node != points[j].node {
			return points[i].node < points[j].node
		}
		return points[i].replica < points[j].replica
	})
}
