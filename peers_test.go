package locache

import (
	"fmt"
	"testing"
	"time"

	"github.com/scut2024hjt/locache/consistenthash"
)

func newPickerForUnitTest(self string) *ClientPicker {
	p := &ClientPicker{
		selfAddr:     self,
		ring:         consistenthash.New(),
		members:      map[string]struct{}{self: {}},
		clients:      make(map[string]*Client),
		rpcTO:        time.Second,
		hashReplicas: consistenthash.DefaultConfig.Replicas,
	}
	_ = p.ring.Add(self)
	p.epoch.Store(1)
	return p
}

func closeTestPickerClients(p *ClientPicker) {
	for _, c := range p.clients {
		_ = c.Close()
	}
}

func TestPickersWithSameMembershipChooseSameOwner(t *testing.T) {
	members := map[string]struct{}{
		"127.0.0.1:8001": {},
		"127.0.0.1:8002": {},
		"127.0.0.1:8003": {},
	}
	pickers := []*ClientPicker{
		newPickerForUnitTest("127.0.0.1:8001"),
		newPickerForUnitTest("127.0.0.1:8002"),
		newPickerForUnitTest("127.0.0.1:8003"),
	}
	for _, p := range pickers {
		p.replaceMembership(cloneMemberSet(members))
		defer closeTestPickerClients(p)
	}

	for i := 0; i < 2000; i++ {
		key := fmt.Sprintf("key-%d", i)
		owner0, _, _, _, ok := pickers[0].PickOwner(key)
		if !ok {
			t.Fatalf("no owner for %s", key)
		}
		for _, p := range pickers[1:] {
			owner, _, _, _, ok := p.PickOwner(key)
			if !ok || owner != owner0 {
				t.Fatalf("inconsistent owner for %s: %s vs %s", key, owner0, owner)
			}
		}
	}
}

func TestMembershipChangeAdvancesEpoch(t *testing.T) {
	p := newPickerForUnitTest("127.0.0.1:8001")
	defer closeTestPickerClients(p)
	before := p.Epoch()
	p.replaceMembership(map[string]struct{}{
		"127.0.0.1:8001": {},
		"127.0.0.1:8002": {},
	})
	if p.Epoch() <= before {
		t.Fatalf("epoch did not advance: before=%d after=%d", before, p.Epoch())
	}
	stable := p.Epoch()
	p.replaceMembership(map[string]struct{}{
		"127.0.0.1:8001": {},
		"127.0.0.1:8002": {},
	})
	if p.Epoch() != stable {
		t.Fatalf("identical membership changed epoch: %d -> %d", stable, p.Epoch())
	}
}

func TestParseAddrFromKey(t *testing.T) {
	if got := parseAddrFromKey("/services/locache/10.0.0.1:8001", "locache"); got != "10.0.0.1:8001" {
		t.Fatalf("got %q", got)
	}
	if got := parseAddrFromKey("/services/other/10.0.0.1:8001", "locache"); got != "" {
		t.Fatalf("unexpected match %q", got)
	}
}

func cloneMemberSet(in map[string]struct{}) map[string]struct{} {
	out := make(map[string]struct{}, len(in))
	for k := range in {
		out[k] = struct{}{}
	}
	return out
}
