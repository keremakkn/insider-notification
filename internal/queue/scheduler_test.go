package queue

import (
	"strings"
	"testing"

	"github.com/keremakkn/insider-notification/internal/domain"
)

func TestSchedulerIsExactAndInterleaved(t *testing.T) {
	s := newScheduler([3]int{6, 3, 1})
	var seq strings.Builder
	counts := map[domain.Priority]int{}
	for range 10 {
		p := s.next()
		counts[p]++
		seq.WriteByte("HNL"[p])
	}
	if counts[domain.PriorityHigh] != 6 || counts[domain.PriorityNormal] != 3 || counts[domain.PriorityLow] != 1 {
		t.Fatalf("one cycle of 10 must be exactly 6/3/1, got %v", counts)
	}
	t.Logf("one cycle: %s", seq.String())
	if strings.Contains(seq.String(), "HHHH") {
		t.Errorf("high should be interleaved, got %s", seq.String())
	}
}

func TestSchedulerHonoursOtherWeights(t *testing.T) {
	s := newScheduler([3]int{1, 1, 1})
	counts := map[domain.Priority]int{}
	for range 300 {
		counts[s.next()]++
	}
	for _, p := range domain.Priorities {
		if counts[p] != 100 {
			t.Fatalf("equal weights must split evenly, got %v", counts)
		}
	}
}
