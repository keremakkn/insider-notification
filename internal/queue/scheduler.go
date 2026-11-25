package queue

import (
	"sync"

	"github.com/keremakkn/insider-notification/internal/domain"
)

// scheduler is smooth weighted round-robin, the algorithm nginx uses for
// weighted upstreams. With weights 6/3/1 every 10 turns contain exactly
// 6 high, 3 normal and 1 low, interleaved (H N H L H H N H N H …) rather than
// in runs (HHHHHH NNN L), so low priority never waits a whole cycle.
//
// Each turn: every priority gains its weight; the one with the most credit
// wins and pays back the total.
type scheduler struct {
	mu      sync.Mutex
	weights [3]int
	credit  [3]int
	total   int
}

func newScheduler(weights [3]int) *scheduler {
	s := &scheduler{weights: weights}
	for _, w := range weights {
		s.total += w
	}
	return s
}

func (s *scheduler) next() domain.Priority {
	s.mu.Lock()
	defer s.mu.Unlock()
	best := 0
	for i := range s.credit {
		s.credit[i] += s.weights[i]
		if s.credit[i] > s.credit[best] { // ties go to the higher priority
			best = i
		}
	}
	s.credit[best] -= s.total
	return domain.Priority(best)
}
