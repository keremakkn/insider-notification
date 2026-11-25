package domain

type Status string

const (
	StatusPending    Status = "pending"
	StatusProcessing Status = "processing"
	StatusSent       Status = "sent" // accepted by the provider, not necessarily received by the user
	StatusFailed     Status = "failed"
	StatusCancelled  Status = "cancelled"
)

func ParseStatus(s string) (Status, error) {
	switch st := Status(s); st {
	case StatusPending, StatusProcessing, StatusSent, StatusFailed, StatusCancelled:
		return st, nil
	}
	return "", errInvalidStatus
}

// transitions is the state machine from docs/DESIGN.md §3. The repository
// enforces each transition with UPDATE … WHERE status = <from>; this table
// documents the allowed edges and is what the tests check against.
var transitions = map[Status][]Status{
	StatusPending: {
		StatusProcessing, // a worker claims it
		StatusCancelled,  // the client cancels it
	},
	StatusProcessing: {
		StatusSent,    // provider returned 2xx
		StatusPending, // retryable error, or the lease expired
		StatusFailed,  // permanent error or attempts exhausted
	},
}

func (s Status) CanTransitionTo(next Status) bool {
	for _, allowed := range transitions[s] {
		if allowed == next {
			return true
		}
	}
	return false
}

// IsFinal reports whether no further transition is possible.
func (s Status) IsFinal() bool {
	return s == StatusSent || s == StatusFailed || s == StatusCancelled
}
