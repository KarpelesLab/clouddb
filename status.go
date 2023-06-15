package clouddb

import "fmt"

type Status int

const (
	Initializing Status = iota
	Syncing
	Ready
	Error
)

func (s Status) String() string {
	switch s {
	case Initializing:
		return "Initializing"
	case Syncing:
		return "Syncing"
	case Ready:
		return "Ready"
	case Error:
		return "Error"
	default:
		return fmt.Sprintf("invalid status %d", s)
	}
}
