package clouddb

type Status int

const (
	Initializing Status = iota
	Syncing
	Ready
	Error
)
