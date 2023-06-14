package clouddb

import "context"

// RPC interface is standardized
type RPC interface {
	// All will send a given data object to all other RPC instances on the fleet
	// and will collect responses
	All(ctx context.Context, data []byte) ([]any, error)

	// Broadcast will do the same as All but will not wait for responses
	Broadcast(ctx context.Context, data []byte) error

	// Request will send a given object to a specific peer and return the response
	Request(ctx context.Context, id string, data []byte) ([]byte, error)

	// Send will send a given buffer to a specific peer and ignore the response
	Send(ctx context.Context, id string, data []byte) error

	// Self will return the id of the local peer, can be used for other instances
	// to contact here with Send().
	Self() string

	// Connect connects this RPC instance incoming events to a given function
	// that will be called each time an event is received.
	Connect(cb func(context.Context, []byte) ([]byte, error))
}
