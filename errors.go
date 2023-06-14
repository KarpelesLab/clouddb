package clouddb

import "errors"

var (
	ErrKeyConflict = errors.New("record's key conflicts with an existing record")
)
