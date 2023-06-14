package clouddb

import (
	"encoding/json"
	"errors"
)

type Record json.RawMessage

// Value returns a record as a parsed object, or an error if the record is not valid. Since this operation
// can be expensive the result should be reused if possible.
func (r Record) Value() (any, error) {
	if len(r) == 0 || r[0] != '{' {
		// length needs to be >0 and it must be a json object (starting with '{')
		return nil, errors.New("invalid record")
	}
	var v any
	err := json.Unmarshal(r, &v)
	return v, err
}
