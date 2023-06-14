package clouddb

import (
	"encoding/json"
	"fmt"
)

// interpretObj will accept an object and return it as a raw message and a map
func interpretObj(v any) (json.RawMessage, map[string]any, error) {
	switch o := v.(type) {
	case json.RawMessage:
		var r map[string]any
		err := json.Unmarshal(o, &r)
		return o, r, err
	case map[string]any:
		buf, err := json.Marshal(o)
		return buf, o, err
	default:
		return nil, nil, fmt.Errorf("unsupported type %T", v)
	}
}

func dup(in []byte) []byte {
	out := make([]byte, len(in))
	copy(out, in)
	return out
}
