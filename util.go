package clouddb

import (
	"encoding/binary"
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

func strln16(s string) []byte {
	if len(s) > 65535 {
		s = s[:65535]
	}
	buf := make([]byte, len(s)+2)
	binary.BigEndian.PutUint16(buf[:2], uint16(len(s)))
	copy(buf[2:], s)
	return buf
}

func uint64be(v uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, v)
	return buf
}
