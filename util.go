package clouddb

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"reflect"
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
		// this is a bit complex...
		typ := reflect.TypeOf(v)
		for typ.Kind() == reflect.Ptr {
			typ = typ.Elem()
		}
		buf, err := json.Marshal(v)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to marshal object: %w", err)
		}
		// parse buf back to a map[string]any
		var r map[string]any
		err = json.Unmarshal(buf, &r)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to unmarshal object: %w", err)
		}
		r["@type"] = typ.String()
		// re-marshal into json to include @type
		buf, err = json.Marshal(r)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to re-marshal object: %w", err)
		}
		// OK
		return buf, r, nil
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

func byteln8(b []byte) []byte {
	if len(b) > 255 {
		b = b[:255]
	}
	return append([]byte{byte(len(b))}, b...)
}

func uint64be(v uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, v)
	return buf
}

func getstrln16(b *[]byte) string {
	if len(*b) < 2 {
		return ""
	}
	ln := binary.BigEndian.Uint16((*b)[:2])
	if len(*b) < int(ln)+2 {
		return ""
	}
	res := (*b)[2 : int(ln)+2]
	*b = (*b)[int(ln)+2:]
	return string(res)
}

func getuint64be(b *[]byte) uint64 {
	if len(*b) < 8 {
		return 0
	}
	v := binary.BigEndian.Uint64((*b)[:8])
	*b = (*b)[8:]
	return v
}

func getbyteln8(b *[]byte) []byte {
	if len(*b) < 1 {
		return nil
	}
	ln := (*b)[0]
	if len(*b) < int(ln)+1 {
		return nil
	}
	res := (*b)[1 : int(ln)+1]
	*b = (*b)[int(ln)+1:]
	return res
}
