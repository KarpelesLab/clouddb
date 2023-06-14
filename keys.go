package clouddb

import (
	"bytes"
	"encoding/binary"
	"errors"
)

func parseKeysData(kdt []byte) ([][]byte, error) {
	var res [][]byte
	for len(kdt) > 0 {
		if len(kdt) < 4 {
			return nil, errors.New("malformed kdt data")
		}
		ln := binary.BigEndian.Uint32(kdt[:4])
		if len(kdt) < int(ln+4) {
			return nil, errors.New("malformed kdt data (short key)")
		}
		res = append(res, kdt[4:4+ln])
		kdt = kdt[4+ln:]
	}
	return res, nil
}

func buildKeysData(keys [][]byte) []byte {
	// build keys data based on keys
	if len(keys) == 0 {
		return nil
	}

	buf := &bytes.Buffer{}
	for _, k := range keys {
		binary.Write(buf, binary.BigEndian, uint32(len(k)))
		buf.Write(k)
	}
	return buf.Bytes()
}

func keysToMap(keys [][]byte) map[string]bool {
	res := make(map[string]bool)

	for _, k := range keys {
		// converting to string in a map index is optimized by go and binary safe
		res[string(k)] = true
	}
	return res
}
