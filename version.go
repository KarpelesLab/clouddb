package clouddb

import (
	"encoding/binary"
	"time"
)

type RecordVersion struct {
	unix  int64
	nano  uint32 // range [0, 999,999,999]
	srvid uint32 // server numeric id, so we can differenciate records generated at the exact same time (optional)
}

func newRecordVersion() RecordVersion {
	n := time.Now()
	r := RecordVersion{
		unix: n.Unix(),
		nano: uint32(n.Nanosecond()),
	}
	return r
}

func parseRecordVersion(buf []byte) RecordVersion {
	// we assume buf will always be 16 bytes

	return RecordVersion{
		unix:  int64(binary.BigEndian.Uint64(buf[:8])),
		nano:  binary.BigEndian.Uint32(buf[8:12]),
		srvid: binary.BigEndian.Uint32(buf[12:16]),
	}
}

// Time returns a time.Time object matching the passed RecordVersion
func (r RecordVersion) Time() time.Time {
	return time.Unix(r.unix, int64(r.nano))
}

func (r RecordVersion) Bytes() []byte {
	res := make([]byte, 16)
	binary.BigEndian.PutUint64(res[:8], uint64(r.unix))
	binary.BigEndian.PutUint32(res[8:12], r.nano)
	binary.BigEndian.PutUint32(res[12:], r.srvid)

	return res
}

// Put will put the content of this version in the provided buffer which must be 16 bytes
func (r RecordVersion) Put(b []byte) {
	binary.BigEndian.PutUint64(b[:8], uint64(r.unix))
	binary.BigEndian.PutUint32(b[8:12], r.nano)
	binary.BigEndian.PutUint32(b[12:], r.srvid)
}

func (r RecordVersion) Less(r2 RecordVersion) bool {
	// return true if r < r2
	if r.unix < r2.unix {
		return true
	} else if r.unix > r2.unix {
		return false
	}
	if r.nano < r2.nano {
		return true
	} else if r.nano > r2.nano {
		return false
	}
	if r.srvid < r2.srvid {
		return true
	}
	return false
}
