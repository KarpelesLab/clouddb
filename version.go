package clouddb

import (
	"encoding/binary"
	"time"
)

type recordVersion struct {
	unix  int64
	nano  uint32 // range [0, 999,999,999]
	srvid uint32 // server numeric id, so we can differenciate records generated at the exact same time (optional)
}

func newRecordVersion() recordVersion {
	n := time.Now()
	r := recordVersion{
		unix: n.Unix(),
		nano: uint32(n.Nanosecond()),
	}
	return r
}

func parseRecordVersion(buf []byte) recordVersion {
	// we assume buf will always be 16 bytes

	return recordVersion{
		unix:  int64(binary.BigEndian.Uint64(buf[:8])),
		nano:  binary.BigEndian.Uint32(buf[8:12]),
		srvid: binary.BigEndian.Uint32(buf[12:16]),
	}
}

// Time returns a time.Time object matching the passed recordVersion
func (r recordVersion) Time() time.Time {
	return time.Unix(r.unix, int64(r.nano))
}

func (r recordVersion) Bytes() []byte {
	res := make([]byte, 16)
	binary.BigEndian.PutUint64(res[:8], uint64(r.unix))
	binary.BigEndian.PutUint32(res[8:12], r.nano)
	binary.BigEndian.PutUint32(res[12:], r.srvid)

	return res
}

func (r recordVersion) epoch() int64 {
	return r.unix / 86400
}

// Put will put the content of this version in the provided buffer which must be 16 bytes
func (r recordVersion) Put(b []byte) {
	binary.BigEndian.PutUint64(b[:8], uint64(r.unix))
	binary.BigEndian.PutUint32(b[8:12], r.nano)
	binary.BigEndian.PutUint32(b[12:], r.srvid)
}

func (r recordVersion) Less(r2 recordVersion) bool {
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
