package clouddb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
)

type checkpoint struct {
	epoch     int64 // checkpoint epoch, equals time.Now().Unix()/86400 (increments by 1 every 24 hours)
	db        *DB
	logcnt    uint64 // number of log count in epoch
	totlogcnt uint64 // total log count in db lifetime (zero for now)
	logsum    []byte // xor of hash of logs (helps detect case when logcnt is equal but a missing log is replaced by another) we use xor to ensure that a+b+c == c+b+a
}

func (d *DB) nextCheckpointFor(cache map[int64]*checkpoint, t recordVersion) (*checkpoint, error) {
	epoch := t.epoch()
	if v, ok := cache[epoch]; ok {
		return v, nil
	}

	// load from db (if any)
	dat, err := d.store.Get(checkpointKey(epoch+1), nil)
	if err != nil {
		if errors.Is(err, leveldb.ErrNotFound) {
			// generate
			ckpt := &checkpoint{epoch: epoch + 1, db: d, logsum: make([]byte, 32)}
			cache[ckpt.epoch] = ckpt
			return ckpt, nil
		}
		// ???
		return nil, err
	}

	ckpt := &checkpoint{}
	err = ckpt.UnmarshalBinary(dat)
	if err != nil {
		return nil, err
	}
	cache[ckpt.epoch] = ckpt

	return ckpt, nil
}

func currentEpoch() int64 {
	return time.Now().Unix() / 86400
}

func nextEpoch() int64 {
	return (time.Now().Unix() / 86400) + 1
}

func timeUntilNextEpoch() time.Duration {
	// compute how long until next epoch
	now := time.Now()
	curE := now.Unix() / 86400
	nextE := curE + 1
	nextEtime := time.Unix(nextE*86400, 0)
	return nextEtime.Sub(now)
}

func checkpointKey(epoch int64) []byte {
	b := make([]byte, 11)
	copy(b, "chk")
	binary.BigEndian.PutUint64(b[3:], uint64(epoch))
	return b
}

func (d *DB) loadCheckpoint(epoch int64) (*checkpoint, error) {
	k := checkpointKey(epoch)
	val, err := d.store.Get(k, nil)
	if err != nil {
		if errors.Is(err, leveldb.ErrNotFound) {
			return nil, fs.ErrNotExist
		}
		return nil, err
	}
	chk := &checkpoint{}
	err = chk.UnmarshalBinary(val)
	return chk, err
}

func (c *checkpoint) Time() time.Time {
	return time.Unix(c.epoch*86400, 0)
}

func (c *checkpoint) UnmarshalBinary(b []byte) error {
	// parse checkpoint
	// format: <version+flags>:uint32 <epoch>:int64 <logcnt>:uint64 <totlogcnt>:uint64 <logsum>:32bytes 4+8*3+32=60 bytes
	if len(b) != 60 {
		return errors.New("invalid checkpoint data length")
	}
	vers := binary.BigEndian.Uint32(b[:4])
	if vers != 0 {
		return fmt.Errorf("unsupported checkpoint data version %d", vers)
	}

	c.epoch = int64(binary.BigEndian.Uint64(b[4:12]))
	c.logcnt = binary.BigEndian.Uint64(b[12:20])
	c.totlogcnt = binary.BigEndian.Uint64(b[20:28])
	c.logsum = dup(b[28:])
	return nil
}

func (c *checkpoint) Bytes() []byte {
	buf := make([]byte, 60)
	binary.BigEndian.PutUint32(buf[:4], 0) // version+flags
	binary.BigEndian.PutUint64(buf[4:12], uint64(c.epoch))
	binary.BigEndian.PutUint64(buf[12:20], c.logcnt)
	binary.BigEndian.PutUint64(buf[20:28], c.totlogcnt)
	copy(buf[28:], c.logsum)
	return buf
}

func (c *checkpoint) key() []byte {
	return checkpointKey(c.epoch)
}

func (c *checkpoint) add(l *dblog) {
	// add log
	c.logcnt += 1
	h := l.Hash()
	for i := range c.logsum {
		c.logsum[i] ^= h[i]
	}
}

func (c *checkpoint) String() string {
	return fmt.Sprintf("Checkpoint epoch=%s cnt=%d xhash=%x", c.Time(), c.logcnt, c.logsum)
}
