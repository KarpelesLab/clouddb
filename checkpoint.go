package clouddb

import "time"

type Checkpoint struct {
	epoch     int64 // checkpoint epoch, equals time.Now().Unix()/86400 (increments by 1 every 24 hours)
	db        *DB
	logcnt    uint64 // number of log count in epoch
	totlogcnt uint64 // total log count in db lifetime
	logsum    []byte // xor of hash of logs (helps detect case when logcnt is equal but a missing log is replaced by another) we use xor to ensure that a+b+c == c+b+a
}

func currentEpoch() int64 {
	return time.Now().Unix() / 86400
}

func timeUntilNextEpoch() time.Duration {
	// compute how long until next epoch
	now := time.Now()
	curE := now.Unix() / 86400
	nextE := curE + 1
	nextEtime := time.Unix(nextE*86400, 0)
	return nextEtime.Sub(now)
}
