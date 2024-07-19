package clouddb

import (
	"errors"
	"fmt"
	"log/slog"

	"github.com/syndtr/goleveldb/leveldb"
)

const runnerLogBuffer = 32

// runner is a background routine processing incoming log entries in sequential order. This helps ensure that
// some checks, such as if a log is already applied, won't run in parallel. This method can receive logs from
// multiple threads and will run these at the same time.
func (d *DB) runner() {
	logs := make([]*dblog, 0, runnerLogBuffer)
	goodLogs := make([]*dblog, 0, runnerLogBuffer)
	var bcast [][]byte

	for {
		// wait for log to come
		logs = append(logs, <-d.runq)

		// attempt to feed more pending logs to batch together
	feedloop:
		for len(logs) < runnerLogBuffer {
			select {
			case l := <-d.runq:
				logs = append(logs, l)
			default:
				break feedloop
			}
		}

		// store
		rc := d.newRunCtx()

		for _, l := range logs {
			if ret, err := rc.Has(l.key()); err == nil && ret {
				// we already know this log
				if l.res != nil {
					// should not happen in theory
					l.res <- errors.New("log entry already known - this error should not appear, please contact support")
				}
				continue
			}
			b := &leveldb.Batch{}
			err := l.apply(rc, b)
			if err != nil {
				if l.res != nil {
					// an error happened for a log entry we created locally, this is typically linked to a duplicate key or something similar, anyway we can just report his as an error
					l.res <- err
					continue
				}
				// something went wrong
				slog.Error(fmt.Sprintf("[clouddb] runner failed to apply log: %s", err), "event", "clouddb:runner:apply_fail")
				// don't panic since we will try to fetch this again as part of sync process
				continue
			}

			b.Replay(rc)
			goodLogs = append(goodLogs, l)

			if l.res != nil {
				bcast = append(bcast, l.Bytes())
			}
		}

		if len(bcast) > 0 {
			go d.broadcastLogs(bcast)
		}

		// update checkpoint
		cache := make(map[int64]*checkpoint)
		for _, l := range goodLogs {
			ckpt, err := d.nextCheckpointFor(cache, l.Version)
			if err != nil {
				slog.Error(fmt.Sprintf("[clouddb] failed to fetch checkpoint: %s", err), "event", "clouddb:runner:next_cp_fail")
				// drop the whole update because wtf
				continue
			}
			ckpt.add(l)
		}
		for _, ckpt := range cache {
			// store new value for checkpoint
			rc.Put(ckpt.key(), ckpt.Bytes())
		}

		// write locally
		err := d.store.Write(rc.Batch(), nil)
		for _, l := range goodLogs {
			if l.res != nil {
				// report write result
				l.res <- err
			}
		}
		if err != nil {
			slog.Error(fmt.Sprintf("[clouddb] write to local db failed: %s", err), "event", "clouddb:runner:local_write_fail")
		}

		// broadcast log ids here

		// truncate but not unallocate
		logs = logs[:0]
		goodLogs = goodLogs[:0]
		bcast = nil
	}
}
