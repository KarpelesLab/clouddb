package clouddb

import (
	"errors"
	"log"

	"github.com/syndtr/goleveldb/leveldb"
)

const runnerLogBuffer = 32

// runner is a background routine processing incoming log entries in sequential order. This helps ensure that
// some checks, such as if a log is already applied, won't run in parallel. This method can receive logs from
// multiple threads and will run these at the same time.
func (d *DB) runner() {
	logs := make([]*Log, 0, runnerLogBuffer)
	goodLogs := make([]*Log, 0, runnerLogBuffer)

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
			if ret, err := d.store.Has(l.key(), nil); err == nil && ret {
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
				log.Printf("[clouddb] runner failed to apply log: %s", err)
				// don't panic since we will try to fetch this again as part of sync process
				continue
			}

			b.Replay(rc)
			goodLogs = append(goodLogs, l)
		}

		// update checkpoint

		// write locally
		err := d.store.Write(rc.Batch(), nil)
		for _, l := range goodLogs {
			if l.res != nil {
				// report write result
				l.res <- err
			}
		}
		if err != nil {
			log.Printf("[clouddb] write to local db failed: %s", err)
		}

		// broadcast log here?

		// truncate but not unallocate
		logs = logs[:0]
		goodLogs = goodLogs[:0]
	}
}
