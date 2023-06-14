package clouddb

import (
	"log"

	"github.com/syndtr/goleveldb/leveldb"
)

func (d *DB) runner() {
	logs := make([]*Log, 0, 32)
	goodLogs := make([]*Log, 0, 32)

	for {
		// wait for log to come
		logs = append(logs, <-d.runq)

		// attempt to feed more pending logs to batch together
	feedloop:
		for len(logs) < 32 {
			select {
			case l := <-d.runq:
				logs = append(logs, l)
			default:
				break feedloop
			}
		}

		// store
		b := &leveldb.Batch{}

		for _, l := range logs {
			err := l.apply(d, b)
			if err != nil {
				if l.res != nil {
					l.res <- err
					continue
				}
				// something went wrong
				log.Printf("[clouddb] runner failed to apply log: %s", err)
				// don't panic since we will try to fetch this again as part of sync process
				continue
			}

			goodLogs = append(goodLogs, l)
		}

		// broadcast log here?

		// write locally
		err := d.store.Write(b, nil)
		for _, l := range goodLogs {
			if l.res != nil {
				// report write result
				l.res <- err
			}
		}
		if err != nil {
			log.Printf("[clouddb] write to local db failed: %s", err)
			// don't panic on write failure because we already broadcasted the logs
		}

		// truncate but not unallocate
		logs = logs[:0]
		goodLogs = goodLogs[:0]
	}
}
