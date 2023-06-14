package clouddb

import (
	"fmt"
	"io"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// DebugDump will dump the whole database to log.DefaultLogger
func (d *DB) DebugDump(w io.Writer) {
	fmt.Fprintf(w, "Dumping database %s\n", d.name)

	snap, err := d.store.GetSnapshot()
	if err != nil {
		fmt.Fprintf(w, "dump failed: %s\n", err)
		return
	}
	defer snap.Release()

	// dump data
	d.dumpData(w, snap)
	d.dumpLogs(w, snap)
	d.dumpCkpt(w, snap)
}

func (d *DB) dumpData(w io.Writer, snap *leveldb.Snapshot) {
	iter := snap.NewIterator(util.BytesPrefix([]byte("dat")), nil)
	defer iter.Release()

	for iter.Next() {
		fmt.Fprintf(w, " * %q = %s\n", iter.Key()[3:], iter.Value())
	}
}

func (d *DB) dumpLogs(w io.Writer, snap *leveldb.Snapshot) {
	iter := snap.NewIterator(util.BytesPrefix([]byte("log")), nil)
	defer iter.Release()

	for iter.Next() {
		v := &dblog{}
		err := v.UnmarshalBinary(iter.Value())
		if err != nil {
			fmt.Fprintf(w, " * Error: %s\n", err)
			continue
		}
		fmt.Fprintf(w, " * %s\n", v)
	}
}

func (d *DB) dumpCkpt(w io.Writer, snap *leveldb.Snapshot) {
	iter := snap.NewIterator(util.BytesPrefix([]byte("chk")), nil)
	defer iter.Release()

	for iter.Next() {
		v := &checkpoint{}
		err := v.UnmarshalBinary(iter.Value())
		if err != nil {
			fmt.Fprintf(w, " * Error: %s\n", err)
			continue
		}
		fmt.Fprintf(w, " * %s\n", v)
	}
}
