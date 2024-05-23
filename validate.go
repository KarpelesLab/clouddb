package clouddb

import (
	"fmt"
	"log/slog"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// Validate will check all records in the db and ensure everything looks ok, fixing some issues if found
func (d *DB) Validate() error {
	tx, err := d.store.OpenTransaction()
	if err != nil {
		return err
	}
	defer tx.Discard()

	if err := d.validateLogs(tx); err != nil {
		return err
	}
	// TODO check data
	// TODO re-build indices
	return tx.Commit()
}

// validateLogs check logs and erases anything that looks invalid so it can be synched again
func (d *DB) validateLogs(tx *leveldb.Transaction) error {
	iter := tx.NewIterator(util.BytesPrefix([]byte("log")), nil)
	defer iter.Release()

	for iter.Next() {
		v := &dblog{}
		err := v.UnmarshalBinary(iter.Value())
		if err != nil {
			slog.Debug(fmt.Sprintf("[clouddb] failed to parse cloud record, rejecting"), "event", "clouddb:validate:bad_log")
			tx.Delete(iter.Key(), nil)
			continue
		}

		if !v.Valid() {
			slog.Debug(fmt.Sprintf("[clouddb] found invalid log %s", v), "event", "clouddb:validate:invalid_log")
			tx.Delete(iter.Key(), nil)
			continue
		}
	}
	return nil
}
