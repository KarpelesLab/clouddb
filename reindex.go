package clouddb

import (
	"encoding/json"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// Reindex will re-generate all the indices for the records in the database
func (d *DB) Reindex() error {
	tx, err := d.store.OpenTransaction()
	if err != nil {
		return err
	}
	defer tx.Discard()

	err = d.reindexWithTransaction(tx)
	if err != nil {
		return err
	}

	return tx.Commit()
}

func (d *DB) reindexWithTransaction(tx *leveldb.Transaction) error {
	err := d.clearAllKeysTx(tx)
	if err != nil {
		return err
	}

	// for all data...
	iter := tx.NewIterator(util.BytesPrefix([]byte("dat")), nil)
	defer iter.Release()

	typeCache := make(map[string]*Type)

	for iter.Next() {
		id := iter.Key()[3:]
		var val map[string]any
		err = json.Unmarshal(iter.Value(), &val)
		if err != nil {
			return err
		}

		typ, ok := val["@type"].(string)
		if !ok {
			typ = "invalid"
		}
		tobj, err := d.getTypeTx(tx, typeCache, typ)
		if err != nil {
			return err
		}

		// compute indices
		keys := tobj.computeIndices(id, val)
		for _, k := range keys {
			tx.Put(append([]byte("idx"), k...), id, nil)
		}
		tx.Put(append([]byte("kdt"), id...), buildKeysData(keys), nil)
	}

	return nil
}

func (d *DB) clearAllKeysTx(tx *leveldb.Transaction) error {
	// clear all records with prefixes "kdt" and "idx"
	for _, pfx := range []string{"kdt", "idx"} {
		err := d.clearDataByPrefixTx(tx, pfx)
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *DB) clearDataByPrefixTx(tx *leveldb.Transaction, pfx string) error {
	iter := tx.NewIterator(util.BytesPrefix([]byte(pfx)), nil)
	defer iter.Release()

	for iter.Next() {
		err := tx.Delete(iter.Key(), nil)
		if err != nil {
			return err
		}
	}
	return nil
}
