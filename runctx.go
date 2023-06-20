package clouddb

import (
	"errors"

	"github.com/syndtr/goleveldb/leveldb"
)

type runContext struct {
	db        *DB
	newValues map[string][]byte
	delValues map[string]bool
}

func (d *DB) newRunCtx() *runContext {
	rc := &runContext{
		db:        d,
		newValues: make(map[string][]byte),
		delValues: make(map[string]bool),
	}
	return rc
}

func (rc *runContext) Has(k []byte) (bool, error) {
	if _, f := rc.newValues[string(k)]; f {
		return true, nil
	}
	if _, f := rc.delValues[string(k)]; f {
		return false, nil
	}
	return rc.db.store.Has(k, nil)
}

func (rc *runContext) Get(k []byte) ([]byte, error) {
	if v, f := rc.newValues[string(k)]; f {
		return v, nil
	}
	if _, f := rc.delValues[string(k)]; f {
		return nil, leveldb.ErrNotFound
	}
	return rc.db.store.Get(k, nil)
}

func (rc *runContext) Put(k, v []byte) {
	delete(rc.delValues, string(k))
	rc.newValues[string(k)] = v
}

func (rc *runContext) Delete(k []byte) {
	delete(rc.newValues, string(k))
	rc.delValues[string(k)] = true
}

func (rc *runContext) getKeys(id []byte) ([][]byte, error) {
	// get keys currently stored in db for this id
	kdt, err := rc.Get(append([]byte("kdt"), id...))
	if err != nil {
		if errors.Is(err, leveldb.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return parseKeysData(kdt)
}

func (rc *runContext) Batch() *leveldb.Batch {
	b := &leveldb.Batch{}

	for k := range rc.delValues {
		b.Delete([]byte(k))
	}
	for k, v := range rc.newValues {
		b.Put([]byte(k), v)
	}
	return b
}
