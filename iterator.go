package clouddb

import (
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type Iterator interface {
	Key() []byte
	Value() []byte
	Release()
	Next() bool
	Prev() bool
	Seek([]byte) bool
}

type iteratorContainer struct {
	iterator.Iterator
}

// Seek is Iterator's seek function but with an override to ensure the
// "dat" prefix is added
func (i *iteratorContainer) Seek(v []byte) bool {
	return i.Iterator.Seek(append([]byte("dat"), v...))
}

func (d *DB) NewIterator(start, limit []byte) Iterator {
	iter := d.store.NewIterator(&util.Range{Start: start, Limit: limit}, nil)
	return &iteratorContainer{iter}
}

func (d *DB) NewIteratorPrefix(pfx []byte) Iterator {
	iter := d.store.NewIterator(util.BytesPrefix(pfx), nil)
	return &iteratorContainer{iter}
}
