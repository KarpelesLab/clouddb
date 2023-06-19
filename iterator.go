package clouddb

import (
	"encoding/json"

	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type Iterator interface {
	// Key returns the key of the currently seeked item
	Key() []byte

	// Value return the raw value of the currently seeked item. Use Apply() to obtain the value as
	// an object.
	Value() []byte

	// Release releases resources and must be called after using the iterator
	Release()

	// Next seeks to the next record
	Next() bool

	// Prev seeks to the previous record
	Prev() bool

	// Seek seeks to the index specified and returns if it exists
	Seek([]byte) bool

	// Apply will decode the contents of the value currently pointed by Iterator
	// into the object passed by reference
	Apply(v any) error
}

type iteratorContainer struct {
	iterator.Iterator
}

// Seek is Iterator's seek function but with an override to ensure the
// "dat" prefix is added
func (i *iteratorContainer) Seek(v []byte) bool {
	return i.Iterator.Seek(append([]byte("dat"), v...))
}

// Key is Iterator's key function but we skip the prefix
func (i *iteratorContainer) Key() []byte {
	return i.Iterator.Key()[3:]
}

func (i *iteratorContainer) Apply(v any) error {
	return json.Unmarshal(i.Value(), v)
}

func (d *DB) NewIterator(start, limit []byte) Iterator {
	start = append([]byte("dat"), start...)
	if limit == nil {
		limit = []byte{'d', 'a', 't' + 1} // dau
	} else {
		limit = append([]byte("dat"), limit...)
	}
	iter := d.store.NewIterator(&util.Range{Start: start, Limit: limit}, nil)
	return &iteratorContainer{iter}
}

func (d *DB) NewIteratorPrefix(pfx []byte) Iterator {
	iter := d.store.NewIterator(util.BytesPrefix(append([]byte("dat"), pfx...)), nil)
	return &iteratorContainer{iter}
}

type indirectIterator struct {
	iterator.Iterator
	prefix []byte
	db     *DB
}

func (i *indirectIterator) Key() []byte {
	// this is where the key actually is for indirect iterators
	return i.Iterator.Value()
}

func (i *indirectIterator) Value() []byte {
	// grab raw value
	res, _ := i.db.GetRaw(i.Iterator.Value())
	return res
}

func (i *indirectIterator) Seek([]byte) bool {
	panic("cannot seek indirect iterators")
}

func (i *indirectIterator) Apply(v any) error {
	return i.db.Get(i.Iterator.Value(), v)
}
