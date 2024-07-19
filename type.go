package clouddb

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"sync"

	"github.com/KarpelesLab/dbidx"
	"github.com/syndtr/goleveldb/leveldb"
	"golang.org/x/text/collate"
)

var (
	globalTypeMap   = make(map[string]*Type)
	globalTypeMapLk sync.RWMutex
)

// Type is a registered index type used in records. A record of a given type will inherit
// the passed keys and be indexed based on it. Some keys will always exist, such as to
// ensure objects can be referenced at all times.
//
// Types will be stored in the database to ensure records can be indexed properly on
// first try.
type Type struct {
	Name    string     `json:"name"`
	Keys    []*TypeKey `json:"keys"`
	Version int        `json:"version"` // version for the definition, can be set to zero
}

type TypeKey dbidx.Index

// encodeValueContext is used to optimize some aspects of value encoding
type encodeValueContext struct {
	colBuf *collate.Buffer
}

func RegisterType(t *Type) {
	// register type as a global type
	globalTypeMapLk.Lock()
	defer globalTypeMapLk.Unlock()

	globalTypeMap[t.Name] = t
}

func (t *Type) Hash() []byte {
	h := sha256.New()
	if t != nil {
		e := json.NewEncoder(h)
		e.Encode(t)
	}
	return h.Sum(nil)
}

func (d *DB) getTypeFromMem(typ string) *Type {
	d.typLk.RLock()
	defer d.typLk.RUnlock()

	if v, ok := d.typMap[typ]; ok {
		return v
	}
	return nil
}

func getTypeFromGlobal(typ string) *Type {
	globalTypeMapLk.RLock()
	defer globalTypeMapLk.RUnlock()

	if v, ok := globalTypeMap[typ]; ok {
		return v
	}
	return nil
}

func (d *DB) setTypeMem(typ string, t *Type) {
	d.typLk.Lock()
	defer d.typLk.Unlock()

	d.typMap[typ] = t
}

func (d *DB) getType(typ string) (*Type, error) {
	t := d.getTypeFromMem(typ)
	if t != nil {
		return t, nil
	}
	t = getTypeFromGlobal(typ)
	if t != nil {
		// store in db & in mem
		buf, err := json.Marshal(t)
		if err != nil {
			return nil, err
		}
		//log.Printf("storing type in db: %s = %s", typ, buf)
		err = d.store.Put(append([]byte("typ"), typ...), buf, nil)
		if err != nil {
			return nil, err
		}
		d.setTypeMem(typ, t)
		return t, nil
	}

	// load from db...
	dat, err := d.store.Get(append([]byte("typ"), typ...), nil)
	if err != nil {
		if errors.Is(err, leveldb.ErrNotFound) {
			return nil, fmt.Errorf("type %s: %w", typ, fs.ErrNotExist)
		}
		return nil, err
	}
	err = json.Unmarshal(dat, &t)
	if err != nil {
		return nil, err
	}
	d.setTypeMem(typ, t)
	return t, nil
}

// getTypeTx is a variant of getType that only uses the transaction
func (d *DB) getTypeTx(tx *leveldb.Transaction, typeCache map[string]*Type, typ string) (*Type, error) {
	if t, ok := typeCache[typ]; ok {
		return t, nil
	}
	t := getTypeFromGlobal(typ)
	if t != nil {
		// store in tx
		buf, err := json.Marshal(t)
		if err != nil {
			return nil, err
		}
		err = tx.Put(append([]byte("typ"), typ...), buf, nil)
		if err != nil {
			return nil, err
		}
		typeCache[typ] = t
		return t, nil
	}
	// load from tx
	dat, err := tx.Get(append([]byte("typ"), typ...), nil)
	if err != nil {
		if errors.Is(err, leveldb.ErrNotFound) {
			return nil, fmt.Errorf("type %s: %w", typ, fs.ErrNotExist)
		}
		return nil, err
	}
	err = json.Unmarshal(dat, &t)
	if err != nil {
		return nil, err
	}
	typeCache[typ] = t
	return t, nil
}

// computeIndices returns the indices for a given object, and will always return the same value for a given object
func (t *Type) computeIndices(id []byte, v any) [][]byte {
	// compute indices for a given object
	var res [][]byte

	if t == nil {
		// write down type is null
		res = append(res, append([]byte("undefined\x00@type\x00"), id...), append([]byte("undefined\x00@version\x00invalid\x00"), id...))
		return res
	}

	switch n := v.(type) {
	case json.RawMessage:
		var v any
		json.Unmarshal(n, &v)
	}

	pfx := []byte(t.Name + "\x00")
	res = append(res, append([]byte(t.Name+"\x00@type\x00"), id...))
	res = append(res, append(append(append([]byte(t.Name+"\x00@version\x00"), t.Hash()...), 0), id...))

	for _, k := range t.Keys {
		res = append(res, (*dbidx.Index)(k).ComputeIndices(pfx, id, v)...)
	}

	return res
}

func (t *Type) findSearchPrefix(search map[string]any) ([]byte, error) {
	// attempt to find a TypeKey that has the exact same fields as search and return the prefix for this search
	if len(search) == 0 {
		// search all records of this type
		if t == nil {
			return []byte("undefined\x00@type\x00"), nil
		}
		return []byte(t.Name + "\x00@type\x00"), nil
	}
	if t == nil {
		return nil, errors.New("cannot perform search on unknown/invalid type")
	}

	pfx := []byte(t.Name + "\x00")

	cnt := len(search)
keysloop:
	for _, k := range t.Keys {
		found := 0
		for _, s := range k.Fields {
			if _, ok := search[s]; ok {
				found += 1
			} else {
				continue keysloop
			}
		}
		if found != cnt {
			continue
		}

		// found the key!
		return (*dbidx.Index)(k).ComputeSearchPrefix(pfx, search, 0)
	}

	return nil, errors.New("no key matching search")
}
