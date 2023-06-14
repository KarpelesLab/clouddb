package clouddb

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io/fs"
	"net/url"
	"strconv"
	"strings"
	"sync"

	"github.com/KarpelesLab/typutil"
	"github.com/syndtr/goleveldb/leveldb"
	"golang.org/x/text/collate"
	"golang.org/x/text/language"
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

type TypeKey struct {
	Name   string   `json:"name"`
	Fields []string `json:"fields"`
	Method string   `json:"method"` // one of: utf8, int, binary
	Unique bool     `json:"unique,omitempty"`
}

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
			return nil, fs.ErrNotExist
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
	case Record:
		v, _ = n.Value()
	case json.RawMessage:
		var v any
		json.Unmarshal(n, &v)
	}

	res = append(res, append([]byte(t.Name+"\x00@type\x00"), id...))
	res = append(res, append(append(append([]byte(t.Name+"\x00@version\x00"), t.Hash()...), 0), id...))

	for _, k := range t.Keys {
		res = append(res, k.computeIndices(t, id, v)...)
	}

	return res
}

func (t *Type) findSearchPrefix(search map[string]any) ([]byte, error) {
	// attempt to find a TypeKey that has the exact same fields as search and return the prefix for this search
	if search == nil || len(search) == 0 {
		// search all records of this type
		if t == nil {
			return []byte("undefined\x00@type\x00"), nil
		}
		return []byte(t.Name + "\x00@type\x00"), nil
	}
	if t == nil {
		return nil, errors.New("cannot perform search on unknown/invalid type")
	}

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
		return k.computeSearchPrefix(t, search)
	}

	return nil, errors.New("no key matching search")
}

func (k *TypeKey) computeIndices(t *Type, id []byte, v any) [][]byte {
	var res [][]byte
	encodeCtx := &encodeValueContext{}

	// start key with type name & key name
	newKey := []byte(t.Name + "\x00" + k.Name + "\x00")

	cnt := 0
	forceNotUnique := false
	for _, fn := range k.Fields {
		val := getValueForField(v, fn)
		if val == nil {
			if cnt > 0 {
				// partial key that ends on a nil, force it to be non-unique
				forceNotUnique = true
				break
			}
			// first part of key is missing, drop the whole key
			return nil
		}
		cnt += 1
		newKey = append(append(newKey, 0), k.encodeValue(encodeCtx, val)...)
	}

	if forceNotUnique || !k.Unique {
		// append object ID to ensure key will not hit anything existing
		newKey = append(append(newKey, 0), id...)
	}

	res = append(res, newKey)

	return res
}

func (k *TypeKey) computeSearchPrefix(t *Type, search map[string]any) ([]byte, error) {
	encodeCtx := &encodeValueContext{}
	newKey := []byte(t.Name + "\x00" + k.Name + "\x00")

	for _, fn := range k.Fields {
		val := search[fn]
		if val == nil {
			return nil, errors.New("search value cannot be nil")
		}

		newKey = append(append(newKey, 0), k.encodeValue(encodeCtx, val)...)
	}

	return newKey, nil
}

func (k *TypeKey) encodeValue(ctx *encodeValueContext, val any) []byte {
	switch k.Method {
	case "utf8": // utf8_general_ci
		col := collate.New(language.Und, collate.IgnoreCase, collate.IgnoreWidth)
		ss, _ := typutil.AsString(val)
		if ctx.colBuf == nil {
			ctx.colBuf = &collate.Buffer{}
		}
		return col.KeyFromString(ctx.colBuf, ss)
	case "binary":
		ss, _ := typutil.AsString(val)
		return []byte(ss)
	default:
		// do same as binary
		ss, _ := typutil.AsString(val)
		return []byte(ss)
	}
}

func getValueForField(v any, f string) any {
	fa := strings.Split(f, "/")

	for _, s := range fa {
		switch o := v.(type) {
		case map[string]any:
			v = o[s]
		case map[string]string:
			v = o[s]
		case url.Values:
			v = o[s]
		case []any:
			n, err := strconv.ParseInt(s, 0, 64)
			if err != nil {
				return nil
			}
			if n < 0 || int(n) > len(o) {
				return nil
			}
			v = o[n]
		default:
			return nil
		}
	}
	return v
}
