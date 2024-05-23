package clouddb

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type DB struct {
	name     string
	store    *leveldb.DB
	rpc      RPC
	start    time.Time
	status   Status
	statusLk sync.RWMutex
	statusCd *sync.Cond
	syncRate float64

	runq         chan *dblog
	peersState   map[string]*peerInfo
	peersStateLk sync.RWMutex

	typMap map[string]*Type
	typLk  sync.RWMutex

	prevPkt byte // for debug
}

func New(name string, rpc RPC) (*DB, error) {
	// build new db
	// path will be in os.UserConfigDir() unless an absolute path is given, or we cannot get os.UserConfigDir() (in which case we use os.TempDir())
	p := filepath.Join(os.TempDir(), name)

	if filepath.IsAbs(name) {
		p = name
		name = filepath.Base(name)
	} else if ucd, err := os.UserConfigDir(); err == nil {
		p = filepath.Join(ucd, name)
	}

	// bloom filter makes finding keys faster
	o := &opt.Options{
		Filter: filter.NewBloomFilter(10),
	}

	db, err := leveldb.OpenFile(p, o)
	if err != nil {
		return nil, fmt.Errorf("failed to open leveldb: %w", err)
	}

	// create db object
	res := &DB{
		name:       name,
		store:      db,
		rpc:        rpc,
		start:      time.Now(),
		runq:       make(chan *dblog),
		peersState: make(map[string]*peerInfo),
		typMap:     make(map[string]*Type),
	}
	res.statusCd = sync.NewCond(res.statusLk.RLocker())
	if rpc != nil {
		rpc.Connect(res.recv)
	}

	go res.runner()
	go res.process()

	return res, nil
}

// Set will update a given record in database. id must be unique in the whole db, even across types
func (d *DB) Set(id []byte, val any) error {
	jsonBuf, mapObj, err := interpretObj(val)
	if err != nil {
		return fmt.Errorf("invalid object to store: %w", err)
	}

	// generate log entry
	vers := newRecordVersion()
	l := &dblog{
		Type:    RecordSet,
		Id:      id,
		Version: vers,
		Data:    jsonBuf,
		dataobj: mapObj,
	}

	return d.newdblog(l)
}

// Delete globally removes an entry from the database
func (d *DB) Delete(id []byte) error {
	// generate log entry
	vers := newRecordVersion()
	l := &dblog{
		Type:    RecordDelete,
		Id:      id,
		Version: vers,
	}

	return d.newdblog(l)
}

// newdblog receives & process an locally created log record
func (d *DB) newdblog(l *dblog) error {
	l.res = make(chan error)
	d.runq <- l
	return <-l.res
}

// Has will check if a given key exists
func (d *DB) Has(id []byte) (bool, error) {
	return d.store.Has(append([]byte("dat"), id...), nil)
}

// Get will load and apply the object for the given id to target
func (d *DB) Get(id []byte, target any) error {
	v, err := d.GetRaw(id)
	if err != nil {
		return err
	}
	return json.Unmarshal(v, target)
}

// GetRaw returns the json value for a given id
func (d *DB) GetRaw(id []byte) (json.RawMessage, error) {
	kS := append([]byte("dat"), id...)
	res, err := d.store.Get(kS, nil)
	if err != nil {
		if errors.Is(err, leveldb.ErrNotFound) {
			// force not found errors to be fs.ErrNotExist
			return nil, fs.ErrNotExist
		}
	}
	return res, err
}

func (d *DB) Search(typ string, search map[string]any) (Iterator, error) {
	t, err := d.getType(typ)
	if err != nil {
		return nil, err
	}
	// attempt to find index
	pfx, err := t.findSearchPrefix(search)
	if err != nil {
		return nil, err
	}
	// prepare prefix
	kPfx := append([]byte("idx"), pfx...)
	iter := d.store.NewIterator(util.BytesPrefix(kPfx), nil)

	// return iterator
	return &indirectIterator{Iterator: iter, prefix: kPfx, db: d}, nil
}

// SearchFirst will find the first record matching the search params and set target
// search must match an existing key in the provided type
func (d *DB) SearchFirst(typ string, search map[string]any, target any) ([]byte, error) {
	id, v, err := d.SearchFirstRaw(typ, search)
	if err != nil {
		return nil, err
	}
	return id, json.Unmarshal(v, target)
}

// SearchFirstRaw will find the first record matching the search params and return its json value
// search must match an existing key in the provided type
func (d *DB) SearchFirstRaw(typ string, search map[string]any) ([]byte, json.RawMessage, error) {
	t, err := d.getType(typ)
	if err != nil {
		return nil, nil, err
	}
	// attempt to find index
	pfx, err := t.findSearchPrefix(search)
	if err != nil {
		return nil, nil, err
	}
	// search with idx+pfx
	kPfx := append([]byte("idx"), pfx...)
	iter := d.store.NewIterator(util.BytesPrefix(kPfx), nil)
	defer iter.Release()

	for iter.Next() {
		id := iter.Value()
		res, err := d.GetRaw(id)
		return id, res, err
	}
	return nil, nil, fs.ErrNotExist
}

func (d *DB) String() string {
	return d.name
}

func (d *DB) getKeys(id []byte) ([][]byte, error) {
	// get keys currently stored in db for this id
	kdt, err := d.store.Get(append([]byte("kdt"), id...), nil)
	if err != nil {
		if errors.Is(err, leveldb.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return parseKeysData(kdt)
}

func (d *DB) Close() error {
	// TODO close sync process
	return d.store.Close()
}
