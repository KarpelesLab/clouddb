package clouddb_test

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"testing"

	"github.com/KarpelesLab/clouddb"
)

type TestObj struct {
	Name string
	Key  string
}

func init() {
	clouddb.RegisterType(&clouddb.Type{
		Name: "test1",
		Keys: []*clouddb.TypeKey{
			&clouddb.TypeKey{
				Name:   "foo",
				Fields: []string{"foo"},
				Method: "utf8",
				Unique: true,
			},
			&clouddb.TypeKey{
				Name:   "bar",
				Fields: []string{"bar"},
				Method: "binary",
				Unique: false,
			},
			&clouddb.TypeKey{
				Name:   "hellobar",
				Fields: []string{"hello", "bar"},
				Method: "utf8",
				Unique: true,
			},
		},
	})
	clouddb.RegisterType(&clouddb.Type{
		Name: "clouddb_test.TestObj",
		Keys: []*clouddb.TypeKey{
			&clouddb.TypeKey{
				Name:   "Name",
				Fields: []string{"Name"},
				Method: "utf8",
				Unique: false,
			},
			&clouddb.TypeKey{
				Name:   "Key",
				Fields: []string{"Key"},
				Method: "binary",
				Unique: true,
			},
		},
	})
}

func TestLocal(t *testing.T) {
	// create a local db, test stuff
	p := filepath.Join(os.TempDir(), fmt.Sprintf("clouddb-test-%d", os.Getpid()))
	// make sure p does not exist & won't exist after we're done with it
	defer os.RemoveAll(p)
	os.RemoveAll(p)

	log.Printf("using this path for testing: %s", p)

	db, err := clouddb.New(filepath.Join(p, "test_a"), nil)
	if err != nil {
		t.Fatalf("failed to init db: %s", err)
		return
	}
	defer db.Close()

	db.WaitReady() // should happen almost instantly on local mode

	// store some data
	err = db.Set([]byte("trec001"), map[string]any{"@type": "test1", "foo": "bar"})
	if err != nil {
		t.Errorf("failed to set record: %s", err)
	}

	err = db.Set([]byte("trec002"), map[string]any{"@type": "test1", "foo": "bar"})
	if err == nil {
		t.Errorf("insert succeeded where it should have failed (duplicate value foo=bar)")
	} else if !errors.Is(err, clouddb.ErrKeyConflict) {
		t.Errorf("unexpected error on dup value foo=bar: %s", err)
	}

	// update a given record
	err = db.Set([]byte("trec001"), map[string]any{"@type": "test1", "foo": "bar", "irrelevant": true})
	if err != nil {
		t.Errorf("failed to update record with unique key: %s", err)
	}

	// fetch record
	var v map[string]any
	id, err := db.SearchFirst("test1", map[string]any{"foo": "bar"}, &v)
	if err != nil {
		t.Errorf("failed to fetch record: %s", err)
	} else if a, b := v["irrelevant"].(bool); string(id) != "trec001" || !a || !b {
		t.Errorf("invalid record fetch from db: %v (expected: map[@type:test1 foo:bar irrelevant:true])", v)
	}

	// insert records with non-unique key
	err = db.Set([]byte("trec003"), map[string]any{"@type": "test1", "bar": "b1", "canary": 1})
	if err != nil {
		t.Errorf("failed to insert trec003: %s", err)
	}
	err = db.Set([]byte("trec004"), map[string]any{"@type": "test1", "bar": "b1", "canary": 2})
	if err != nil {
		t.Errorf("failed to insert trec004: %s", err)
	}

	// find first value with bar=b1 which should have canary=1
	v = nil
	id, err = db.SearchFirst("test1", map[string]any{"bar": "b1"}, &v)
	if err != nil {
		t.Errorf("failed to fetch trec003 record: %s", err)
	} else if n, ok := v["canary"].(float64); string(id) != "trec003" || !ok || n != 1 {
		t.Errorf("invalid record returned for bar=b1: %v", v)
	}

	// search all records for bar=b1
	siter, err := db.Search("test1", map[string]any{"bar": "b1"})
	if err != nil {
		t.Errorf("failed to search bar=b1: %s", err)
	} else {
		defer siter.Release()

		canary := 1
		cnt := 0

		for siter.Next() {
			v = nil
			err := siter.Apply(&v)
			if err != nil {
				t.Errorf("failed to decode %s: %s", siter.Key(), err)
			} else {
				// map[@type:test1 bar:b1 canary:1]
				// map[@type:test1 bar:b1 canary:2]
				if n, ok := v["canary"].(float64); !ok || int(n) != canary {
					t.Errorf("bad canary value for search result key %s", siter.Key())
				}
				cnt += 1
				canary += 1
			}
		}

		if cnt != 2 {
			t.Errorf("search expected to yield 2 results, got %d results", cnt)
		}
	}

	// test collation
	err = db.Set([]byte("trec005"), map[string]any{"@type": "test1", "foo": "HÉｈé", "canary": 3})
	if err != nil {
		t.Errorf("failed to insert trec005: %s", err)
	}
	v = nil
	id, err = db.SearchFirst("test1", map[string]any{"foo": "hehe"}, &v)
	if err != nil {
		t.Errorf("failed to fetch trec005 record: %s", err)
	} else if n, ok := v["canary"].(float64); string(id) != "trec005" || !ok || n != 3 {
		t.Errorf("invalid record returned for foo=hehe: %v", v)
	}

	// test multiple col unique keys
	err = db.Set([]byte("trec006"), map[string]any{"@type": "test1", "hello": 1, "bar": "a", "canary": 4})
	if err != nil {
		t.Errorf("failed to insert trec006: %s", err)
	}
	err = db.Set([]byte("trec007"), map[string]any{"@type": "test1", "hello": 1, "bar": "a", "canary": 5})
	if err == nil {
		t.Errorf("insert succeeded where it should have failed (duplicate value hello=1&bar=a)")
	} else if !errors.Is(err, clouddb.ErrKeyConflict) {
		t.Errorf("unexpected error on dup value hello=1&bar=a: %s", err)
	}
	err = db.Set([]byte("trec008"), map[string]any{"@type": "test1", "hello": 1, "bar": "b", "canary": 6})
	if err != nil {
		t.Errorf("failed to insert trec008: %s", err)
	}

	// load from multiple cols
	v = nil
	id, err = db.SearchFirst("test1", map[string]any{"hello": 1, "bar": "a"}, &v)
	if err != nil {
		t.Errorf("failed to fetch trec006 record: %s", err)
	} else if n, ok := v["canary"].(float64); string(id) != "trec006" || !ok || n != 4 {
		t.Errorf("invalid record returned for hello=1&bar=a: %v", v)
	}

	// store a TestObj object
	err = db.Set([]byte("trec009"), &TestObj{Name: "John Smith", Key: "js007"})
	if err != nil {
		t.Errorf("failed to insert trec009: %s", err)
	}

	var v2 *TestObj
	id, err = db.SearchFirst("clouddb_test.TestObj", map[string]any{"Key": "js007"}, &v2)
	if err != nil {
		t.Errorf("failed to search trec009 record: %s", err)
	} else if string(id) != "trec009" || v2.Name != "John Smith" {
		t.Errorf("invalid record returned for Key=js007: %v", v2)
	}

	db.DebugDump(os.Stderr)

	// generate backup of db
	buf := &bytes.Buffer{}
	err = db.BackupTo(buf)
	if err != nil {
		t.Fatalf("failed to backup db: %s", err)
		return
	}

	log.Printf("Backup of data generated, size = %d bytes", buf.Len())
	//log.Printf("Backup data:\n%s", hex.Dump(buf.Bytes()))

	// validate backup
	err = clouddb.ValidateBackup(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Errorf("failed to validate backup: %s", err)
	}

	db2, err := clouddb.New(filepath.Join(p, "test_b"), nil)
	if err != nil {
		t.Fatalf("failed to init db: %s", err)
		return
	}
	defer db2.Close()

	db2.WaitReady() // should happen almost instantly on local mode

	err = db2.RestoreFrom(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("failed to restore from backup: %s", err)
		return
	}

	// attempt to load a record from this db
	v = nil
	id, err = db2.SearchFirst("test1", map[string]any{"bar": "b1"}, &v)
	if err != nil {
		t.Errorf("failed to fetch trec003 record: %s", err)
	} else if n, ok := v["canary"].(float64); string(id) != "trec003" || !ok || n != 1 {
		t.Errorf("invalid record returned for bar=b1: %v", v)
	}

	// compare all values
	iter := db.NewIterator(nil, nil)

	testkeys := ""

	for iter.Next() {
		testkeys += string(iter.Key())

		db2val, err := db2.GetRaw(iter.Key())
		if err != nil {
			t.Errorf("failed to fetch %s from db2: %s", iter.Key(), err)
		} else if !bytes.Equal(db2val, iter.Value()) {
			t.Errorf("values did not match for key=%s", iter.Key())
		}
	}

	// ensures we went over all keys
	if testkeys != "trec001trec003trec004trec005trec006trec008trec009" {
		t.Errorf("expected testkeys value to be ok, but got %s", testkeys)
	}
}
