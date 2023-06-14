package clouddb_test

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"testing"

	"github.com/KarpelesLab/clouddb"
)

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
}

func TestLocal(t *testing.T) {
	// create a local db, test stuff
	p := filepath.Join(os.TempDir(), fmt.Sprintf("clouddb-test-%d", os.Getpid()))
	// make sure p does not exist & won't exist after we're done with it
	defer os.RemoveAll(p)
	os.RemoveAll(p)

	log.Printf("using this path for testing: %s", p)

	db, err := clouddb.New(p, nil)
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
}
