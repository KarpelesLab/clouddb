package clouddb_test

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/KarpelesLab/clouddb"
	"github.com/KarpelesLab/rpctest"
)

func init() {
	clouddb.RegisterType(&clouddb.Type{
		Name: "test_rpc",
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

func TestSyncRPC(t *testing.T) {
	rpc := rpctest.NewSync()
	rpca := rpctest.NewLogPeer(rpc.NewPeer("a"))
	rpcb := rpctest.NewLogPeer(rpc.NewPeer("b"))

	// create a local db, test stuff
	p := filepath.Join(os.TempDir(), fmt.Sprintf("clouddb-test-%d", os.Getpid()))
	// make sure p does not exist & won't exist after we're done with it
	defer os.RemoveAll(p)
	os.RemoveAll(p)

	log.Printf("using this path for RPC testing: %s", p)

	dba, err := clouddb.New(filepath.Join(p, "a"), rpca)
	if err != nil {
		t.Fatalf("failed to init db a: %s", err)
		return
	}
	defer dba.Close()

	dbb, err := clouddb.New(filepath.Join(p, "b"), rpcb)
	if err != nil {
		t.Fatalf("failed to init db b: %s", err)
		return
	}
	defer dbb.Close()

	dba.WaitReady() // should happen almost instantly on local mode
	dbb.WaitReady()

	dba.Set([]byte("test001"), map[string]any{"@type": "test_rpc", "foo": "abc"})
	dba.Set([]byte("test002"), map[string]any{"@type": "test_rpc", "foo": "def"})
	dba.Set([]byte("test003"), map[string]any{"@type": "test_rpc", "foo": "ghi"})
	dba.Set([]byte("test004"), map[string]any{"@type": "test_rpc", "foo": "jkl"})

	dbb.Set([]byte("test005"), map[string]any{"@type": "test_rpc", "foo": "mno"})

	// wait a little bit
	time.Sleep(100 * time.Millisecond)

	var v map[string]any
	err = dbb.Get([]byte("test001"), &v)
	if err != nil {
		t.Errorf("failed to read test001 from dbb")
	}
	log.Printf("got record from dbb: %v", v)

	dba.DebugDump(os.Stderr)
	dbb.DebugDump(os.Stderr)

	// new player joins
	rpcc := rpctest.NewLogPeer(rpc.NewPeer("c"))
	dbc, err := clouddb.New(filepath.Join(p, "c"), rpcc)
	if err != nil {
		t.Fatalf("failed to init db c: %s", err)
		return
	}
	defer dbc.Close()

	dbc.WaitReady()

	v = nil
	err = dbb.Get([]byte("test001"), &v)
	if err != nil {
		t.Errorf("failed to read test001 from dbc")
	}

	dbc.DebugDump(os.Stderr)
	// TODO
}
