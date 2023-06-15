package clouddb_test

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"testing"

	"github.com/KarpelesLab/clouddb"
	"github.com/KarpelesLab/rpctest"
)

func TestSyncRPC(t *testing.T) {
	rpc := rpctest.NewSyncLog()
	rpca := rpc.NewPeer("a")
	rpcb := rpc.NewPeer("b")

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
	// TODO
}
