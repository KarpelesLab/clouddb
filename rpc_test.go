package clouddb_test

import (
	"context"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"testing"

	"github.com/KarpelesLab/clouddb"
)

type fakeRPC interface {
	clouddb.RPC
	run([]byte) ([]byte, error)
}

type rpcSyncPool map[string]*rpcSyncPeer

func (p rpcSyncPool) newPeer(id string) clouddb.RPC {
	n := &rpcSyncPeer{
		pool: p,
		id:   id,
	}
	p[id] = n
	return n
}

type rpcSyncPeer struct {
	pool   rpcSyncPool
	id     string
	target func(context.Context, []byte) ([]byte, error)
}

func (r *rpcSyncPeer) All(ctx context.Context, data []byte) ([]any, error) {
	var res []any
	for _, p := range r.pool {
		r, e := p.run(ctx, data)
		if e != nil {
			res = append(res, e)
		} else {
			res = append(res, r)
		}
	}
	return res, nil
}

func (r *rpcSyncPeer) Broadcast(ctx context.Context, data []byte) error {
	for _, p := range r.pool {
		p.run(ctx, data)
	}
	return nil
}

func (r *rpcSyncPeer) Request(ctx context.Context, id string, data []byte) ([]byte, error) {
	p, ok := r.pool[id]
	if !ok {
		return nil, fs.ErrNotExist
	}
	return p.run(ctx, data)
}

func (r *rpcSyncPeer) Send(ctx context.Context, id string, data []byte) error {
	if p, ok := r.pool[id]; ok {
		p.run(ctx, data)
	}
	return nil
}

func (r *rpcSyncPeer) Self() string {
	return r.id
}

func (r *rpcSyncPeer) Connect(cb func(context.Context, []byte) ([]byte, error)) {
	r.target = cb
}

func (r *rpcSyncPeer) run(ctx context.Context, data []byte) ([]byte, error) {
	t := r.target
	if t != nil {
		return t(ctx, data)
	}
	return nil, fs.ErrNotExist
}

func TestSyncRPC(t *testing.T) {
	rpc := make(rpcSyncPool)
	rpca := rpc.newPeer("a")
	rpcb := rpc.newPeer("b")

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

	// dba.WaitReady() // should happen almost instantly on local mode
	// TODO
}
