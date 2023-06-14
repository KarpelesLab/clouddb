package clouddb

import (
	"context"
	"fmt"
	"log"
	"time"
)

func (d *DB) process() {
	// this runs in a goroutine
	if d.rpc == nil {
		d.setStatus(Ready)
		return
	}

	d.setStatus(Syncing)

	// first, start the initial sync process
	go d.subprocessGetNetInfo()
}

func (d *DB) setStatus(s Status) {
	d.statusLk.Lock()
	defer d.statusLk.Unlock()

	d.status = s
	d.statusCd.Broadcast()
}

func (d *DB) WaitReady() {
	d.statusLk.RLock()
	defer d.statusLk.RUnlock()

	for {
		if d.status == Ready {
			return
		}
		d.statusCd.Wait()
	}
}

func (d *DB) subprocessGetNetInfo() {
	pkt := []byte{PktGetInfo}
	t := time.NewTicker(5 * time.Second)
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		res, err := d.rpc.All(ctx, pkt)
		if err != nil {
			log.Printf("[clouddb] initial sync broadcast failed: %s", err)
		} else {
			d.feedBroadcastGetInfo(ctx, res)
		}
		<-t.C
	}
}

func (d *DB) feedBroadcastGetInfo(ctx context.Context, data []any) {
	// data items can be of type error or []byte

	for _, v := range data {
		switch buf := v.(type) {
		case error:
			// do nothing
		case []byte:
			// must be a PktGetInfoResp
			if len(buf) > 0 && buf[0] == PktGetInfoResp {
				d.recv(ctx, buf)
			}
		}
	}
}

func (d *DB) recv(ctx context.Context, buf []byte) ([]byte, error) {
	// recv handles a message coming from outside
	if len(buf) == 0 {
		return nil, fmt.Errorf("buffer cannot be empty")
	}

	switch buf[0] {
	case PktGetInfo:
		// return info
		// if len(buf)==1 (empty packet) we return the data, else the data afterward is a node id we need to send the response to
		res := []byte{PktGetInfoResp}
		if len(buf) == 1 {
			return res, nil
		}
		// send packet
		return nil, d.rpc.Send(ctx, string(buf[1:]), res)
	case PktGetInfoResp:
		// Receive response
		log.Printf("got response!")
	default:
		log.Printf("[clouddb] Received object %d", buf[0])
	}
	return nil, nil
}
