package clouddb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"time"

	"github.com/syndtr/goleveldb/leveldb/util"
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

// GetStatus returns the current status of this database instance
func (d *DB) GetStatus() Status {
	d.statusLk.RLock()
	defer d.statusLk.RUnlock()

	return d.status
}

// WaitReady waits for the database instance to be ready, or does nothing if it is already ready
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
	success := 0

	for i := 0; i < 10; i++ {
		res, err := d.rpc.All(ctx, pkt)
		if err != nil {
			log.Printf("[clouddb] initial sync broadcast failed: %s", err)
		} else {
			d.feedBroadcastGetInfo(ctx, res)
			success += 1
			if success > 5 {
				// let's consider we're online, TODO: unless we're syncing?
				d.setStatus(Ready)
			}
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
		res := &bytes.Buffer{}
		res.WriteByte(PktGetInfoResp)
		selfId := d.rpc.Self()
		binary.Write(res, binary.BigEndian, uint16(len(selfId)))
		res.WriteString(selfId)

		// write all my checkpoints
		iter := d.store.NewIterator(util.BytesPrefix([]byte("chk")), nil)
		defer iter.Release()

		for iter.Next() {
			// checkpoints have a fixed 60 bytes length, so we can just write all of them continuously (max 6kB of data)
			res.Write(iter.Value())
		}

		if len(buf) == 1 {
			return res.Bytes(), nil
		}
		// send packet
		return nil, d.rpc.Send(ctx, string(buf[1:]), res.Bytes())
	case PktGetInfoResp:
		// Receive response
		buf = buf[1:]
		if len(buf) < 2 {
			log.Printf("bad PktGetInfoResp pkt")
			return nil, nil
		}
		ln := binary.BigEndian.Uint16(buf[:2])
		buf = buf[2:]
		if len(buf) < int(ln) {
			log.Printf("bad PktGetInfoResp pkt 2")
			return nil, nil
		}
		remote := string(buf[:ln])
		buf = buf[ln:] // should be checkpoints starting this point
		d.ingestCheckpoints(remote, buf)
	default:
		log.Printf("[clouddb] Received object %d", buf[0])
	}
	return nil, nil
}

func (d *DB) ingestCheckpoints(peer string, buf []byte) {
	// buf is a number of *checkpoint binary data end to end
	log.Printf("todo check checkpoints ln=%d", len(buf))

	ckpt := &checkpoint{}
	r := bytes.NewReader(buf)

	for {
		_, err := ckpt.ReadFrom(r)
		if err != nil {
			if err == io.EOF {
				return
			}
			log.Printf("[sync] unable to read checkpoint from peer %s: %s", peer, err)
			// give up since we're probably not in the right location in the buffer
			return
		}

		newer, err := d.isCheckpointNewer(peer, ckpt)
		if err != nil {
			log.Printf("[sync] error while checking if checkpoint is newer: %s", err)
			continue
		}
		if newer {
			// need to trigger sync of transactions up to this checkpoint
		}
	}
}

func (d *DB) isCheckpointNewer(peer string, ckpt *checkpoint) (bool, error) {
	myCkpt, err := d.loadCheckpoint(ckpt.epoch)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			// we're missing this checkpoint
			return true, nil
		}
		// can't verify system is down
		return false, err
	}

	if myCkpt.logcnt < ckpt.logcnt {
		// we're missing some logs
		return true, nil
	}
	if myCkpt.logcnt > ckpt.logcnt {
		// we have more logs (for now)
		return false, nil
	}
	if !bytes.Equal(myCkpt.logsum, ckpt.logsum) {
		// we have the same logcnt but not the same logs → we're missing some
		return true, nil
	}
	// all is equal, all is good
	return false, nil
}
