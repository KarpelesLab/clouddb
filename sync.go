package clouddb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type peerInfo struct {
	name     string
	good     int
	total    int
	syncRate float64
}

func (d *DB) process() {
	// this runs in a goroutine
	if d.rpc == nil {
		d.updateSyncRate(1) // 100%
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

func (d *DB) updateSyncRate(val float64) {
	d.statusLk.Lock()
	defer d.statusLk.Unlock()

	log.Printf("[clouddb] %s sync status: %01.2f%%", d.name, val*100)

	d.syncRate = val

	if val > .98 && d.status == Syncing {
		d.status = Ready
		d.statusCd.Broadcast()
	}
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
	ctx := context.Background()

	// launch a few queries with short interval first to seed data quickly
	d.doGetNetInfo(ctx)
	time.Sleep(100 * time.Millisecond)
	d.doGetNetInfo(ctx)
	time.Sleep(250 * time.Millisecond)
	d.doGetNetInfo(ctx)
	time.Sleep(500 * time.Millisecond)
	d.doGetNetInfo(ctx)
	time.Sleep(5 * time.Second)
	d.doGetNetInfo(ctx)
	time.Sleep(5 * time.Second)

	for i := 0; i < 10; i++ {
		if d.GetStatus() == Ready {
			break
		}
		d.doGetNetInfo(ctx)
		time.Sleep(5 * time.Second)
	}
	t := time.NewTicker(time.Hour)
	defer t.Stop()

	for {
		// once an hour
		d.doGetNetInfo(ctx)
		<-t.C
	}
}

func (d *DB) doGetNetInfoSingle(ctx context.Context, peer string) error {
	buf, err := d.rpc.Request(ctx, peer, []byte{PktGetInfo})
	if err != nil {
		return err
	}
	if len(buf) > 0 && buf[0] == PktGetInfoResp {
		_, err = d.recv(ctx, buf)
		return err
	}
	return errors.New("invalid PktGetInfo response")
}

func (d *DB) doGetNetInfo(ctx context.Context) error {
	err := d.rpc.Broadcast(ctx, append([]byte{PktGetInfo}, d.rpc.Self()...))
	if err != nil {
		log.Printf("[clouddb] GetInfo broadcast failed: %s", err)
		return err
	}
	return nil
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
		res.Write(strln16(d.rpc.Self()))

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
		remote := getstrln16(&buf)
		if remote == "" {
			log.Printf("bad PktGetInfoResp pkt")
			return nil, nil
		}
		d.ingestCheckpoints(remote, buf)
	case PktGetLogIds:
		buf = buf[1:]
		peer := getstrln16(&buf)
		epoch := int64(getuint64be(&buf))
		go d.sendLogIdsToPeer(peer, epoch)
	case PktFetchLog:
		// fetch a given log entry
		key := append([]byte("log"), buf[1:]...)
		data, err := d.store.Get(key, nil)
		if err != nil {
			if errors.Is(err, leveldb.ErrNotFound) {
				err = fs.ErrNotExist
			}
		}
		return data, err
	case PktLogPush:
		buf = buf[1:]
		l := &dblog{}
		err := l.UnmarshalBinary(buf)
		if err != nil {
			log.Printf("[clouddb] failed to parse log packet from peer: %s", err)
			return nil, err
		}
		d.runq <- l
	case PktLogIdsPush:
		buf = buf[1:]
		peer := getstrln16(&buf)
		go d.processLogIdsFromPeer(peer, buf)
	default:
		log.Printf("[clouddb] Received object %d", buf[0])
	}
	return nil, nil
}

func (d *DB) ingestCheckpoints(peer string, buf []byte) {
	// buf is a number of *checkpoint binary data end to end

	ckpt := &checkpoint{}
	r := bytes.NewReader(buf)

	total := 0
	good := 0

	for {
		_, err := ckpt.ReadFrom(r)
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Printf("[sync] unable to read checkpoint from peer %s: %s", peer, err)
			// give up since we're probably not in the right location in the buffer
			return
		}

		newer, err := d.isCheckpointNewer(peer, ckpt, &good, &total)
		if err != nil {
			log.Printf("[sync] error while checking if checkpoint is newer: %s", err)
			continue
		}
		if newer {
			// need to trigger sync of transactions up to this checkpoint
			go d.requestCheckpointFromPeer(peer, ckpt)
		}
	}

	var syncRate float64

	if total == good {
		syncRate = 1
	} else {
		syncRate = float64(good) / float64(total)
	}

	log.Printf("[clouddb] %s checkpoints status: %d/%d sync (%01.2f%%)", peer, good, total, syncRate*100)
	d.setPeerSyncRate(peer, good, total, syncRate)
}

func (d *DB) requestCheckpointFromPeer(peer string, ckpt *checkpoint) {
	log.Printf("[clouddb] %s requesting checkpoint=[%s] from %s", d, ckpt, peer)
	req := append(append([]byte{PktGetLogIds}, strln16(d.rpc.Self())...), uint64be(uint64(ckpt.epoch))...)

	d.rpc.Send(context.Background(), peer, req)
}

func (d *DB) setPeerSyncRate(peer string, good, total int, syncRate float64) {
	d.peersStateLk.Lock()
	defer d.peersStateLk.Unlock()

	state := &peerInfo{
		name:     peer,
		good:     good,
		total:    total,
		syncRate: syncRate,
	}
	d.peersState[peer] = state

	var totalSync float64
	for _, p := range d.peersState {
		totalSync += p.syncRate
	}

	totCnt := d.rpc.CountAllPeers()
	if totCnt < 2 {
		// this is wrong! :(
		// let's use our own known peers count
		totCnt = len(d.peersState)
	}

	d.updateSyncRate(totalSync / float64(totCnt-1))
}

func (d *DB) isCheckpointNewer(peer string, ckpt *checkpoint, good, total *int) (bool, error) {
	*total += int(ckpt.logcnt)

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
		*good += int(myCkpt.logcnt)
		// we're missing some logs
		return true, nil
	}
	// add from ckpt because we don't want good>total
	*good += int(ckpt.logcnt)

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

func (d *DB) broadcastLogs(data [][]byte) {
	if d.rpc == nil {
		return
	}

	ctx := context.Background()

	for _, buf := range data {
		d.rpc.Broadcast(ctx, append([]byte{PktLogPush}, buf...))
	}
}

func (d *DB) sendLogIdsToPeer(peer string, epoch int64) {
	// load matching checkpoint
	ckpt, err := d.loadCheckpoint(epoch)
	if err != nil {
		log.Printf("[clouddb] sync failed to send epoch=%d to peer=%s: %s", epoch, peer, err)
		return
	}
	log.Printf("[clouddb] Sending log ids to %s for checkpoint=[%s]", peer, ckpt)

	iter := d.store.NewIterator(ckpt.logRange(), nil)
	defer iter.Release()

	ctx := context.Background()

	buf := strln16(d.rpc.Self())
	cnt := 0

	for iter.Next() {
		buf = append(buf, byteln8(iter.Key()[3:])...)
		cnt += 1
		if len(buf) > 4096 {
			// send it now
			d.rpc.Send(ctx, peer, append([]byte{PktLogIdsPush}, buf...))
			// reset vars
			buf = strln16(d.rpc.Self())
			cnt = 0
		}
	}
	if cnt > 0 {
		d.rpc.Send(ctx, peer, append([]byte{PktLogIdsPush}, buf...))
	}
}
func (d *DB) processLogIdsFromPeer(peer string, buf []byte) {
	var missingIds [][]byte

	for len(buf) > 0 {
		id := getbyteln8(&buf)
		if len(id) == 0 {
			// end of list? invalid?
			break
		}
		// id is a log id
		found, err := d.store.Has(append([]byte("log"), id...), nil)
		if err != nil {
			log.Printf("[clouddb] processLogIdsFromPeer failed: %s", err)
			// give up at this point
			return
		}
		if !found {
			missingIds = append(missingIds, id)
		}
	}

	if len(missingIds) == 0 {
		return
	}

	ctx := context.Background()

	for _, id := range missingIds {
		buf, err := d.rpc.Request(ctx, peer, append([]byte{PktFetchLog}, id...))
		if err != nil {
			log.Printf("[clouddb] failed fetching log id %x from %s: %s", id, peer, err)
			continue
		}
		l := &dblog{}
		err = l.UnmarshalBinary(buf)
		if err != nil {
			log.Printf("[clouddb] failed decoding log id %x from %s: %s", id, peer, err)
			continue
		}
		d.runq <- l
	}
	d.doGetNetInfoSingle(ctx, peer)
}
