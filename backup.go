package clouddb

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// BackupTo will generate a snapshot and write it to the given writer. The format
// is fairly simple and can be fed to RestoreFrom to restore the database to a given
// point in time.
//
// Backup will include part of the database record modification history, so the
// backup data will usually be larger than the original database, but because the
// data has a lot of repetition, compression should be effective.
func (d *DB) BackupTo(w io.Writer) error {
	snap, err := d.store.GetSnapshot()
	if err != nil {
		return err
	}

	out := bufio.NewWriter(w)

	_, err = out.WriteString("CDBK") // CloudDB Backup
	if err != nil {
		return err
	}
	err = binary.Write(out, binary.BigEndian, uint32(0)) // version+flags
	if err != nil {
		return err
	}

	// include all types, checkpoints, logs & data
	backupPrefixes := []string{"typ", "chk", "log", "nfo", "dat"}

	for _, pfx := range backupPrefixes {
		err = d.backupPrefix(out, snap, pfx)
		if err != nil {
			return err
		}
	}

	// Add EOF mark
	writeVarint(out, -1)

	// flush output & return any error
	return out.Flush()
}

// backupPrefix will write data for a given prefix to the output file
func (d *DB) backupPrefix(out *bufio.Writer, snap *leveldb.Snapshot, pfx string) error {
	iter := snap.NewIterator(util.BytesPrefix([]byte(pfx)), nil)
	defer iter.Release()

	lnbuf := make([]byte, binary.MaxVarintLen64)
	var err error
	var n int

	for iter.Next() {
		// write key
		n = binary.PutVarint(lnbuf, int64(len(iter.Key())))
		_, err = out.Write(lnbuf[:n])
		if err != nil {
			return err
		}
		_, err = out.Write(iter.Key())
		if err != nil {
			return err
		}

		// write value
		n = binary.PutVarint(lnbuf, int64(len(iter.Value())))
		_, err = out.Write(lnbuf[:n])
		if err != nil {
			return err
		}
		_, err = out.Write(iter.Value())
		if err != nil {
			return err
		}
	}
	return nil
}

func writeVarint(out io.Writer, v int64) error {
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutVarint(buf, v)
	_, err := out.Write(buf[:n])
	return err
}

// ValidateBackup will read a backup and check for some possible errors, such as corrupted checkpoints
//
// It is a good practice to call ValidateBackup after creating a backup to ensure it will be possible
// to restore it in the future should the need arise.
func ValidateBackup(r io.Reader) error {
	b := bufio.NewReader(r)

	h := make([]byte, 4)
	_, err := io.ReadFull(b, h)
	if err != nil {
		return err
	}
	if string(h) != "CDBK" {
		return errors.New("invalid backup file (bad header)")
	}
	var flags uint32
	err = binary.Read(b, binary.BigEndian, &flags)
	if err != nil {
		return err
	}
	if flags != 0 {
		return errors.New("invalid backup file (unsupported flags or version")
	}

	ckpts := make(map[int64]*checkpoint)
	ckptVerif := make(map[int64]*checkpoint)
	typ := make(map[string]bool) // we don't actually care about content of the type data while reading

	for {
		ln, err := binary.ReadVarint(b)
		if err != nil {
			return err
		}
		if ln == -1 {
			// reached EOF, stop reading here
			break
		}
		// we don't like keys that are too long
		if ln > 65535 {
			return errors.New("invalid file, has key with length >65535 bytes")
		}
		k := make([]byte, ln)
		_, err = io.ReadFull(b, k)
		if err != nil {
			return err
		}

		ln, err = binary.ReadVarint(b)
		if err != nil {
			return err
		}
		// this is kinda arbitrary, we can actually process records that large, but wtf are you storing in there?
		if ln > 128*1024*1024 {
			return errors.New("invalid file, has record with length >128MB")
		}
		v := make([]byte, ln)
		_, err = io.ReadFull(b, v)
		if err != nil {
			return err
		}

		pfx := string(k[:3])
		switch pfx {
		case "typ":
			// parse it
			t := &Type{}
			err = json.Unmarshal(v, &t)
			if err != nil {
				return err
			}
			tname := string(k[3:])
			if tname != t.Name {
				return fmt.Errorf("type %s has wrong key name", t.Name)
			}
			// ok this type is valid, record it
			typ[t.Name] = true
		case "chk":
			ckpt := &checkpoint{}
			err = ckpt.UnmarshalBinary(v)
			if err != nil {
				return err
			}
			ckpts[ckpt.epoch] = ckpt
			ckptVerif[ckpt.epoch] = &checkpoint{epoch: ckpt.epoch, logsum: make([]byte, 32)}
		case "log":
			l := &dblog{}
			err = l.UnmarshalBinary(v)
			if err != nil {
				return err
			}
			// check if we have the appropriate checkpoint, add this log to the Verif ckpt
			if ckpt, ok := ckptVerif[l.Version.epoch()+1]; ok {
				ckpt.add(l)
			} else {
				return fmt.Errorf("found log [%s] but no corresponding checkpoint", l)
			}
		case "nfo":
			// check len?
		case "dat":
			var x map[string]any
			err = json.Unmarshal(v, &x)
			if err != nil {
				return fmt.Errorf("record %q parse error: %w", string(k[3:]), err)
			}
			t := "invalid"
			if st, ok := x["@type"].(string); ok {
				t = st
			}
			if t != "invalid" {
				if _, ok := typ[t]; !ok {
					return fmt.Errorf("record %q has unknown type %s", string(k[3:]), t)
				}
			}
		default:
			return fmt.Errorf("backup contains unexpected record type %s", pfx)
		}
	}

	// check that checkpoints in ckpts match the ones in ckptVerif
	for epoch := range ckpts {
		a := ckpts[epoch]
		b := ckptVerif[epoch]

		if a.logcnt != b.logcnt {
			return fmt.Errorf("invalid checkpoint in file: %s (expected %s)", a, b)
		}
		if !bytes.Equal(a.logsum, b.logsum) {
			return fmt.Errorf("invalid checkpoint in file: %s (expected %s)", a, b)
		}
	}
	return nil
}

// RestoreFrom will read a backup and restore the database from it. The current
// database must be pristine, or this will fail.
//
// After a restore, in case of a cluster configuration, other peers will start
// replicating the data.
// This method will skip some basic checks on the input, and ValidateBackup should
// be called first to ensure backup usability (actually, ValidateBackup should
// be called after saving the backup as detecting invalid backups is more useful
// while the database still exists).
func (d *DB) RestoreFrom(r io.Reader) error {
	tx, err := d.store.OpenTransaction()
	if err != nil {
		return err
	}
	defer tx.Discard() // discard by default, we will commit at the end

	b := bufio.NewReader(r)

	h := make([]byte, 4)
	_, err = io.ReadFull(b, h)
	if err != nil {
		return err
	}
	if string(h) != "CDBK" {
		return errors.New("invalid backup file (bad header)")
	}
	var flags uint32
	err = binary.Read(b, binary.BigEndian, &flags)
	if err != nil {
		return err
	}
	if flags != 0 {
		return errors.New("invalid backup file (unsupported flags or version")
	}

	for {
		ln, err := binary.ReadVarint(b)
		if err != nil {
			return err
		}
		if ln == -1 {
			// reached EOF, stop reading here
			break
		}
		// we don't like keys that are too long
		if ln > 65535 {
			return errors.New("invalid file, has key with length >65535 bytes")
		}
		k := make([]byte, ln)
		_, err = io.ReadFull(b, k)
		if err != nil {
			return err
		}

		ln, err = binary.ReadVarint(b)
		if err != nil {
			return err
		}
		// this is kinda arbitrary, we can actually process records that large, but wtf are you storing in there?
		if ln > 128*1024*1024 {
			return errors.New("invalid file, has record with length >128MB")
		}
		v := make([]byte, ln)
		_, err = io.ReadFull(b, v)
		if err != nil {
			return err
		}

		pfx := string(k[:3])
		switch pfx {
		case "chk":
			// run duplication check on checkpoints
			exists, err := tx.Has(k, nil)
			if err != nil {
				return err
			}
			if exists {
				return fmt.Errorf("cannot restore backup as checkpoint %x already exists", k[3:])
			}
		}
		tx.Put(k, v, nil)
	}

	// reindex db
	err = d.reindexWithTransaction(tx)
	if err != nil {
		return err
	}

	// commit it
	return tx.Commit()
}
