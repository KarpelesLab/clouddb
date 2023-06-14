package clouddb

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"

	"github.com/syndtr/goleveldb/leveldb"
)

type LogType uint8

const (
	RecordSet    LogType = iota // set the full value of a record
	RecordDelete                // delete a record
)

type Log struct {
	Type    LogType
	Id      []byte
	Version RecordVersion
	Data    json.RawMessage
	dataobj map[string]any // cached representation of Data
}

func (l *Log) apply(d *DB, b *leveldb.Batch, force bool) error {
	switch l.Type {
	case RecordSet:
		nfoKey := append([]byte("nfo"), l.Id...)
		nfo, err := d.store.Get(nfoKey, nil)
		if err != nil {
			if !errors.Is(err, leveldb.ErrNotFound) {
				return err
			}
		}
		if nfo != nil {
			curVer := parseRecordVersion(nfo[:16])
			if l.Version.Less(curVer) {
				// current version is already more recent, only store log
				b.Put(l.key(), l.Bytes())
				return nil
			}
		}
		// store
		beforeKeys, err := d.getKeys(l.Id)
		if err != nil {
			return err
		}
		beforeKeysMap := keysToMap(beforeKeys)

		keys, err := l.getObjectKeys(d)
		if err != nil {
			return err
		}

		// check / set keys
		for _, k := range keys {
			if _, found := beforeKeysMap[string(k)]; found {
				// it (should) exists, remove it from the list
				delete(beforeKeysMap, string(k))
				continue
			}
			kS := append([]byte("idx"), k...)
			v, err := d.store.Get(kS, nil)
			if err == nil {
				if !force && !bytes.Equal(v, l.Id) {
					return ErrKeyConflict
				}
				// do not Put since already good value
				continue
			} else if err != nil && !errors.Is(err, leveldb.ErrNotFound) {
				return err
			}
			b.Put(kS, l.Id)
		}

		b.Put(l.key(), l.Bytes())
		b.Put(nfoKey, l.Version.Bytes())
		b.Put(append([]byte("kdt"), l.Id...), buildKeysData(keys)) // always put even if empty so we override previous version
		b.Put(append([]byte("dat"), l.Id...), l.Data)
		for k := range beforeKeysMap {
			b.Delete(append([]byte("idx"), k...))
		}
		return nil
	case RecordDelete:
		nfoKey := append([]byte("nfo"), l.Id...)
		nfo, err := d.store.Get(nfoKey, nil)
		if err != nil {
			if !errors.Is(err, leveldb.ErrNotFound) {
				return err
			}
		}
		if nfo != nil {
			curVer := parseRecordVersion(nfo[:16])
			if l.Version.Less(curVer) {
				// current version is newer than this delete
				b.Put(l.key(), l.Bytes())
				return nil
			}
		}
		// read keys for records
		keys, err := d.getKeys(l.Id)
		if err != nil {
			return err
		}
		// perform delete
		b.Put(l.key(), l.Bytes())
		b.Delete(append([]byte("dat"), l.Id...))
		b.Delete(append([]byte("kdt"), l.Id...))
		b.Delete(nfoKey)
		for _, k := range keys {
			b.Delete(append([]byte("idx"), k...))
		}
		return nil
	}
	return errors.New("unsupported log type")
}

// key returns the log's internal storage key
func (l *Log) key() []byte {
	return append(append([]byte("log"), l.Version.Bytes()...), l.Id...)
}

func (l *Log) getObjectKeys(d *DB) ([][]byte, error) {
	if l.dataobj == nil {
		// parse l.Data into l.dataobj
		err := json.Unmarshal(l.Data, &l.dataobj)
		if err != nil {
			return nil, err
		}
	}

	typ, ok := l.dataobj["@type"].(string)
	if !ok {
		typ = "invalid"
	}
	tobj, err := d.getType(typ)

	// compute indices
	var keys [][]byte
	if err == nil {
		keys = tobj.computeIndices(l.Id, l.dataobj)
	}

	return keys, err
}

// Bytes return the binary representation of this log entry
func (l *Log) Bytes() []byte {
	// prepare binary structure for log message
	// CDBL = CloudDB Log (we use this as header so disaster recovery can at least find those records)

	// CDBL <version+flags>:uint32 <version>:16bytes <keyLen>:16bits <key>:... <data>...
	// len is 26 bytes + <len of key> + <len of data>
	ln := 26 + len(l.Id) + len(l.Data)

	buf := make([]byte, ln)
	copy(buf[:4], "CDBL")
	binary.BigEndian.PutUint32(buf[4:8], 0)
	l.Version.Put(buf[8:24])
	binary.BigEndian.PutUint16(buf[24:26], uint16(len(l.Id)))
	copy(buf[26:], l.Id)
	copy(buf[26+len(l.Id):], l.Data)

	return buf
}

func (l *Log) Hash() []byte {
	h := sha256.New()
	l.WriteTo(h)
	return h.Sum(nil)
}

func (l *Log) WriteTo(w io.Writer) (int64, error) {
	n1, err := w.Write([]byte("CDBL"))
	if err != nil {
		return int64(n1), err
	}

	err = binary.Write(w, binary.BigEndian, uint32(0)) // version+flags
	n1 += 4
	if err != nil {
		return int64(n1), err
	}

	n2, err := w.Write(l.Version.Bytes())
	n1 += n2
	if err != nil {
		return int64(n1), err
	}

	err = binary.Write(w, binary.BigEndian, uint16(len(l.Id)))
	n1 += 2
	if err != nil {
		return int64(n1), err
	}

	n2, err = w.Write(l.Id)
	n1 += n2
	if err != nil {
		return int64(n1), err
	}

	n2, err = w.Write(l.Data)
	n1 += n2
	if err != nil {
		return int64(n1), err
	}

	return int64(n1), nil
}

func (l *Log) MarshalBinary() ([]byte, error) {
	return l.Bytes(), nil
}

func (l *Log) UnmarshalBinary(buf []byte) error {
	// min len = 26(+3)
	if len(buf) < 26 {
		return errors.New("failed parsing log: buffer too short")
	}
	if string(buf[:4]) != "CDBL" {
		return errors.New("failed parsing log: invalid header")
	}
	versFlags := binary.BigEndian.Uint32(buf[4:8])
	if versFlags != 0 {
		return errors.New("failed parsing log: unsupported version and/or flags")
	}
	l.Version = parseRecordVersion(buf[8:24])
	idLen := binary.BigEndian.Uint16(buf[24:26])
	if len(buf) < int(26+idLen) {
		return errors.New("failed parsing log: buffer is incomplete")
	}
	if idLen == 0 {
		l.Id = nil
	} else {
		l.Id = buf[26 : 26+idLen]
	}
	l.Data = buf[26+idLen:]
	return nil
}
