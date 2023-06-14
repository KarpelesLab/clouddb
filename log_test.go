package clouddb

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestLogMarshal(t *testing.T) {
	// test log binary format
	l := &Log{
		Type:    RecordSet,
		Id:      []byte("hello123"),
		Version: newRecordVersion(),
		Data:    json.RawMessage([]byte(`{"key":"value"}`)),
	}
	l.Version.srvid = 0xdeadbeef

	buf1 := l.Bytes()
	buf2 := &bytes.Buffer{}
	l.WriteTo(buf2)

	if !bytes.Equal(buf1, buf2.Bytes()) {
		t.Errorf("Log.Bytes() and Log.WriteTo() do not have the same output")
	}

	l2 := &Log{}
	err := l2.UnmarshalBinary(buf1)
	if err != nil {
		t.Errorf("Log.UnmarshalBinary() failed: %s", err)
	}

	// compare values
	if l.Type != l2.Type {
		t.Errorf("Log.UnmarshalBinary() failed: Type value different")
	}
	if !bytes.Equal(l.Id, l2.Id) {
		t.Errorf("Log.UnmarshalBinary() failed: Id value different")
	}
	if l.Version.unix != l2.Version.unix {
		t.Errorf("Log.UnmarshalBinary() failed: Version.unix different")
	}
	if l.Version.nano != l2.Version.nano {
		t.Errorf("Log.UnmarshalBinary() failed: Version.nano different")
	}
	if l.Version.srvid != l2.Version.srvid {
		t.Errorf("Log.UnmarshalBinary() failed: Version.srvid different")
	}
	if !bytes.Equal(l.Data, l2.Data) {
		t.Errorf("Log.UnmarshalBinary() failed: Data value different")
	}

	//log.Printf("log data:\n%s", hex.Dump(buf1))
}
