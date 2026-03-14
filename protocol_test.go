package netembeddb

import (
	"bytes"
	"testing"

	"github.com/yay101/embeddb"
)

func TestEncodeVarint(t *testing.T) {
	encoded := embeddb.EncodeVarint(nil, 42)
	if len(encoded) == 0 {
		t.Error("expected encoded bytes")
	}

	val, _, err := embeddb.DecodeVarint(encoded)
	if err != nil {
		t.Fatalf("DecodeVarint failed: %v", err)
	}
	if val != 42 {
		t.Errorf("got %d, want 42", val)
	}
}

func TestEncodeUvarint(t *testing.T) {
	encoded := embeddb.EncodeUvarint(nil, 42)
	if len(encoded) == 0 {
		t.Error("expected encoded bytes")
	}

	val, _, err := embeddb.DecodeUvarint(encoded)
	if err != nil {
		t.Fatalf("DecodeUvarint failed: %v", err)
	}
	if val != 42 {
		t.Errorf("got %d, want 42", val)
	}
}

func TestEncodeString(t *testing.T) {
	encoded := embeddb.EncodeString(nil, "hello")
	if len(encoded) == 0 {
		t.Error("expected encoded bytes")
	}

	val, _, err := embeddb.DecodeString(encoded)
	if err != nil {
		t.Fatalf("DecodeString failed: %v", err)
	}
	if val != "hello" {
		t.Errorf("got %s, want hello", val)
	}
}

func TestEncodeBool(t *testing.T) {
	encoded := embeddb.EncodeBool(nil, true)
	if len(encoded) == 0 {
		t.Error("expected encoded bytes")
	}

	val, _, err := embeddb.DecodeBool(encoded)
	if err != nil {
		t.Fatalf("DecodeBool failed: %v", err)
	}
	if val != true {
		t.Errorf("got %v, want true", val)
	}

	encoded = embeddb.EncodeBool(nil, false)
	val, _, _ = embeddb.DecodeBool(encoded)
	if val != false {
		t.Errorf("got %v, want false", val)
	}
}

func TestEncodeFloat(t *testing.T) {
	encoded := embeddb.EncodeFloat64(nil, 3.14159)
	if len(encoded) == 0 {
		t.Error("expected encoded bytes")
	}

	val, _, err := embeddb.DecodeFloat64(encoded)
	if err != nil {
		t.Fatalf("DecodeFloat64 failed: %v", err)
	}
	if val != 3.14159 {
		t.Errorf("got %f, want 3.14159", val)
	}
}

func TestWriterReader(t *testing.T) {
	w := NewWriter(nil)
	w.WriteByte(0x01)
	w.WriteString("test")
	w.WriteUint64(123)
	w.WriteInt64(-456)
	w.WriteBool(true)

	r := NewReader(bytes.NewReader(w.Bytes()))

	b, err := r.ReadByte()
	if err != nil || b != 0x01 {
		t.Errorf("ReadByte failed: got %v", b)
	}

	s, err := r.ReadString()
	if err != nil || s != "test" {
		t.Errorf("ReadString failed: got %v", s)
	}

	u, err := r.ReadUint64()
	if err != nil || u != 123 {
		t.Errorf("ReadUint64 failed: got %v", u)
	}

	i, err := r.ReadInt64()
	if err != nil || i != -456 {
		t.Errorf("ReadInt64 failed: got %v", i)
	}

	bl, err := r.ReadBool()
	if err != nil || bl != true {
		t.Errorf("ReadBool failed: got %v", bl)
	}
}
