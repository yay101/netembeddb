package netembeddb

import (
	"bytes"
	"testing"
	"time"
)

func TestProtocolEncodeDecodeTime(t *testing.T) {
	now := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)

	encoded, err := EncodeValue(now)
	if err != nil {
		t.Fatalf("EncodeValue failed: %v", err)
	}

	decoded, err := DecodeValue(encoded, "time.Time")
	if err != nil {
		t.Fatalf("DecodeValue failed: %v", err)
	}

	decodedTime := decoded.(time.Time)
	if !decodedTime.Equal(now) {
		t.Errorf("Time mismatch: got %v, want %v", decodedTime, now)
	}
}

func TestProtocolEncodeDecodeSliceString(t *testing.T) {
	slice := []string{"admin", "developer", "user"}

	encoded, err := EncodeValue(slice)
	if err != nil {
		t.Fatalf("EncodeValue failed: %v", err)
	}

	decoded, err := DecodeValue(encoded, "[]string")
	if err != nil {
		t.Fatalf("DecodeValue failed: %v", err)
	}

	decodedSlice, ok := decoded.([]string)
	if !ok {
		t.Fatalf("Expected []string, got %T", decoded)
	}

	if len(decodedSlice) != len(slice) {
		t.Errorf("Length mismatch: got %d, want %d", len(decodedSlice), len(slice))
	}

	for i := range slice {
		if decodedSlice[i] != slice[i] {
			t.Errorf("Slice[%d] mismatch: got %s, want %s", i, decodedSlice[i], slice[i])
		}
	}
}

func TestProtocolEncodeDecodeSliceInt(t *testing.T) {
	slice := []int{10, 20, 30, 40}

	encoded, err := EncodeValue(slice)
	if err != nil {
		t.Fatalf("EncodeValue failed: %v", err)
	}

	decoded, err := DecodeValue(encoded, "[]int")
	if err != nil {
		t.Fatalf("DecodeValue failed: %v", err)
	}

	decodedSlice, ok := decoded.([]int)
	if !ok {
		t.Fatalf("Expected []int, got %T", decoded)
	}

	if len(decodedSlice) != len(slice) {
		t.Errorf("Length mismatch: got %d, want %d", len(decodedSlice), len(slice))
	}

	for i := range slice {
		if decodedSlice[i] != slice[i] {
			t.Errorf("Slice[%d] mismatch: got %d, want %d", i, decodedSlice[i], slice[i])
		}
	}
}

func TestProtocolEncodeDecodeInt(t *testing.T) {
	encoded, err := EncodeValue(int64(42))
	if err != nil {
		t.Fatalf("EncodeValue failed: %v", err)
	}

	decoded, err := DecodeValue(encoded, "int64")
	if err != nil {
		t.Fatalf("DecodeValue failed: %v", err)
	}

	if decoded != int64(42) {
		t.Errorf("Int mismatch: got %v, want 42", decoded)
	}
}

func TestProtocolEncodeDecodeString(t *testing.T) {
	encoded, err := EncodeValue("hello world")
	if err != nil {
		t.Fatalf("EncodeValue failed: %v", err)
	}

	decoded, err := DecodeValue(encoded, "string")
	if err != nil {
		t.Fatalf("DecodeValue failed: %v", err)
	}

	if decoded != "hello world" {
		t.Errorf("String mismatch: got %v, want 'hello world'", decoded)
	}
}

func TestProtocolEncodeDecodeBool(t *testing.T) {
	encoded, err := EncodeValue(true)
	if err != nil {
		t.Fatalf("EncodeValue failed: %v", err)
	}

	decoded, err := DecodeValue(encoded, "bool")
	if err != nil {
		t.Fatalf("DecodeValue failed: %v", err)
	}

	if decoded != true {
		t.Errorf("Bool mismatch: got %v, want true", decoded)
	}
}

func TestProtocolWriteReadTime(t *testing.T) {
	now := time.Date(2024, 6, 15, 14, 30, 0, 0, time.UTC)

	w := NewWriter(nil)
	if err := w.WriteTime(now); err != nil {
		t.Fatalf("WriteTime failed: %v", err)
	}

	r := NewReader(bytes.NewReader(w.Bytes()))
	decoded, err := r.ReadTime()
	if err != nil {
		t.Fatalf("ReadTime failed: %v", err)
	}

	if !decoded.Equal(now) {
		t.Errorf("Time mismatch: got %v, want %v", decoded, now)
	}
}

func TestProtocolEncodeDecodeFloat(t *testing.T) {
	encoded, err := EncodeValue(float64(3.14159))
	if err != nil {
		t.Fatalf("EncodeValue failed: %v", err)
	}

	decoded, err := DecodeValue(encoded, "float64")
	if err != nil {
		t.Fatalf("DecodeValue failed: %v", err)
	}

	if decoded != float64(3.14159) {
		t.Errorf("Float mismatch: got %v, want 3.14159", decoded)
	}
}
