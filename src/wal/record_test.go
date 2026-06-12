package wal

import (
	"bytes"
	"testing"
)

func TestEncodeDecodePutRoundTrip(t *testing.T) {
	key := []byte("user:42")
	value := []byte("alice")
	lsn := uint64(7)

	encoded, err := EncodeRecord(OpPut, key, value, lsn)
	if err != nil {
		t.Fatalf("EncodeRecord failed: %v", err)
	}

	got, err := DecodeRecord(encoded)
	if err != nil {
		t.Fatalf("DecodeRecord failed: %v", err)
	}

	if got.LSN != lsn {
		t.Fatalf("LSN mismatch: got %d want %d", got.LSN, lsn)
	}
	if got.Op != OpPut {
		t.Fatalf("Op mismatch: got %d want %d", got.Op, OpPut)
	}
	if !bytes.Equal(got.Key, key) {
		t.Fatalf("Key mismatch: got %q want %q", got.Key, key)
	}
	if !bytes.Equal(got.Value, value) {
		t.Fatalf("Value mismatch: got %q want %q", got.Value, value)
	}
}

func TestEncodeDecodeDeleteRoundTrip(t *testing.T) {
	key := []byte("user:42")
	lsn := uint64(8)

	encoded, err := EncodeRecord(OpDelete, key, nil, lsn)
	if err != nil {
		t.Fatalf("EncodeRecord failed: %v", err)
	}

	got, err := DecodeRecord(encoded)
	if err != nil {
		t.Fatalf("DecodeRecord failed: %v", err)
	}

	if got.LSN != lsn {
		t.Fatalf("LSN mismatch: got %d want %d", got.LSN, lsn)
	}
	if got.Op != OpDelete {
		t.Fatalf("Op mismatch: got %d want %d", got.Op, OpDelete)
	}
	if !bytes.Equal(got.Key, key) {
		t.Fatalf("Key mismatch: got %q want %q", got.Key, key)
	}
	if len(got.Value) != 0 {
		t.Fatalf("Value should be empty for delete, got %q", got.Value)
	}
}

func TestEncodeInvalidOp(t *testing.T) {
	_, err := EncodeRecord(Op(0x99), []byte("k"), []byte("v"), 1)
	if err == nil {
		t.Fatal("expected error for invalid op")
	}
}

func TestEncodeDeleteWithValue(t *testing.T) {
	_, err := EncodeRecord(OpDelete, []byte("k"), []byte("v"), 1)
	if err == nil {
		t.Fatal("expected error for delete with non-empty value")
	}
}

func TestDecodeTooShort(t *testing.T) {
	_, err := DecodeRecord([]byte{1, 2, 3})
	if err == nil {
		t.Fatal("expected error for short buffer")
	}
}

func TestDecodeCRCMismatch(t *testing.T) {
	encoded, err := EncodeRecord(OpPut, []byte("k"), []byte("v"), 1)
	if err != nil {
		t.Fatalf("EncodeRecord failed: %v", err)
	}
	encoded[0] ^= 0xff

	_, err = DecodeRecord(encoded)
	if err == nil {
		t.Fatal("expected error for crc mismatch")
	}
}

func TestDecodeLengthMismatch(t *testing.T) {
	encoded, err := EncodeRecord(OpPut, []byte("k"), []byte("v"), 1)
	if err != nil {
		t.Fatalf("EncodeRecord failed: %v", err)
	}
	encoded = encoded[:len(encoded)-1]

	_, err = DecodeRecord(encoded)
	if err == nil {
		t.Fatal("expected error for truncated record")
	}
}
