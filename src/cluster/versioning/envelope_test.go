package versioning

import (
	engineconfig "nosqlEngine/src/config"
	"strings"
	"testing"
	"time"
)

func TestEnvelopeEncodeDecodePutRoundTrip(t *testing.T) {
	now := time.Unix(0, 123)
	original := NewPut(VectorClock{"node-a": 2}, "value", now)

	raw, err := Encode(original)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if !IsEncoded(raw) {
		t.Fatalf("encoded value missing magic prefix: %q", raw)
	}

	decoded, err := Decode(raw)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if decoded.Version != EnvelopeVersion {
		t.Fatalf("version = %d", decoded.Version)
	}
	if decoded.VectorClock["node-a"] != 2 {
		t.Fatalf("vector clock = %#v", decoded.VectorClock)
	}
	if decoded.UpdatedAtUnixNano != now.UnixNano() {
		t.Fatalf("timestamp = %d", decoded.UpdatedAtUnixNano)
	}
	if decoded.Deleted {
		t.Fatalf("put decoded as deleted")
	}
	if decoded.Value != "value" {
		t.Fatalf("value = %q", decoded.Value)
	}
}

func TestEnvelopeDeleteClearsValue(t *testing.T) {
	env := Envelope{
		Version:           EnvelopeVersion,
		VectorClock:       VectorClock{"node-a": 1},
		UpdatedAtUnixNano: time.Unix(0, 456).UnixNano(),
		Deleted:           true,
		Value:             "stale-value",
	}

	raw, err := Encode(env)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	decoded, err := Decode(raw)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !decoded.Deleted {
		t.Fatalf("delete decoded as live value")
	}
	if decoded.Value != "" {
		t.Fatalf("deleted envelope retained value %q", decoded.Value)
	}
}

func TestNewDeleteDoesNotEncodeAsEngineTombstone(t *testing.T) {
	env := NewDelete(VectorClock{"node-a": 1}, time.Unix(0, 789))
	raw, err := Encode(env)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	if raw == engineconfig.GetConfig().Tombstone {
		t.Fatalf("cluster delete encoded as engine tombstone")
	}
}

func TestDecodeRejectsMissingMagicPrefix(t *testing.T) {
	_, err := Decode(`{"version":1}`)
	if err == nil || !strings.Contains(err.Error(), "missing magic prefix") {
		t.Fatalf("expected missing prefix error, got %v", err)
	}
}

func TestDecodeRejectsCorruptJSON(t *testing.T) {
	_, err := Decode(MagicPrefix + `{`)
	if err == nil || !strings.Contains(err.Error(), "json") {
		t.Fatalf("expected json error, got %v", err)
	}
}

func TestDecodeRejectsUnknownVersion(t *testing.T) {
	raw := MagicPrefix + `{"version":2,"vector_clock":{},"updated_at_unix_nano":1,"deleted":false}`
	_, err := Decode(raw)
	if err == nil || !strings.Contains(err.Error(), "unsupported envelope version") {
		t.Fatalf("expected version error, got %v", err)
	}
}

func TestEncodeRejectsMissingTimestamp(t *testing.T) {
	env := Envelope{
		Version:     EnvelopeVersion,
		VectorClock: VectorClock{"node-a": 1},
		Value:       "value",
	}
	_, err := Encode(env)
	if err == nil || !strings.Contains(err.Error(), "updated_at_unix_nano") {
		t.Fatalf("expected timestamp error, got %v", err)
	}
}

func TestEncodeClonesVectorClock(t *testing.T) {
	clock := VectorClock{"node-a": 1}
	env := NewPut(clock, "value", time.Unix(0, 1))
	clock["node-a"] = 99

	raw, err := Encode(env)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	decoded, err := Decode(raw)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if decoded.VectorClock["node-a"] != 1 {
		t.Fatalf("envelope shared input clock: %#v", decoded.VectorClock)
	}
}
