package node

import (
	"errors"
	"strings"
	"testing"
	"time"

	"nosqlEngine/src/cluster/versioning"
)

func TestNodeStorePutUsesSyncWrite(t *testing.T) {
	engine := newFakeEngine()
	store := NewNodeStore(engine, "cluster-user")
	env := versioning.NewPut(versioning.VectorClock{"node-a": 1}, "value", time.Unix(0, 1))

	if err := store.Put("key", env, true); err != nil {
		t.Fatalf("put: %v", err)
	}

	if len(engine.writeCalls) != 1 {
		t.Fatalf("write calls = %d", len(engine.writeCalls))
	}
	call := engine.writeCalls[0]
	if call.user != "cluster-user" || call.key != "key" || call.fromWal {
		t.Fatalf("unexpected write call: %#v", call)
	}
	decoded, err := versioning.Decode(call.value)
	if err != nil {
		t.Fatalf("decode stored value: %v", err)
	}
	if decoded.Value != "value" {
		t.Fatalf("stored value = %q", decoded.Value)
	}
	if len(engine.asyncCalls) != 0 {
		t.Fatalf("unexpected async calls: %#v", engine.asyncCalls)
	}
}

func TestNodeStorePutUsesAsyncWrite(t *testing.T) {
	engine := newFakeEngine()
	store := NewNodeStore(engine, "")
	env := versioning.NewPut(versioning.VectorClock{"node-a": 1}, "value", time.Unix(0, 1))

	if err := store.Put("key", env, false); err != nil {
		t.Fatalf("put: %v", err)
	}

	if len(engine.asyncCalls) != 1 {
		t.Fatalf("async calls = %d", len(engine.asyncCalls))
	}
	call := engine.asyncCalls[0]
	if call.user != DefaultStoreUser || call.key != "key" {
		t.Fatalf("unexpected async call: %#v", call)
	}
	if len(engine.writeCalls) != 0 {
		t.Fatalf("unexpected sync calls: %#v", engine.writeCalls)
	}
}

func TestNodeStoreGetDecodesEnvelope(t *testing.T) {
	engine := newFakeEngine()
	store := NewNodeStore(engine, "cluster-user")
	env := versioning.NewPut(versioning.VectorClock{"node-a": 2}, "value", time.Unix(0, 2))
	raw, err := versioning.Encode(env)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	engine.values["key"] = raw

	got, ok, err := store.Get("key")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !ok {
		t.Fatalf("expected key to exist")
	}
	if got.Value != "value" || got.VectorClock["node-a"] != 2 {
		t.Fatalf("decoded envelope = %#v", got)
	}
}

func TestNodeStoreGetMissingKey(t *testing.T) {
	store := NewNodeStore(newFakeEngine(), "cluster-user")

	_, ok, err := store.Get("missing")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if ok {
		t.Fatalf("missing key returned ok")
	}
}

func TestNodeStoreGetReturnsDecodeError(t *testing.T) {
	engine := newFakeEngine()
	engine.values["key"] = "not-an-envelope"
	store := NewNodeStore(engine, "cluster-user")

	_, _, err := store.Get("key")
	if err == nil || !strings.Contains(err.Error(), "decode") {
		t.Fatalf("expected decode error, got %v", err)
	}
}

func TestNodeStoreDeleteStoresDeletedEnvelope(t *testing.T) {
	engine := newFakeEngine()
	store := NewNodeStore(engine, "cluster-user")
	env := versioning.NewDelete(versioning.VectorClock{"node-a": 3}, time.Unix(0, 3))

	if err := store.Delete("key", env, true); err != nil {
		t.Fatalf("delete: %v", err)
	}

	decoded, err := versioning.Decode(engine.writeCalls[0].value)
	if err != nil {
		t.Fatalf("decode stored delete: %v", err)
	}
	if !decoded.Deleted {
		t.Fatalf("stored delete as live envelope: %#v", decoded)
	}
}

func TestNodeStoreRejectsWrongEnvelopeOperation(t *testing.T) {
	store := NewNodeStore(newFakeEngine(), "cluster-user")
	put := versioning.NewPut(versioning.VectorClock{"node-a": 1}, "value", time.Unix(0, 1))
	del := versioning.NewDelete(versioning.VectorClock{"node-a": 2}, time.Unix(0, 2))

	if err := store.Put("key", del, true); err == nil {
		t.Fatalf("expected put to reject deleted envelope")
	}
	if err := store.Delete("key", put, true); err == nil {
		t.Fatalf("expected delete to reject live envelope")
	}
}

func TestNodeStoreScanRangeDecodesRows(t *testing.T) {
	engine := newFakeEngine()
	store := NewNodeStore(engine, "cluster-user")
	first := versioning.NewPut(versioning.VectorClock{"node-a": 1}, "a", time.Unix(0, 1))
	second := versioning.NewDelete(versioning.VectorClock{"node-a": 2}, time.Unix(0, 2))
	rawFirst, err := versioning.Encode(first)
	if err != nil {
		t.Fatalf("encode first: %v", err)
	}
	rawSecond, err := versioning.Encode(second)
	if err != nil {
		t.Fatalf("encode second: %v", err)
	}
	engine.rangeRows = [][]string{{"a", rawFirst}, {"b", rawSecond}}

	rows, err := store.ScanRange("a", "z", 1, 10)
	if err != nil {
		t.Fatalf("scan range: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("rows = %#v", rows)
	}
	if rows[0].Key != "a" || rows[0].Envelope.Value != "a" {
		t.Fatalf("first row = %#v", rows[0])
	}
	if rows[1].Key != "b" || !rows[1].Envelope.Deleted {
		t.Fatalf("second row = %#v", rows[1])
	}
	if engine.lastRange.start != "a" || engine.lastRange.end != "z" || engine.lastRange.pageNum != 1 || engine.lastRange.pageSize != 10 {
		t.Fatalf("range args = %#v", engine.lastRange)
	}
}

func TestNodeStoreScanRangeReturnsDecodeError(t *testing.T) {
	engine := newFakeEngine()
	engine.rangeRows = [][]string{{"key", "not-an-envelope"}}
	store := NewNodeStore(engine, "cluster-user")

	_, err := store.ScanRange("a", "z", 1, 10)
	if err == nil || !strings.Contains(err.Error(), "decode") {
		t.Fatalf("expected decode error, got %v", err)
	}
}

func TestNodeStoreReturnsEngineErrors(t *testing.T) {
	engine := newFakeEngine()
	engine.writeErr = errors.New("write failed")
	store := NewNodeStore(engine, "cluster-user")
	env := versioning.NewPut(versioning.VectorClock{"node-a": 1}, "value", time.Unix(0, 1))

	err := store.Put("key", env, true)
	if !errors.Is(err, engine.writeErr) {
		t.Fatalf("expected write error, got %v", err)
	}
}

type fakeEngine struct {
	values     map[string]string
	writeCalls []writeCall
	asyncCalls []writeCall
	rangeRows  [][]string
	lastRange  rangeCall
	writeErr   error
	asyncErr   error
	readErr    error
	rangeErr   error
}

type writeCall struct {
	user    string
	key     string
	value   string
	fromWal bool
}

type rangeCall struct {
	user     string
	start    string
	end      string
	pageNum  int
	pageSize int
}

func newFakeEngine() *fakeEngine {
	return &fakeEngine{values: make(map[string]string)}
}

func (e *fakeEngine) Write(user, key, value string, fromWal bool) error {
	e.writeCalls = append(e.writeCalls, writeCall{user: user, key: key, value: value, fromWal: fromWal})
	if e.writeErr != nil {
		return e.writeErr
	}
	e.values[key] = value
	return nil
}

func (e *fakeEngine) WriteAsync(user, key, value string) error {
	e.asyncCalls = append(e.asyncCalls, writeCall{user: user, key: key, value: value})
	if e.asyncErr != nil {
		return e.asyncErr
	}
	e.values[key] = value
	return nil
}

func (e *fakeEngine) Read(user, key string) (string, bool, error) {
	if e.readErr != nil {
		return "", false, e.readErr
	}
	value, ok := e.values[key]
	return value, ok, nil
}

func (e *fakeEngine) RangeScan(user, start, end string, pageNum, pageSize int) ([][]string, error) {
	e.lastRange = rangeCall{user: user, start: start, end: end, pageNum: pageNum, pageSize: pageSize}
	if e.rangeErr != nil {
		return nil, e.rangeErr
	}
	return e.rangeRows, nil
}
