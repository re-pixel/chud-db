package antientropy

import (
	"bytes"
	"fmt"
	"testing"
)

type fakeScanner struct {
	entries []RawEntry
	err     error
}

func (f *fakeScanner) ScanRawRange(start, end string) ([]RawEntry, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.entries, nil
}

func TestComputeMerkleRootEmptyRangeIsDeterministic(t *testing.T) {
	hash1, count1, err := ComputeMerkleRoot(&fakeScanner{}, "a", "z")
	if err != nil {
		t.Fatalf("ComputeMerkleRoot: %v", err)
	}
	if count1 != 0 {
		t.Fatalf("item count = %d, want 0", count1)
	}

	hash2, count2, err := ComputeMerkleRoot(&fakeScanner{}, "a", "z")
	if err != nil {
		t.Fatalf("ComputeMerkleRoot: %v", err)
	}
	if count2 != 0 || !bytes.Equal(hash1, hash2) {
		t.Fatalf("expected identical empty-range root hashes, got %x vs %x", hash1, hash2)
	}
}

func TestComputeMerkleRootSingleKey(t *testing.T) {
	scanner := &fakeScanner{entries: []RawEntry{{Key: "k1", Value: "v1"}}}

	hash, count, err := ComputeMerkleRoot(scanner, "a", "z")
	if err != nil {
		t.Fatalf("ComputeMerkleRoot: %v", err)
	}
	if count != 1 {
		t.Fatalf("item count = %d, want 1", count)
	}
	if len(hash) == 0 {
		t.Fatalf("expected non-empty root hash")
	}

	emptyHash, _, err := ComputeMerkleRoot(&fakeScanner{}, "a", "z")
	if err != nil {
		t.Fatalf("ComputeMerkleRoot: %v", err)
	}
	if bytes.Equal(hash, emptyHash) {
		t.Fatalf("expected single-key root hash to differ from empty-range root hash")
	}
}

func TestComputeMerkleRootIsIndependentOfScanOrder(t *testing.T) {
	forward := &fakeScanner{entries: []RawEntry{
		{Key: "a", Value: "1"},
		{Key: "b", Value: "2"},
		{Key: "c", Value: "3"},
	}}
	reverse := &fakeScanner{entries: []RawEntry{
		{Key: "c", Value: "3"},
		{Key: "a", Value: "1"},
		{Key: "b", Value: "2"},
	}}

	hashForward, countForward, err := ComputeMerkleRoot(forward, "a", "z")
	if err != nil {
		t.Fatalf("ComputeMerkleRoot: %v", err)
	}
	hashReverse, countReverse, err := ComputeMerkleRoot(reverse, "a", "z")
	if err != nil {
		t.Fatalf("ComputeMerkleRoot: %v", err)
	}

	if countForward != countReverse {
		t.Fatalf("item counts differ: %d vs %d", countForward, countReverse)
	}
	if !bytes.Equal(hashForward, hashReverse) {
		t.Fatalf("expected order-independent root hash, got %x vs %x", hashForward, hashReverse)
	}
}

func TestComputeMerkleRootDiffersWhenAnyValueDiffers(t *testing.T) {
	base := &fakeScanner{entries: []RawEntry{
		{Key: "a", Value: "1"},
		{Key: "b", Value: "2"},
	}}
	changed := &fakeScanner{entries: []RawEntry{
		{Key: "a", Value: "1"},
		{Key: "b", Value: "different"},
	}}

	hashBase, _, err := ComputeMerkleRoot(base, "a", "z")
	if err != nil {
		t.Fatalf("ComputeMerkleRoot: %v", err)
	}
	hashChanged, _, err := ComputeMerkleRoot(changed, "a", "z")
	if err != nil {
		t.Fatalf("ComputeMerkleRoot: %v", err)
	}
	if bytes.Equal(hashBase, hashChanged) {
		t.Fatalf("expected differing values to produce differing root hashes")
	}
}

func TestComputeMerkleRootIsSensitiveToKeyValueBoundary(t *testing.T) {
	// "ab" + "c" must not hash the same as "a" + "bc": length-prefixing
	// must prevent the key/value boundary from being ambiguous.
	first := &fakeScanner{entries: []RawEntry{{Key: "ab", Value: "c"}}}
	second := &fakeScanner{entries: []RawEntry{{Key: "a", Value: "bc"}}}

	hashFirst, _, err := ComputeMerkleRoot(first, "a", "z")
	if err != nil {
		t.Fatalf("ComputeMerkleRoot: %v", err)
	}
	hashSecond, _, err := ComputeMerkleRoot(second, "a", "z")
	if err != nil {
		t.Fatalf("ComputeMerkleRoot: %v", err)
	}
	if bytes.Equal(hashFirst, hashSecond) {
		t.Fatalf("expected key/value boundary to be unambiguous")
	}
}

func TestComputeMerkleRootPropagatesScanError(t *testing.T) {
	scanner := &fakeScanner{err: fmt.Errorf("boom")}

	_, _, err := ComputeMerkleRoot(scanner, "a", "z")
	if err == nil {
		t.Fatalf("expected error from failing scanner")
	}
}
