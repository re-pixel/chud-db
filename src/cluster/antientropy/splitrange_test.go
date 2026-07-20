package antientropy

import (
	"testing"
)

func assertContiguousCoverage(t *testing.T, start, end string, buckets []Bucket) {
	t.Helper()
	if len(buckets) == 0 {
		t.Fatalf("expected at least one bucket")
	}
	if buckets[0].Start != start {
		t.Fatalf("first bucket start = %q, want %q", buckets[0].Start, start)
	}
	if buckets[len(buckets)-1].End != end {
		t.Fatalf("last bucket end = %q, want %q", buckets[len(buckets)-1].End, end)
	}
	for i := 1; i < len(buckets); i++ {
		if buckets[i-1].End != buckets[i].Start {
			t.Fatalf("gap/overlap between bucket %d (end=%q) and bucket %d (start=%q)", i-1, buckets[i-1].End, i, buckets[i].Start)
		}
	}
}

func TestSplitRangeProducesRequestedBucketCountForWideRange(t *testing.T) {
	buckets, err := SplitRange("a", "z", 4)
	if err != nil {
		t.Fatalf("SplitRange: %v", err)
	}
	if len(buckets) != 4 {
		t.Fatalf("expected 4 buckets, got %d: %+v", len(buckets), buckets)
	}
	assertContiguousCoverage(t, "a", "z", buckets)
}

func TestSplitRangeHandlesUnboundedEnd(t *testing.T) {
	buckets, err := SplitRange("m", "", 3)
	if err != nil {
		t.Fatalf("SplitRange: %v", err)
	}
	if len(buckets) != 3 {
		t.Fatalf("expected 3 buckets, got %d: %+v", len(buckets), buckets)
	}
	assertContiguousCoverage(t, "m", "", buckets)
}

func TestSplitRangeHandlesUnboundedStart(t *testing.T) {
	buckets, err := SplitRange("", "z", 3)
	if err != nil {
		t.Fatalf("SplitRange: %v", err)
	}
	assertContiguousCoverage(t, "", "z", buckets)
}

func TestSplitRangeHandlesFullyUnboundedRange(t *testing.T) {
	buckets, err := SplitRange("", "", 4)
	if err != nil {
		t.Fatalf("SplitRange: %v", err)
	}
	assertContiguousCoverage(t, "", "", buckets)
}

func TestSplitRangeSingleBucketReturnsWholeRangeUnrounded(t *testing.T) {
	buckets, err := SplitRange("start-key", "end-key", 1)
	if err != nil {
		t.Fatalf("SplitRange: %v", err)
	}
	if len(buckets) != 1 || buckets[0].Start != "start-key" || buckets[0].End != "end-key" {
		t.Fatalf("unexpected single bucket: %+v", buckets)
	}
}

func TestSplitRangeHandlesKeysLongerThanBoundaryPrefix(t *testing.T) {
	buckets, err := SplitRange("aaaaaaaaaaaaaaaa", "zzzzzzzzzzzzzzzz", 4)
	if err != nil {
		t.Fatalf("SplitRange: %v", err)
	}
	assertContiguousCoverage(t, "aaaaaaaaaaaaaaaa", "zzzzzzzzzzzzzzzz", buckets)
}

func TestSplitRangeDegeneratesToSingleBucketWhenPrefixesCollide(t *testing.T) {
	// Both keys share the same first 8 bytes ("same-key"), so the
	// interpolation math can't distinguish them - this is the
	// documented byte-prefix simplification.
	buckets, err := SplitRange("same-key-1", "same-key-9", 4)
	if err != nil {
		t.Fatalf("SplitRange: %v", err)
	}
	if len(buckets) != 1 {
		t.Fatalf("expected degenerate single bucket, got %+v", buckets)
	}
	assertContiguousCoverage(t, "same-key-1", "same-key-9", buckets)
}

func TestSplitRangeClampsBucketCountWhenNumericRangeIsSmall(t *testing.T) {
	// Requesting far more buckets than the numeric range spans should
	// still return a valid, contiguous, non-overlapping partition
	// rather than erroring or producing zero-width nonsense.
	buckets, err := SplitRange("\x00", "\x03", 100)
	if err != nil {
		t.Fatalf("SplitRange: %v", err)
	}
	assertContiguousCoverage(t, "\x00", "\x03", buckets)
	if len(buckets) > 100 {
		t.Fatalf("expected at most 100 buckets, got %d", len(buckets))
	}
}

func TestSplitRangeRejectsNonPositiveBucketCount(t *testing.T) {
	if _, err := SplitRange("a", "z", 0); err == nil {
		t.Fatalf("expected error for n=0")
	}
	if _, err := SplitRange("a", "z", -1); err == nil {
		t.Fatalf("expected error for negative n")
	}
}

func TestSplitRangeIsDeterministic(t *testing.T) {
	first, err := SplitRange("alpha", "omega", 6)
	if err != nil {
		t.Fatalf("SplitRange: %v", err)
	}
	second, err := SplitRange("alpha", "omega", 6)
	if err != nil {
		t.Fatalf("SplitRange: %v", err)
	}
	if len(first) != len(second) {
		t.Fatalf("non-deterministic bucket count: %d vs %d", len(first), len(second))
	}
	for i := range first {
		if first[i] != second[i] {
			t.Fatalf("non-deterministic bucket %d: %+v vs %+v", i, first[i], second[i])
		}
	}
}
