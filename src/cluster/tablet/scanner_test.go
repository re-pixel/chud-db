package tablet

import (
	"errors"
	"testing"
)

func TestEstimateSizeSumsRawKeysAndValues(t *testing.T) {
	scanner := &fakeScanner{entries: []RawEntry{
		{Key: "a", Value: "123"},
		{Key: "long", Value: "xy"},
	}}

	size, err := EstimateSize(scanner, "a", "z")
	if err != nil {
		t.Fatalf("estimate size: %v", err)
	}
	if size != 10 {
		t.Fatalf("size = %d, want 10", size)
	}
	if scanner.start != "a" || scanner.end != "z" {
		t.Fatalf("scan = [%q, %q), want [a, z)", scanner.start, scanner.end)
	}
}

func TestEstimateSizePropagatesScanError(t *testing.T) {
	want := errors.New("scan failed")
	scanner := &fakeScanner{err: want}

	if _, err := EstimateSize(scanner, "", ""); !errors.Is(err, want) {
		t.Fatalf("error = %v, want wrapped %v", err, want)
	}
}

func TestPickSplitBoundaryBalancesBytes(t *testing.T) {
	scanner := &fakeScanner{entries: []RawEntry{
		{Key: "d", Value: "x"},
		{Key: "a", Value: "123456"},
		{Key: "c", Value: "x"},
		{Key: "b", Value: "x"},
	}}

	boundary, err := PickSplitBoundary(scanner, "", "")
	if err != nil {
		t.Fatalf("pick split boundary: %v", err)
	}
	if boundary != "b" {
		t.Fatalf("boundary = %q, want b", boundary)
	}
}

func TestPickSplitBoundaryRejectsFewerThanTwoDistinctKeys(t *testing.T) {
	for _, entries := range [][]RawEntry{
		nil,
		{{Key: "a", Value: "1"}},
		{{Key: "a", Value: "1"}, {Key: "a", Value: "2"}},
	} {
		_, err := PickSplitBoundary(&fakeScanner{entries: entries}, "", "")
		if !errors.Is(err, ErrCannotSplit) {
			t.Fatalf("entries = %#v, error = %v, want ErrCannotSplit", entries, err)
		}
	}
}

func TestPickSplitBoundaryPropagatesScanError(t *testing.T) {
	want := errors.New("scan failed")
	_, err := PickSplitBoundary(&fakeScanner{err: want}, "", "")
	if !errors.Is(err, want) {
		t.Fatalf("error = %v, want wrapped %v", err, want)
	}
}

type fakeScanner struct {
	entries []RawEntry
	err     error
	start   string
	end     string
}

func (s *fakeScanner) ScanRawRange(start, end string) ([]RawEntry, error) {
	s.start = start
	s.end = end
	return s.entries, s.err
}
