package engine

import (
	"fmt"
	"sync"
	"testing"
)

func TestConcurrentWrites(t *testing.T) {
	eng, err := NewBenchEngine(t.TempDir())
	if err != nil {
		t.Fatalf("NewBenchEngine: %v", err)
	}
	eng.Start()
	defer eng.Shut()

	const goroutines = 16
	const writesPerGoroutine = 50

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		g := g
		go func() {
			defer wg.Done()
			for i := range writesPerGoroutine {
				key := fmt.Sprintf("g%d-k%d", g, i)
				if err := eng.Write("", key, "value", false); err != nil {
					t.Errorf("Write(%q): %v", key, err)
				}
			}
		}()
	}
	wg.Wait()
}

func TestConcurrentReadsAndWrites(t *testing.T) {
	eng, err := NewBenchEngine(t.TempDir())
	if err != nil {
		t.Fatalf("NewBenchEngine: %v", err)
	}
	eng.Start()
	defer eng.Shut()

	const keys = 20
	for i := range keys {
		if err := eng.Write("", fmt.Sprintf("key%d", i), fmt.Sprintf("val%d", i), false); err != nil {
			t.Fatalf("preload Write: %v", err)
		}
	}

	var wg sync.WaitGroup
	const writers = 8
	const readers = 8
	wg.Add(writers + readers)

	for w := range writers {
		go func() {
			defer wg.Done()
			for i := range 30 {
				key := fmt.Sprintf("w%d-k%d", w, i)
				if err := eng.Write("", key, "v", false); err != nil {
					t.Errorf("Write(%q): %v", key, err)
				}
			}
		}()
	}

	for range readers {
		go func() {
			defer wg.Done()
			for i := range keys * 10 {
				eng.Read("", fmt.Sprintf("key%d", i%keys))
			}
		}()
	}

	wg.Wait()
}

func TestWriteOrdering(t *testing.T) {
	eng, err := NewBenchEngine(t.TempDir())
	if err != nil {
		t.Fatalf("NewBenchEngine: %v", err)
	}
	eng.Start()
	defer eng.Shut()

	// Last write to a key wins regardless of goroutine ordering.
	const goroutines = 8
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := range goroutines {
		go func() {
			defer wg.Done()
			eng.Write("", "shared-key", fmt.Sprintf("val-from-%d", g), false)
		}()
	}
	wg.Wait()

	val, ok, err := eng.Read("", "shared-key")
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !ok || val == "" {
		t.Fatalf("shared-key not found after concurrent writes")
	}
}
