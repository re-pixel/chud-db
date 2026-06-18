package engine

import (
	"fmt"
	"sync"
	"testing"
	"time"
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

	const goroutines = 8
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := range goroutines {
		go func() {
			defer wg.Done()
			eng.Write("", "shared-key", fmt.Sprintf("%d", g), false)
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

func TestReadAfterFlush(t *testing.T) {
	eng, err := NewBenchEngine(t.TempDir())
	if err != nil {
		t.Fatalf("NewBenchEngine: %v", err)
	}
	eng.Start()
	defer eng.Shut()

	const n = 10
	keys := make([]string, n)
	vals := make([]string, n)
	for i := range n {
		keys[i] = fmt.Sprintf("key%d", i+1)
		vals[i] = fmt.Sprintf("value%d", i+1)
		if err := eng.Write("", keys[i], vals[i], false); err != nil {
			t.Fatalf("Write[%d]: %v", i, err)
		}
	}

	eng.WaitForPendingFlushes()

	for i := range n {
		got, ok, err := eng.Read("", keys[i])
		if err != nil {
			t.Fatalf("Read(%q): %v", keys[i], err)
		}
		if !ok {
			t.Fatalf("key %q not found after flush", keys[i])
		}
		if got != vals[i] {
			t.Fatalf("key %q: got %q, want %q", keys[i], got, vals[i])
		}
	}
}

func TestBackpressure(t *testing.T) {
	eng, err := NewBenchEngine(t.TempDir())
	if err != nil {
		t.Fatalf("NewBenchEngine: %v", err)
	}
	eng.Start()
	defer eng.Shut()

	const writes = 200
	done := make(chan error, 1)
	go func() {
		for i := range writes {
			if err := eng.Write("", fmt.Sprintf("b%d", i), fmt.Sprintf("v%d", i), false); err != nil {
				done <- fmt.Errorf("Write %d: %w", i, err)
				return
			}
		}
		done <- nil
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("writes did not complete — possible backpressure deadlock")
	}
}
