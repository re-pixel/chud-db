package engine

// writeReq is a single write operation submitted to the writer goroutine.
// The caller blocks on done until the writer finishes and sends the result.
type writeReq struct {
	user  string
	key   string
	value string
	done  chan error // buffered cap 1 — writer never blocks on send
}

func (e *Engine) startWriter() {
	e.writerWG.Add(1)
	go e.runWriter()
}

func (e *Engine) runWriter() {
	defer e.writerWG.Done()

	for {
		// Block until the first request arrives (or channel closes).
		req, ok := <-e.writeCh
		if !ok {
			return
		}

		// Drain any additional requests already waiting — they will all share
		// a single WaitDurable call below.
		batch := []writeReq{req}
	drain:
		for {
			select {
			case r, ok := <-e.writeCh:
				if !ok {
					// Channel closed mid-drain; process what we have.
					break drain
				}
				batch = append(batch, r)
			default:
				break drain
			}
		}

		// Apply every request to the WAL buffer and memtable (no fsync yet).
		lsns := make([]uint64, len(batch))
		errs := make([]error, len(batch))
		var maxLSN uint64
		for i, r := range batch {
			lsns[i], errs[i] = e.applyWriteToMem(r.user, r.key, r.value)
			if errs[i] == nil && lsns[i] > maxLSN {
				maxLSN = lsns[i]
			}
		}

		// One fsync for the entire batch.
		var syncErr error
		if maxLSN > 0 {
			syncErr = e.wal.WaitDurable(maxLSN)
		}

		// Signal every caller.
		for i, r := range batch {
			if errs[i] != nil {
				r.done <- errs[i]
			} else {
				r.done <- syncErr
			}
		}
	}
}

func (e *Engine) stopWriter() {
	close(e.writeCh)
	e.writerWG.Wait()
}
