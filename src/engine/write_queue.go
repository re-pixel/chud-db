package engine

// writeReq is a single write operation submitted to the writer goroutine.
// The caller blocks on done until the writer finishes and sends the result.
type writeReq struct {
	user    string
	key     string
	value   string
	fromWal bool
	done    chan error // buffered cap 1 — writer never blocks on send
}

func (e *Engine) startWriter() {
	e.writerWG.Add(1)
	go func() {
		defer e.writerWG.Done()
		for req := range e.writeCh {
			req.done <- e.applyWrite(req.user, req.key, req.value, req.fromWal)
		}
	}()
}

func (e *Engine) stopWriter() {
	close(e.writeCh)
	e.writerWG.Wait()
}
