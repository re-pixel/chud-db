package engine

func (e *Engine) startFlusher() {
	e.flusherWG.Add(1)
	go e.runFlusher()
}

func (e *Engine) runFlusher() {
	defer e.flusherWG.Done()
	for {
		im := e.immQueue.Peek()
		if im == nil {
			return
		}
		path := e.ss_parser.FlushMemtable(im.ToRaw(), im.MaxLSN())
		e.registerSSTable(0, path)
		e.wal.PurgeUpTo(im.MaxLSN())
		e.immQueue.PopFront()
		im.MarkFlushed()
		if !e.skipCompaction {
			select {
			case e.compactCh <- struct{}{}:
			default:
			}
		}
	}
}
