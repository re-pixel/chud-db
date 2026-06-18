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
		e.ss_parser.FlushMemtable(im.ToRaw())
		e.wal.Purge()
		e.immQueue.PopFront()
		im.MarkFlushed()
		if !e.skipCompaction {
			e.ss_compacter.CheckCompactionConditions(e.block_manager, e.dataRoot)
		}
	}
}
