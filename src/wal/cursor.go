package wal

import (
	"io"
	"nosqlEngine/src/wal/storage"
)

// WALCursor streams durable WAL entries in LSN order starting after a given
// LSN. It is safe to call Next concurrently with WAL writes, but Next itself
// is not goroutine-safe — use one cursor per consumer goroutine.
type WALCursor struct {
	wal      *WAL
	afterLSN uint64          // skip entries with LSN <= this
	segIdx   int             // index into segments
	segments []storage.SegmentInfo
	reader   storage.SegmentReader // open reader for segments[segIdx]; nil between segments
	peeked   *Entry          // entry read from the active segment but not yet durable
	notify   chan struct{}   // subscriber channel; signals when durableLSN advances
}

// Next returns the next durable entry in LSN order.
// Returns io.EOF when the cursor has caught up to the current durable point.
// The caller should block on Notify() and then retry.
func (c *WALCursor) Next() (Entry, error) {
	for {
		// If we buffered an entry that wasn't durable yet, check again.
		if c.peeked != nil {
			if c.peeked.LSN <= c.wal.DurableLSN() {
				e := *c.peeked
				c.peeked = nil
				return e, nil
			}
			return Entry{}, io.EOF
		}

		// Open the next segment if we don't have an active reader.
		if c.reader == nil {
			// Refresh the segment list — new segments may have appeared.
			segs, err := c.wal.store.ListSegments()
			if err != nil {
				return Entry{}, err
			}
			c.segments = segs

			if c.segIdx >= len(c.segments) {
				return Entry{}, io.EOF
			}

			r, err := c.wal.store.OpenSegmentReader(c.segments[c.segIdx].ID)
			if err != nil {
				return Entry{}, err
			}
			c.reader = r
		}

		rec, err := c.reader.Next()
		if err == io.EOF {
			c.reader.Close() //nolint:errcheck
			c.reader = nil

			// If the exhausted segment was the active segment, we are caught up.
			active := c.wal.store.ActiveSegment()
			if c.segments[c.segIdx].ID == active.ID {
				return Entry{}, io.EOF
			}
			// Otherwise advance to the next segment and loop.
			c.segIdx++
			continue
		}
		if err != nil {
			return Entry{}, err
		}

		entry := recordToEntry(rec)

		if entry.LSN <= c.afterLSN {
			continue
		}

		// For the active segment, gate on durableLSN so we never return
		// entries that are buffered in memory but not yet fsync'd.
		active := c.wal.store.ActiveSegment()
		if c.segments[c.segIdx].ID == active.ID && entry.LSN > c.wal.DurableLSN() {
			c.peeked = &entry
			return Entry{}, io.EOF
		}

		return entry, nil
	}
}

// Notify returns the channel that receives a signal whenever the WAL's
// durable LSN advances. Use it to avoid a busy-wait after Next returns io.EOF.
//
//	for {
//	    entry, err := cursor.Next()
//	    if err == io.EOF { <-cursor.Notify(); continue }
//	    // handle entry
//	}
func (c *WALCursor) Notify() <-chan struct{} {
	return c.notify
}

// Close releases the cursor's resources. It must be called exactly once.
func (c *WALCursor) Close() error {
	c.wal.Unsubscribe(c.notify)
	if c.reader != nil {
		return c.reader.Close()
	}
	return nil
}
