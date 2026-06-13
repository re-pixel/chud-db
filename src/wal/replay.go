package wal

import (
	"io"

	"nosqlEngine/src/wal/record"
)

func (w *WAL) Replay() ([]Entry, error) {
	var entries []Entry
	err := w.ReplayFunc(func(entry Entry) error {
		entries = append(entries, entry)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return entries, nil
}

func (w *WAL) ReplayFunc(fn func(Entry) error) error {
	segments, err := w.store.ListSegments()
	if err != nil {
		return err
	}

	for _, segment := range segments {
		reader, err := w.store.OpenSegmentReader(segment.ID)
		if err != nil {
			return err
		}
		for {
			rec, err := reader.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}
			if err := fn(recordToEntry(rec)); err != nil {
				return err
			}
		}
	}
	return nil
}

func recordToEntry(rec record.Record) Entry {
	return Entry{
		LSN:   rec.LSN,
		Op:    rec.Op,
		Key:   string(rec.Key),
		Value: string(rec.Value),
	}
}
