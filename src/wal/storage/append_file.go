package storage

import (
	"encoding/binary"
	"fmt"
	"io"
	"nosqlEngine/src/wal/config"
	"nosqlEngine/src/wal/record"
	"os"
	"sync"
)

const recordLenOffset = 4

type AppendFileStorage struct {
	mu              sync.Mutex
	walDir          string
	segmentSize     int64
	writeBufferSize int
	segmentID       uint64
	activeSegmentPath string
	file            *os.File
	writeBuffer     []byte
	nextLSN         uint64
	appendedLSN     uint64
	durableLSN      uint64
}

func NewAppendStorage() (AppendStorage, error) {
	cfg := config.Get()
	return NewAppendStorageInDir(WalDir(), cfg.WALSegmentSize, cfg.WALWriteBufferSize)
}

func NewAppendStorageInDir(walDir string, segmentSize int64, writeBufferSize int) (AppendStorage, error) {
	store := &AppendFileStorage{
		walDir:          walDir,
		segmentSize:     segmentSize,
		writeBufferSize: writeBufferSize,
		nextLSN:         1,
	}
	if err := store.init(); err != nil {
		return nil, err
	}
	return store, nil
}

func (s *AppendFileStorage) init() error {
	if err := os.MkdirAll(s.walDir, 0755); err != nil {
		return err
	}

	segments, err := s.listSegments()
	if err != nil {
		return err
	}
	if len(segments) == 0 {
		return s.openNewSegment(1)
	}

	last := segments[len(segments)-1]
	maxLSN, err := s.maxLSNFromLastSegment(segments)
	if err != nil {
		return err
	}
	if maxLSN > 0 {
		s.nextLSN = maxLSN + 1
	}
	s.appendedLSN = maxLSN
	s.durableLSN = maxLSN

	info, err := os.Stat(last.Path)
	if err != nil {
		return err
	}
	if info.Size() >= s.segmentSize {
		id, err := s.nextSegmentID()
		if err != nil {
			return err
		}
		return s.openNewSegment(id)
	}
	return s.openSegment(last.ID, last.Path)
}

func (s *AppendFileStorage) maxLSNFromLastSegment(segments []SegmentInfo) (uint64, error) {
	last := segments[len(segments)-1]
	info, err := os.Stat(last.Path)
	if err != nil {
		return 0, err
	}
	scanPath := last.Path
	if info.Size() == 0 && len(segments) > 1 {
		scanPath = segments[len(segments)-2].Path
	}
	return maxLSNInSegment(scanPath)
}

func maxLSNInSegment(path string) (uint64, error) {
	reader, err := openSegmentReader(path)
	if err != nil {
		return 0, err
	}
	defer reader.Close()

	var maxLSN uint64
	for {
		record, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, err
		}
		maxLSN = record.LSN
	}
	return maxLSN, nil
}

func (s *AppendFileStorage) Append(op record.Op, key, value []byte) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	lsn := s.nextLSN
	s.nextLSN++

	encoded, err := record.EncodeRecord(op, key, value, lsn)
	if err != nil {
		return 0, err
	}

	s.writeBuffer = append(s.writeBuffer, encoded...)
	s.appendedLSN = lsn
	if len(s.writeBuffer) >= s.writeBufferSize {
		if err := s.flushLocked(); err != nil {
			return 0, err
		}
	}
	if err := s.rotateIfNeededLocked(); err != nil {
		return 0, err
	}
	return lsn, nil
}

func (s *AppendFileStorage) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.syncLocked()
}

func (s *AppendFileStorage) RotateIfNeeded() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rotateIfNeededLocked()
}

func (s *AppendFileStorage) ActiveSegment() SegmentInfo {
	s.mu.Lock()
	defer s.mu.Unlock()
	return SegmentInfo{ID: s.segmentID, Path: s.activeSegmentPath}
}

func (s *AppendFileStorage) DurableLSN() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.durableLSN
}

func (s *AppendFileStorage) AppendedLSN() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.appendedLSN
}

func (s *AppendFileStorage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closeLocked()
}

func (s *AppendFileStorage) Purge() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.closeLocked(); err != nil {
		return err
	}

	segments, err := s.listSegments()
	if err != nil {
		return err
	}
	for _, segment := range segments {
		if err := os.Remove(segment.Path); err != nil {
			return fmt.Errorf("failed to delete segment %s: %w", segment.Path, err)
		}
	}

	s.nextLSN = 1
	return s.openNewSegment(1)
}

func (s *AppendFileStorage) ListSegments() ([]SegmentInfo, error) {
	return s.listSegments()
}

func (s *AppendFileStorage) OpenSegmentReader(segmentID uint64) (SegmentReader, error) {
	path := s.segmentPath(segmentID)
	if _, err := os.Stat(path); err != nil {
		return nil, fmt.Errorf("segment %d not found: %w", segmentID, err)
	}
	return openSegmentReader(path)
}

func (s *AppendFileStorage) listSegments() ([]SegmentInfo, error) {
	return ListSegmentsInDir(s.walDir)
}

func (s *AppendFileStorage) nextSegmentID() (uint64, error) {
	return NextSegmentIDInDir(s.walDir)
}

func (s *AppendFileStorage) segmentPath(id uint64) string {
	return SegmentPathInDir(s.walDir, id)
}

func (s *AppendFileStorage) flushLocked() error {
	if len(s.writeBuffer) == 0 {
		return nil
	}
	if _, err := s.file.Write(s.writeBuffer); err != nil {
		return err
	}
	s.writeBuffer = s.writeBuffer[:0]
	return nil
}

func (s *AppendFileStorage) syncLocked() error {
	if err := s.flushLocked(); err != nil {
		return err
	}
	if s.file == nil {
		return nil
	}
	if err := s.file.Sync(); err != nil {
		return err
	}
	s.durableLSN = s.appendedLSN
	return nil
}

func (s *AppendFileStorage) closeLocked() error {
	if err := s.syncLocked(); err != nil {
		return err
	}
	if s.file != nil {
		err := s.file.Close()
		s.file = nil
		return err
	}
	return nil
}

func (s *AppendFileStorage) rotateIfNeededLocked() error {
	if s.file == nil {
		return nil
	}
	if err := s.flushLocked(); err != nil {
		return err
	}
	info, err := s.file.Stat()
	if err != nil {
		return err
	}
	if info.Size() < s.segmentSize {
		return nil
	}

	if err := s.file.Sync(); err != nil {
		return err
	}
	s.durableLSN = s.appendedLSN
	if err := s.file.Close(); err != nil {
		return err
	}
	s.file = nil

	id, err := s.nextSegmentID()
	if err != nil {
		return err
	}
	return s.openNewSegment(id)
}

func (s *AppendFileStorage) openNewSegment(id uint64) error {
	path := s.segmentPath(id)
	return s.openSegment(id, path)
}

func (s *AppendFileStorage) openSegment(id uint64, path string) error {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	s.segmentID = id
	s.activeSegmentPath = path
	s.file = file
	s.writeBuffer = s.writeBuffer[:0]
	return nil
}

type fileSegmentReader struct {
	file *os.File
}

func openSegmentReader(path string) (*fileSegmentReader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &fileSegmentReader{file: file}, nil
}

func (r *fileSegmentReader) Next() (record.Record, error) {
	header := make([]byte, 8)
	_, err := io.ReadFull(r.file, header)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return record.Record{}, io.EOF
	}
	if err != nil {
		return record.Record{}, err
	}

	recordLen := int(binary.LittleEndian.Uint32(header[recordLenOffset:8]))
	if recordLen < 8 {
		return record.Record{}, io.EOF
	}

	recordBuf := make([]byte, recordLen)
	copy(recordBuf, header)
	_, err = io.ReadFull(r.file, recordBuf[8:recordLen])
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return record.Record{}, io.EOF
	}
	if err != nil {
		return record.Record{}, err
	}

	rec, err := record.DecodeRecord(recordBuf)
	if err != nil {
		return record.Record{}, io.EOF
	}
	return rec, nil
}

func (r *fileSegmentReader) Close() error {
	return r.file.Close()
}

var _ AppendStorage = (*AppendFileStorage)(nil)
var _ SegmentReader = (*fileSegmentReader)(nil)
