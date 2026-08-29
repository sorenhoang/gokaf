package storage

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const (
	entryHeaderSize = 12
	indexEntrySize  = 8
	// maxSegmentBytes must stay below 2 GiB: indexEntry.position is an int32
	// byte offset within a segment and would overflow past that.
	maxSegmentBytes    = 1 << 20
	indexIntervalBytes = 4 << 10
)

type Record struct {
	Offset  int64
	Payload []byte
}

type indexEntry struct {
	relOffset int32
	position  int32
}

type segment struct {
	baseOffset   int64
	logFile      *os.File
	logWriter    *bufio.Writer
	indexFile    *os.File
	logSize      int64
	lastIndexPos int64
	index        []indexEntry
}

type Log struct {
	dir        string
	mu         sync.Mutex
	segments   []*segment
	nextOffset int64
}

func Open(dir string) (*Log, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	bases, err := segmentBaseOffsets(dir)
	if err != nil {
		return nil, err
	}
	if len(bases) == 0 {
		seg, err := createSegment(dir, 0)
		if err != nil {
			return nil, err
		}
		return &Log{dir: dir, segments: []*segment{seg}}, nil
	}

	segments := make([]*segment, 0, len(bases))
	for _, base := range bases {
		seg, err := openSegment(dir, base)
		if err != nil {
			closeSegments(segments)
			return nil, err
		}
		segments = append(segments, seg)
	}

	nextOffset := recoverNextOffset(segments[len(segments)-1])
	return &Log{dir: dir, segments: segments, nextOffset: nextOffset}, nil
}

func (l *Log) Append(payload []byte) (offset int64, err error) {
	return l.AppendWithOffset(payload, nil)
}

func (l *Log) AppendWithOffset(payload []byte, stamp func(offset int64)) (offset int64, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	offset = l.nextOffset
	entrySize := int64(entryHeaderSize + len(payload))
	active := l.activeSegment()
	if active.logSize > 0 && active.logSize+entrySize > maxSegmentBytes {
		if err := active.flush(); err != nil {
			return 0, err
		}
		active, err = createSegment(l.dir, offset)
		if err != nil {
			return 0, err
		}
		l.segments = append(l.segments, active)
	}

	if stamp != nil {
		stamp(offset)
	}

	entryStartPos := active.logSize
	if err := active.maybeWriteIndex(offset, entryStartPos); err != nil {
		return 0, err
	}

	var header [entryHeaderSize]byte
	binary.BigEndian.PutUint64(header[0:8], uint64(offset))
	binary.BigEndian.PutUint32(header[8:12], uint32(len(payload)))

	if _, err := active.logWriter.Write(header[:]); err != nil {
		return 0, err
	}
	if _, err := active.logWriter.Write(payload); err != nil {
		return 0, err
	}
	if err := active.logWriter.Flush(); err != nil {
		return 0, err
	}

	active.logSize += entrySize
	l.nextOffset++
	return offset, nil
}

// Read returns records starting at offset from a single segment, stopping once
// accumulated payload size reaches maxBytes (maxBytes <= 0 means no limit). At
// least one record is returned if any exists at or after offset, even if it
// alone exceeds maxBytes.
func (l *Log) Read(offset int64, maxBytes int) ([]Record, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Reads only need buffered appends made visible, not an index fsync.
	if err := l.activeSegment().logWriter.Flush(); err != nil {
		return nil, err
	}

	seg := l.segmentForOffset(offset)
	if seg == nil {
		return nil, nil
	}
	startPos := seg.startPosition(offset)
	return readRecords(seg.logFile, startPos, offset, maxBytes)
}

func (l *Log) EndOffset() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.nextOffset
}

func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	err := closeSegments(l.segments)
	l.segments = nil
	return err
}

func (l *Log) activeSegment() *segment {
	return l.segments[len(l.segments)-1]
}

func (l *Log) segmentForOffset(offset int64) *segment {
	if offset < 0 || len(l.segments) == 0 {
		return nil
	}
	i := sort.Search(len(l.segments), func(i int) bool {
		return l.segments[i].baseOffset > offset
	})
	if i == 0 {
		return nil
	}
	return l.segments[i-1]
}

func createSegment(dir string, baseOffset int64) (*segment, error) {
	logFile, err := os.OpenFile(segmentLogPath(dir, baseOffset), os.O_APPEND|os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	indexFile, err := os.OpenFile(segmentIndexPath(dir, baseOffset), os.O_APPEND|os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		if closeErr := logFile.Close(); closeErr != nil {
			return nil, errors.Join(err, closeErr)
		}
		return nil, err
	}
	return &segment{
		baseOffset: baseOffset,
		logFile:    logFile,
		logWriter:  bufio.NewWriter(logFile),
		indexFile:  indexFile,
	}, nil
}

func openSegment(dir string, baseOffset int64) (*segment, error) {
	logFile, err := os.OpenFile(segmentLogPath(dir, baseOffset), os.O_APPEND|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	indexFile, err := os.OpenFile(segmentIndexPath(dir, baseOffset), os.O_APPEND|os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		if closeErr := logFile.Close(); closeErr != nil {
			return nil, errors.Join(err, closeErr)
		}
		return nil, err
	}

	_, validSize, err := recoverLog(logFile)
	if err == nil {
		err = logFile.Truncate(validSize)
	}
	if err != nil {
		closeErr := errors.Join(logFile.Close(), indexFile.Close())
		return nil, errors.Join(err, closeErr)
	}

	index, err := loadIndex(indexFile, validSize)
	if err != nil {
		closeErr := errors.Join(logFile.Close(), indexFile.Close())
		return nil, errors.Join(err, closeErr)
	}
	if index == nil {
		index, err = rebuildIndex(logFile, indexFile, baseOffset, validSize)
		if err != nil {
			closeErr := errors.Join(logFile.Close(), indexFile.Close())
			return nil, errors.Join(err, closeErr)
		}
	}

	return &segment{
		baseOffset:   baseOffset,
		logFile:      logFile,
		logWriter:    bufio.NewWriter(logFile),
		indexFile:    indexFile,
		logSize:      validSize,
		lastIndexPos: lastIndexedPosition(index),
		index:        index,
	}, nil
}

func segmentBaseOffsets(dir string) ([]int64, error) {
	matches, err := filepath.Glob(filepath.Join(dir, "*.log"))
	if err != nil {
		return nil, err
	}
	bases := make([]int64, 0, len(matches))
	for _, match := range matches {
		base, ok := parseSegmentBase(filepath.Base(match))
		if ok {
			bases = append(bases, base)
		}
	}
	slices.Sort(bases)
	return bases, nil
}

func parseSegmentBase(name string) (int64, bool) {
	if filepath.Ext(name) != ".log" {
		return 0, false
	}
	raw := strings.TrimSuffix(name, ".log")
	base, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, false
	}
	return base, true
}

func segmentLogPath(dir string, baseOffset int64) string {
	return filepath.Join(dir, fmt.Sprintf("%020d.log", baseOffset))
}

func segmentIndexPath(dir string, baseOffset int64) string {
	return filepath.Join(dir, fmt.Sprintf("%020d.index", baseOffset))
}

func (s *segment) flush() error {
	var err error
	if s.logWriter != nil {
		err = s.logWriter.Flush()
	}
	if s.indexFile != nil {
		err = errors.Join(err, s.indexFile.Sync())
	}
	return err
}

func (s *segment) maybeWriteIndex(offset int64, entryStartPos int64) error {
	if s.logSize-s.lastIndexPos < indexIntervalBytes {
		return nil
	}

	entry := indexEntry{
		relOffset: int32(offset - s.baseOffset),
		position:  int32(entryStartPos),
	}
	var buf [indexEntrySize]byte
	binary.BigEndian.PutUint32(buf[0:4], uint32(entry.relOffset))
	binary.BigEndian.PutUint32(buf[4:8], uint32(entry.position))
	if _, err := s.indexFile.Write(buf[:]); err != nil {
		return err
	}
	s.index = append(s.index, entry)
	s.lastIndexPos = s.logSize
	return nil
}

func (s *segment) startPosition(offset int64) int64 {
	rel := int32(offset - s.baseOffset)
	j := sort.Search(len(s.index), func(j int) bool {
		return s.index[j].relOffset > rel
	})
	if j == 0 {
		return 0
	}
	return int64(s.index[j-1].position)
}

func closeSegments(segments []*segment) error {
	var err error
	for _, seg := range segments {
		if seg.logWriter != nil {
			err = errors.Join(err, seg.logWriter.Flush())
		}
		if seg.logFile != nil {
			err = errors.Join(err, seg.logFile.Close())
			seg.logFile = nil
		}
		if seg.indexFile != nil {
			err = errors.Join(err, seg.indexFile.Close())
			seg.indexFile = nil
		}
	}
	return err
}

// recoverLog scans entry headers only (no payload reads) and returns the offset
// to assign next plus the byte length of all intact entries. A torn tail — a
// header promising more payload than the file holds — ends the scan.
func recoverLog(file *os.File) (nextOffset int64, validSize int64, err error) {
	info, err := file.Stat()
	if err != nil {
		return 0, 0, err
	}
	size := info.Size()

	var pos int64
	var header [entryHeaderSize]byte
	for pos+entryHeaderSize <= size {
		if _, err := file.ReadAt(header[:], pos); err != nil {
			return 0, 0, err
		}
		entryOffset := int64(binary.BigEndian.Uint64(header[0:8]))
		length := int64(binary.BigEndian.Uint32(header[8:12]))
		if pos+entryHeaderSize+length > size {
			break
		}
		nextOffset = entryOffset + 1
		pos += entryHeaderSize + length
	}
	return nextOffset, pos, nil
}

func recoverNextOffset(seg *segment) int64 {
	if seg.logSize == 0 {
		return seg.baseOffset
	}
	nextOffset, _, err := recoverLog(seg.logFile)
	if err != nil {
		return seg.baseOffset
	}
	return nextOffset
}

func loadIndex(file *os.File, logSize int64) ([]indexEntry, error) {
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if info.Size() == 0 {
		if logSize > 0 {
			return nil, nil
		}
		return []indexEntry{}, nil
	}
	if info.Size()%indexEntrySize != 0 {
		return nil, nil
	}

	index := make([]indexEntry, 0, info.Size()/indexEntrySize)
	var pos int64
	var buf [indexEntrySize]byte
	for pos < info.Size() {
		if _, err := file.ReadAt(buf[:], pos); err != nil {
			return nil, err
		}
		entry := indexEntry{
			relOffset: int32(binary.BigEndian.Uint32(buf[0:4])),
			position:  int32(binary.BigEndian.Uint32(buf[4:8])),
		}
		if int64(entry.position) >= logSize {
			return nil, nil
		}
		index = append(index, entry)
		pos += indexEntrySize
	}
	return index, nil
}

func rebuildIndex(logFile *os.File, indexFile *os.File, baseOffset int64, logSize int64) ([]indexEntry, error) {
	if err := indexFile.Truncate(0); err != nil {
		return nil, err
	}
	if _, err := indexFile.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	var index []indexEntry
	var lastIndexPos int64
	var pos int64
	var header [entryHeaderSize]byte
	for pos+entryHeaderSize <= logSize {
		if _, err := logFile.ReadAt(header[:], pos); err != nil {
			return nil, err
		}
		entryOffset := int64(binary.BigEndian.Uint64(header[0:8]))
		length := int64(binary.BigEndian.Uint32(header[8:12]))
		if pos+entryHeaderSize+length > logSize {
			break
		}
		if pos-lastIndexPos >= indexIntervalBytes {
			entry := indexEntry{relOffset: int32(entryOffset - baseOffset), position: int32(pos)}
			var buf [indexEntrySize]byte
			binary.BigEndian.PutUint32(buf[0:4], uint32(entry.relOffset))
			binary.BigEndian.PutUint32(buf[4:8], uint32(entry.position))
			if _, err := indexFile.Write(buf[:]); err != nil {
				return nil, err
			}
			index = append(index, entry)
			lastIndexPos = pos
		}
		pos += entryHeaderSize + length
	}
	return index, nil
}

func lastIndexedPosition(index []indexEntry) int64 {
	if len(index) == 0 {
		return 0
	}
	return int64(index[len(index)-1].position)
}

func readRecords(file *os.File, startPos int64, offset int64, maxBytes int) ([]Record, error) {
	var records []Record
	pos := startPos
	var payloadBytes int
	var header [entryHeaderSize]byte

	for {
		if _, err := file.ReadAt(header[:], pos); err != nil {
			if errors.Is(err, io.EOF) {
				return records, nil
			}
			return nil, err
		}

		entryOffset := int64(binary.BigEndian.Uint64(header[0:8]))
		length := int64(binary.BigEndian.Uint32(header[8:12]))
		payloadPos := pos + entryHeaderSize
		nextPos := payloadPos + length

		if entryOffset < offset {
			pos = nextPos
			continue
		}

		payload := make([]byte, length)
		if _, err := file.ReadAt(payload, payloadPos); err != nil {
			if errors.Is(err, io.EOF) {
				return records, nil
			}
			return nil, err
		}

		records = append(records, Record{Offset: entryOffset, Payload: payload})
		payloadBytes += len(payload)
		if maxBytes > 0 && payloadBytes >= maxBytes {
			return records, nil
		}

		pos = nextPos
	}
}
