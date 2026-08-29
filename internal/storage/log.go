package storage

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const (
	segmentBaseOffset int64 = 0
	entryHeaderSize         = 12
)

type Record struct {
	Offset  int64
	Payload []byte
}

type Log struct {
	dir        string
	mu         sync.Mutex
	file       *os.File
	writer     *bufio.Writer
	nextOffset int64
}

func Open(dir string) (*Log, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	path := filepath.Join(dir, fmt.Sprintf("%020d.log", segmentBaseOffset))
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}

	nextOffset, validSize, err := recoverLog(file)
	if err == nil {
		// Drop a crash-torn tail so the next O_APPEND write lands right after
		// the last intact entry instead of after the garbage.
		err = file.Truncate(validSize)
	}
	if err != nil {
		if closeErr := file.Close(); closeErr != nil {
			return nil, errors.Join(err, closeErr)
		}
		return nil, err
	}

	return &Log{
		dir:        dir,
		file:       file,
		writer:     bufio.NewWriter(file),
		nextOffset: nextOffset,
	}, nil
}

func (l *Log) Append(payload []byte) (offset int64, err error) {
	return l.AppendWithOffset(payload, nil)
}

func (l *Log) AppendWithOffset(payload []byte, stamp func(offset int64)) (offset int64, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	offset = l.nextOffset
	if stamp != nil {
		stamp(offset)
	}

	var header [entryHeaderSize]byte
	binary.BigEndian.PutUint64(header[0:8], uint64(offset))
	binary.BigEndian.PutUint32(header[8:12], uint32(len(payload)))

	if _, err := l.writer.Write(header[:]); err != nil {
		return 0, err
	}
	if _, err := l.writer.Write(payload); err != nil {
		return 0, err
	}
	if err := l.writer.Flush(); err != nil {
		return 0, err
	}

	l.nextOffset++
	return offset, nil
}

// Read returns records starting at offset, stopping once accumulated payload
// size reaches maxBytes (maxBytes <= 0 means no limit). At least one record is
// returned if any exists at or after offset, even if it alone exceeds maxBytes.
// A crash-torn entry at the tail of the segment is skipped.
func (l *Log) Read(offset int64, maxBytes int) ([]Record, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if err := l.writer.Flush(); err != nil {
		return nil, err
	}

	return readRecords(l.file, offset, maxBytes)
}

func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	var err error
	if l.writer != nil {
		err = l.writer.Flush()
	}
	if l.file != nil {
		err = errors.Join(err, l.file.Close())
		l.file = nil
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

func readRecords(file *os.File, offset int64, maxBytes int) ([]Record, error) {
	var records []Record
	var pos int64
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
