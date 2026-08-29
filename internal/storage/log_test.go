package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestAppendAssignsSequentialOffsets(t *testing.T) {
	log := openTestLog(t)
	defer closeLog(t, log)

	for i := int64(0); i < 5; i++ {
		offset, err := log.Append([]byte(fmt.Sprintf("record-%04d", i)))
		if err != nil {
			t.Fatalf("Append(%d): unexpected error: %v", i, err)
		}
		if offset != i {
			t.Fatalf("Append(%d) offset: got %d, want %d", i, offset, i)
		}
	}
}

func TestAppendReadRoundTrip(t *testing.T) {
	log := openTestLog(t)
	defer closeLog(t, log)

	payloads := appendFakeRecords(t, log, 1000)

	assertRecords(t, log, 0, 1<<20, payloads[0:])
	assertRecords(t, log, 500, 1<<20, payloads[500:])
	assertRecords(t, log, 999, 1<<20, payloads[999:])
}

func TestReadRespectsMaxBytes(t *testing.T) {
	log := openTestLog(t)
	defer closeLog(t, log)

	appendFakeRecords(t, log, 10)

	records, err := log.Read(0, 1)
	if err != nil {
		t.Fatalf("Read: unexpected error: %v", err)
	}
	if len(records) < 1 {
		t.Fatal("Read with tiny maxBytes returned no records")
	}
	if len(records) >= 10 {
		t.Fatalf("Read with tiny maxBytes returned %d records, want fewer than 10", len(records))
	}
}

func TestReadPastEndReturnsEmpty(t *testing.T) {
	log := openTestLog(t)
	defer closeLog(t, log)

	appendFakeRecords(t, log, 10)

	records, err := log.Read(5000, 1<<20)
	if err != nil {
		t.Fatalf("Read past end: unexpected error: %v", err)
	}
	if len(records) != 0 {
		t.Fatalf("Read past end returned %d records, want 0", len(records))
	}
}

func TestReadEmptyLogReturnsEmpty(t *testing.T) {
	log := openTestLog(t)
	defer closeLog(t, log)

	records, err := log.Read(0, 1<<20)
	if err != nil {
		t.Fatalf("Read empty log: unexpected error: %v", err)
	}
	if len(records) != 0 {
		t.Fatalf("Read empty log returned %d records, want 0", len(records))
	}
}

func TestRecoversAfterReopen(t *testing.T) {
	dir := t.TempDir()
	log, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: unexpected error: %v", err)
	}
	payloads := appendFakeRecords(t, log, 10)
	closeLog(t, log)

	reopened, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: unexpected error: %v", err)
	}
	defer closeLog(t, reopened)

	offset, err := reopened.Append([]byte("record-0010"))
	if err != nil {
		t.Fatalf("Append after reopen: unexpected error: %v", err)
	}
	if offset != 10 {
		t.Fatalf("Append after reopen offset: got %d, want 10", offset)
	}
	payloads = append(payloads, []byte("record-0010"))

	assertRecords(t, reopened, 0, 1<<20, payloads)
}

func TestRecoversFromTornTail(t *testing.T) {
	dir := t.TempDir()
	log, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: unexpected error: %v", err)
	}
	appendFakeRecords(t, log, 3)
	closeLog(t, log)

	// Simulate a crash mid-append: chop bytes off the end of the segment so the
	// last entry's header promises more payload than remains on disk.
	segment := filepath.Join(dir, "00000000000000000000.log")
	info, err := os.Stat(segment)
	if err != nil {
		t.Fatalf("Stat segment: %v", err)
	}
	if err := os.Truncate(segment, info.Size()-3); err != nil {
		t.Fatalf("Truncate segment: %v", err)
	}

	reopened, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen after torn tail: unexpected error: %v", err)
	}
	defer closeLog(t, reopened)

	records, err := reopened.Read(0, 1<<20)
	if err != nil {
		t.Fatalf("Read after torn tail: unexpected error: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("Read after torn tail: got %d intact records, want 2", len(records))
	}

	offset, err := reopened.Append([]byte("record-0002"))
	if err != nil {
		t.Fatalf("Append after torn tail: unexpected error: %v", err)
	}
	if offset != 2 {
		t.Fatalf("Append after torn tail offset: got %d, want 2", offset)
	}

	rewritten, err := reopened.Read(2, 1<<20)
	if err != nil {
		t.Fatalf("Read rewritten record: unexpected error: %v", err)
	}
	if len(rewritten) != 1 || !bytes.Equal(rewritten[0].Payload, []byte("record-0002")) {
		t.Fatalf("Read rewritten record: got %+v, want one record-0002", rewritten)
	}
}

func TestSegmentRolls(t *testing.T) {
	log := openTestLog(t)
	defer closeLog(t, log)

	firstSegmentRecords := appendUntilRolled(t, log)

	if len(log.segments) < 2 {
		t.Fatalf("segment count: got %d, want at least 2", len(log.segments))
	}
	if log.segments[1].baseOffset != int64(firstSegmentRecords) {
		t.Fatalf("second segment base offset: got %d, want %d", log.segments[1].baseOffset, firstSegmentRecords)
	}
}

func TestReadFromLaterSegment(t *testing.T) {
	log := openTestLog(t)
	defer closeLog(t, log)

	appendUntilRolled(t, log)
	target := log.segments[1].baseOffset
	want := []byte(fmt.Sprintf("record-%04d", target))

	records, err := log.Read(target, 1<<20)
	if err != nil {
		t.Fatalf("Read later segment: unexpected error: %v", err)
	}
	if len(records) == 0 {
		t.Fatal("Read later segment returned no records")
	}
	if records[0].Offset != target || !bytes.Equal(records[0].Payload, want) {
		t.Fatalf("first later segment record: got {%d, %q}, want {%d, %q}", records[0].Offset, records[0].Payload, target, want)
	}
}

func TestIndexEntriesWritten(t *testing.T) {
	log := openTestLog(t)
	defer closeLog(t, log)

	appendPayloads(t, log, 20, bytes.Repeat([]byte("x"), 1024))

	seg := log.segments[0]
	if len(seg.index) == 0 {
		t.Fatal("segment has no index entries")
	}
	for _, entry := range seg.index {
		offset, err := readEntryOffsetAt(seg, int64(entry.position))
		if err != nil {
			t.Fatalf("read indexed entry at position %d: unexpected error: %v", entry.position, err)
		}
		wantOffset := seg.baseOffset + int64(entry.relOffset)
		if offset != wantOffset {
			t.Fatalf("index entry %+v resolves to offset %d, want %d", entry, offset, wantOffset)
		}
	}
}

func TestRecoversMultiSegmentLog(t *testing.T) {
	dir := t.TempDir()
	log, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: unexpected error: %v", err)
	}
	appendUntilRolled(t, log)
	end := log.EndOffset()
	laterOffset := log.segments[1].baseOffset
	closeLog(t, log)

	reopened, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: unexpected error: %v", err)
	}
	defer closeLog(t, reopened)

	if reopened.EndOffset() != end {
		t.Fatalf("EndOffset after reopen: got %d, want %d", reopened.EndOffset(), end)
	}
	records, err := reopened.Read(laterOffset, 1<<20)
	if err != nil {
		t.Fatalf("Read after reopen: unexpected error: %v", err)
	}
	if len(records) == 0 || records[0].Offset != laterOffset {
		t.Fatalf("Read after reopen first record: got %+v, want offset %d", records, laterOffset)
	}
}

func TestRebuildsMissingIndex(t *testing.T) {
	dir := t.TempDir()
	log, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: unexpected error: %v", err)
	}
	appendPayloads(t, log, 20, bytes.Repeat([]byte("x"), 1024))
	closeLog(t, log)

	indexPath := filepath.Join(dir, "00000000000000000000.index")
	if err := os.Remove(indexPath); err != nil {
		t.Fatalf("Remove index: unexpected error: %v", err)
	}

	reopened, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen missing index: unexpected error: %v", err)
	}
	defer closeLog(t, reopened)

	if len(reopened.segments[0].index) == 0 {
		t.Fatal("missing index was not rebuilt")
	}
	records, err := reopened.Read(19, 1<<20)
	if err != nil {
		t.Fatalf("Read with rebuilt index: unexpected error: %v", err)
	}
	if len(records) != 1 || records[0].Offset != 19 {
		t.Fatalf("Read with rebuilt index: got %+v, want one record at offset 19", records)
	}
}

func BenchmarkReadFromLateOffset(b *testing.B) {
	log := openTestLog(b)
	defer closeLog(b, log)

	appendPayloads(b, log, 3*1024, bytes.Repeat([]byte("x"), 1024))
	end := log.EndOffset()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		records, err := log.Read(end-1, 1<<20)
		if err != nil {
			b.Fatalf("Read late offset: unexpected error: %v", err)
		}
		if len(records) != 1 || records[0].Offset != end-1 {
			b.Fatalf("Read late offset: got %+v, want offset %d", records, end-1)
		}
	}
}

func openTestLog(t testing.TB) *Log {
	t.Helper()

	log, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: unexpected error: %v", err)
	}
	return log
}

func appendFakeRecords(t testing.TB, log *Log, count int) [][]byte {
	t.Helper()

	payloads := make([][]byte, count)
	for i := 0; i < count; i++ {
		payload := []byte(fmt.Sprintf("record-%04d", i))
		offset, err := log.Append(payload)
		if err != nil {
			t.Fatalf("Append(%d): unexpected error: %v", i, err)
		}
		if offset != int64(i) {
			t.Fatalf("Append(%d) offset: got %d, want %d", i, offset, i)
		}
		payloads[i] = payload
	}
	return payloads
}

func appendPayloads(t testing.TB, log *Log, count int, payload []byte) {
	t.Helper()

	for i := 0; i < count; i++ {
		if _, err := log.Append(payload); err != nil {
			t.Fatalf("Append(%d): unexpected error: %v", i, err)
		}
	}
}

func appendUntilRolled(t testing.TB, log *Log) int {
	t.Helper()

	for i := 0; len(log.segments) < 2; i++ {
		payload := []byte(fmt.Sprintf("record-%04d", i))
		if _, err := log.Append(payload); err != nil {
			t.Fatalf("Append(%d): unexpected error: %v", i, err)
		}
	}
	return int(log.segments[1].baseOffset)
}

func readEntryOffsetAt(seg *segment, position int64) (int64, error) {
	var header [entryHeaderSize]byte
	if _, err := seg.logFile.ReadAt(header[:], position); err != nil {
		return 0, err
	}
	return int64(binary.BigEndian.Uint64(header[0:8])), nil
}

func assertRecords(t *testing.T, log *Log, offset int64, maxBytes int, wantPayloads [][]byte) {
	t.Helper()

	records, err := log.Read(offset, maxBytes)
	if err != nil {
		t.Fatalf("Read(%d): unexpected error: %v", offset, err)
	}
	if len(records) != len(wantPayloads) {
		t.Fatalf("Read(%d) count: got %d, want %d", offset, len(records), len(wantPayloads))
	}
	for i, record := range records {
		wantOffset := offset + int64(i)
		if record.Offset != wantOffset {
			t.Fatalf("record %d offset: got %d, want %d", i, record.Offset, wantOffset)
		}
		if !bytes.Equal(record.Payload, wantPayloads[i]) {
			t.Fatalf("record %d payload: got %q, want %q", i, record.Payload, wantPayloads[i])
		}
	}
}

func closeLog(t testing.TB, log *Log) {
	t.Helper()

	if err := log.Close(); err != nil {
		t.Fatalf("Close: unexpected error: %v", err)
	}
}
