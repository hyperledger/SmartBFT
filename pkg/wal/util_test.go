// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/hyperledger/SmartBFT/pkg/api"
	"github.com/hyperledger/SmartBFT/smartbftprotos"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

func TestWALUtil(t *testing.T) {
	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)

	logger := basicLog.Sugar()

	t.Run("read wal names", func(t *testing.T) {
		testDir := t.TempDir()

		names, err := dirReadWalNames(testDir)
		assert.NoError(t, err)
		assert.Equal(t, 0, len(names))

		make8LogFiles(t, logger, testDir)

		names, err = dirReadWalNames(testDir)
		assert.NoError(t, err)
		nameSet := arrayToSet(names)
		assert.Equal(t, 8, len(nameSet))
		for i := 1; i <= 8; i++ {
			fn := fmt.Sprintf(walFileTemplate, i)
			assert.True(t, nameSet[fn])
		}

		fnOld := filepath.Join(testDir, fmt.Sprintf(walFileTemplate, 1))
		fnNew := filepath.Join(testDir, fmt.Sprintf(walFileTemplate, 1)+".copy")
		err = os.Rename(fnOld, fnNew)
		assert.NoError(t, err)

		names, err = dirReadWalNames(testDir)
		assert.NoError(t, err)
		nameSet = arrayToSet(names)
		assert.Equal(t, 7, len(nameSet))
		for i := 2; i <= 8; i++ {
			fn := fmt.Sprintf(walFileTemplate, i)
			assert.True(t, nameSet[fn])
		}

		fnOld = filepath.Join(testDir, fmt.Sprintf(walFileTemplate, 8))
		fnNew = filepath.Join(testDir, "oops-"+fmt.Sprintf(walFileTemplate, 8))
		err = os.Rename(fnOld, fnNew)
		assert.NoError(t, err)

		names, err = dirReadWalNames(testDir)
		assert.NoError(t, err)
		nameSet = arrayToSet(names)
		assert.Equal(t, 6, len(nameSet))
		for i := 2; i <= 7; i++ {
			fn := fmt.Sprintf(walFileTemplate, i)
			assert.True(t, nameSet[fn])
		}

		names, err = dirReadWalNames(testDir + ".does-not-exist")
		assert.Contains(t, err.Error(), "no such file or directory")
		assert.Nil(t, names)
	})

	t.Run("check wal names", func(t *testing.T) {
		testDir := t.TempDir()

		indexes, err := checkWalFiles(logger, testDir, []string{})
		assert.NoError(t, err)
		assert.Equal(t, 0, len(indexes))

		make8LogFiles(t, logger, testDir)

		// All good
		names, err := dirReadWalNames(testDir)
		assert.NoError(t, err)
		indexes, err = checkWalFiles(logger, testDir, names)
		assert.NoError(t, err)
		assert.Equal(t, 8, len(indexes))
		for i := 1; i <= 8; i++ {
			assert.Equal(t, uint64(i), indexes[i-1])
		}

		// Dir does not exist
		indexes, err = checkWalFiles(logger, testDir+".does-not-exist", names)
		assert.Contains(t, err.Error(), "no such file or directory")
		assert.Nil(t, indexes)

		// Gap in sequence
		fn4 := filepath.Join(testDir, fmt.Sprintf(walFileTemplate, 4))
		err = os.Remove(fn4)
		assert.NoError(t, err)
		names, err = dirReadWalNames(testDir)
		assert.NoError(t, err)
		_, err = checkWalFiles(logger, testDir, names)
		assert.EqualError(t, err, "wal: files not in sequence")

		// File does not exist
		names = append(names, fmt.Sprintf(walFileTemplate, 4))
		_, err = checkWalFiles(logger, testDir, names)
		assert.Contains(t, err.Error(), "no such file or directory")
		assert.Contains(t, err.Error(), "wal: failed to create reader for file:")

		// Error in the last file
		for i := 1; i <= 3; i++ {
			fn := filepath.Join(testDir, fmt.Sprintf(walFileTemplate, i))
			err = os.Remove(fn)
			assert.NoError(t, err)
		}
		f, err := os.Create(filepath.Join(testDir, fmt.Sprintf(walFileTemplate, 9))) // No anchor
		assert.NoError(t, err)
		err = f.Close()
		assert.NoError(t, err)

		names, err = dirReadWalNames(testDir)
		assert.NoError(t, err)
		_, err = checkWalFiles(logger, testDir, names)
		assert.EqualError(t, err, io.ErrUnexpectedEOF.Error())
	})

	t.Run("scan-verify", func(t *testing.T) {
		testDir := t.TempDir()

		make8LogFiles(t, logger, testDir)
		names, err := dirReadWalNames(testDir)
		assert.NoError(t, err)
		err = scanVerifyFiles(logger, testDir, names)
		assert.NoError(t, err)
	})

	t.Run("scan-repair-short", func(t *testing.T) {
		testDir := t.TempDir()

		// first it's fine
		make8LogFiles(t, logger, testDir)
		names, err := dirReadWalNames(testDir)
		assert.NoError(t, err)
		err = scanVerifyFiles(logger, testDir, names)
		assert.NoError(t, err)

		lastFile := filepath.Join(testDir, names[len(names)-1])
		// repair a good file
		err = scanRepairFile(logger, lastFile)
		assert.NoError(t, err)

		// truncate last record in the last file
		f, err := os.OpenFile(lastFile, os.O_RDWR, walFilePermPrivateRW)
		assert.NoError(t, err)
		offset, err := f.Seek(-1, io.SeekEnd)
		assert.NoError(t, err)
		err = f.Truncate(offset)
		assert.NoError(t, err)
		err = f.Close()
		assert.NoError(t, err)
		logger.Debugf(">>> Truncated at: %d", offset)

		err = scanVerifyFiles(logger, testDir, names)
		assert.EqualError(t, err, io.ErrUnexpectedEOF.Error())

		// repair is good
		err = scanRepairFile(logger, lastFile)
		assert.NoError(t, err)
		err = scanVerifyFiles(logger, testDir, names)
		assert.NoError(t, err)
	})

	t.Run("scan-repair-corrupt-record-in-the-middle", func(t *testing.T) {
		testDir := t.TempDir()

		wal, err := Create(logger, testDir, &Options{FileSizeBytes: 1024 * 1024})
		assert.NoError(t, err)
		for range 20 {
			assert.NoError(t, wal.Append(make([]byte, 1024), false))
		}
		assert.NoError(t, wal.Close())

		names, err := dirReadWalNames(testDir)
		assert.NoError(t, err)
		assert.Len(t, names, 1)
		lastFile := filepath.Join(testDir, names[0])

		// flip a CRC bit of the first record, leaving the records written after it intact
		r, err := NewLogRecordReader(logger, lastFile)
		assert.NoError(t, err)
		_, err = r.Read() // the CRC-anchor
		assert.NoError(t, err)
		offset, err := r.logFile.Seek(0, io.SeekCurrent)
		assert.NoError(t, err)
		assert.NoError(t, r.Close())

		f, err := os.OpenFile(lastFile, os.O_RDWR, walFilePermPrivateRW)
		assert.NoError(t, err)
		crcByte := make([]byte, 1)
		_, err = f.ReadAt(crcByte, offset+5) // upper 32 bits of the header hold the CRC
		assert.NoError(t, err)
		crcByte[0] ^= 0x01
		_, err = f.WriteAt(crcByte, offset+5)
		assert.NoError(t, err)
		assert.NoError(t, f.Close())

		sizeBefore, err := os.Stat(lastFile)
		assert.NoError(t, err)

		// the node cannot be repaired: the records after the corrupt one are lost otherwise
		assert.ErrorIs(t, Repair(logger, testDir), ErrNotRepairable)

		sizeAfter, err := os.Stat(lastFile)
		assert.NoError(t, err)
		assert.Equal(t, sizeBefore.Size(), sizeAfter.Size(), "the file must be left untouched")
	})

	t.Run("scan-repair-tail", func(t *testing.T) {
		testDir := t.TempDir()

		// first it's fine
		make8LogFiles(t, logger, testDir)
		names, err := dirReadWalNames(testDir)
		assert.NoError(t, err)
		err = scanVerifyFiles(logger, testDir, names)
		assert.NoError(t, err)

		// add tail to last file
		lastFile := filepath.Join(testDir, names[len(names)-1])
		f, err := os.OpenFile(lastFile, os.O_RDWR, walFilePermPrivateRW)
		assert.NoError(t, err)
		offset, err := f.Seek(0, io.SeekEnd)
		assert.NoError(t, err)
		_, err = f.Write(make([]byte, 64))
		assert.NoError(t, err)
		err = f.Close()
		assert.NoError(t, err)
		logger.Debugf(">>> add tail at: %d", offset)

		err = scanVerifyFiles(logger, testDir, names)
		assert.EqualError(t, err, ErrCRC.Error())

		// repair is good
		err = scanRepairFile(logger, lastFile)
		assert.NoError(t, err)
		err = scanVerifyFiles(logger, testDir, names)
		assert.NoError(t, err)
	})

	t.Run("repair makes a private copy of the last file", func(t *testing.T) {
		testDir := t.TempDir()

		make8LogFiles(t, logger, testDir)

		lastFile := filepath.Join(testDir, fmt.Sprintf(walFileTemplate, 8))
		// pre-existing world readable copy must be tightened
		lastFileCopy := lastFile + ".copy"
		assert.NoError(t, os.WriteFile(lastFileCopy, []byte("junk"), 0o644))

		// corrupt the last file, so that Repair has something to do
		f, err := os.OpenFile(lastFile, os.O_RDWR, walFilePermPrivateRW)
		assert.NoError(t, err)
		_, err = f.Seek(0, io.SeekEnd)
		assert.NoError(t, err)
		_, err = f.Write(make([]byte, 64))
		assert.NoError(t, err)
		assert.NoError(t, f.Close())

		damaged, err := os.ReadFile(lastFile)
		assert.NoError(t, err)

		assert.NoError(t, Repair(logger, testDir))

		fi, err := os.Stat(lastFileCopy)
		assert.NoError(t, err)
		assert.Equal(t, walFilePermPrivateRW, fi.Mode().Perm())

		copied, err := os.ReadFile(lastFileCopy)
		assert.NoError(t, err)
		assert.Equal(t, damaged, copied)

		names, err := dirReadWalNames(testDir)
		assert.NoError(t, err)
		assert.NoError(t, scanVerifyFiles(logger, testDir, names))
	})

	t.Run("scan-repair-bad-anchor", func(t *testing.T) {
		testDir := t.TempDir()

		// first it's fine
		make8LogFiles(t, logger, testDir)
		names, err := dirReadWalNames(testDir)
		assert.NoError(t, err)
		err = scanVerifyFiles(logger, testDir, names)
		assert.NoError(t, err)

		// override crc anchor of last file
		lastFile := filepath.Join(testDir, names[len(names)-1])
		f, err := os.OpenFile(lastFile, os.O_RDWR, walFilePermPrivateRW)
		assert.NoError(t, err)
		_, err = f.Seek(0, io.SeekStart)
		assert.NoError(t, err)
		_, err = f.Write(make([]byte, 64))
		assert.NoError(t, err)
		err = f.Close()
		assert.NoError(t, err)
		logger.Debugf(">>> wrote over crc anchor")

		err = scanVerifyFiles(logger, testDir, names)
		assert.Contains(t, err.Error(), "failed reading CRC-Anchor from log file:")

		// The file holds records, so deleting it would lose them.
		err = scanRepairFile(logger, lastFile)
		assert.ErrorIs(t, err, ErrNotRepairable)
		names, err = dirReadWalNames(testDir)
		assert.NoError(t, err)
		assert.Equal(t, 8, len(names))
	})

	t.Run("readable records follow - crc-anchor in the chain", func(t *testing.T) {
		// encodeRecord writes a record exactly as the WAL writes it: an 8 byte
		// header of length and CRC, then the payload padded to a multiple of 8.
		encodeRecord := func(record *smartbftprotos.LogRecord, crc uint32) []byte {
			buff, err := proto.Marshal(record)
			assert.NoError(t, err)

			length := len(buff)
			_, pad := getPadBytes(length)
			header := make([]byte, recordHeaderSize)
			binary.LittleEndian.PutUint64(header, uint64(length)|uint64(crc)<<32)

			return append(append(header, buff...), pad...)
		}

		payloadOf := func(record *smartbftprotos.LogRecord) []byte {
			buff, err := proto.Marshal(record)
			assert.NoError(t, err)
			_, pad := getPadBytes(len(buff))

			return append(buff, pad...)
		}

		follows := func(data []byte, expected bool) {
			assert.Equal(t, expected, readableRecordsFollow(data, 0))
		}

		first := &smartbftprotos.LogRecord{Type: smartbftprotos.LogRecord_ENTRY, Data: []byte("first")}
		anchor := &smartbftprotos.LogRecord{Type: smartbftprotos.LogRecord_CRC_ANCHOR}
		second := &smartbftprotos.LogRecord{Type: smartbftprotos.LogRecord_ENTRY, Data: []byte("second")}

		// The first record sits at the probe offset: its CRC depends on the
		// corrupt record and is never checked, it only seeds the chain. An
		// anchor follows; its header CRC is the CRC of the previous record, not
		// of its payload, so it reseeds the chain, and the record after it must
		// chain from that seed - exactly as LogRecordReader.Read accepts it.
		anchorCRC := uint32(0xCAFEBABE)
		secondCRC := crc32.Update(anchorCRC, crcTable, payloadOf(second))

		chain := encodeRecord(first, 0xDEADBEEF)
		chain = append(chain, encodeRecord(anchor, anchorCRC)...)
		chain = append(chain, encodeRecord(second, secondCRC)...)
		follows(chain, true)
		assert.True(t, readableLinkPresent(chain, 0))

		// a lone anchor proves nothing: no CRC link was verified
		lone := encodeRecord(first, 0xDEADBEEF)
		lone = append(lone, encodeRecord(anchor, anchorCRC)...)
		follows(lone, false)
		assert.False(t, readableLinkPresent(lone, 0))

		// a record chained to the anchor with a broken CRC does not verify
		broken := encodeRecord(first, 0xDEADBEEF)
		broken = append(broken, encodeRecord(anchor, anchorCRC)...)
		broken = append(broken, encodeRecord(second, secondCRC^0xFFFFFFFF)...)
		follows(broken, false)
		assert.False(t, readableLinkPresent(broken, 0))
	})
}

// TestRepairMidCorruptPlusTornTail is the reproduction from the review of PR #694:
// a corrupt record in the middle of the file, with only the tail torn after it,
// must not be repaired - truncating would drop records written before the crash.
func TestRepairMidCorruptPlusTornTail(t *testing.T) {
	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)

	logger := basicLog.Sugar()
	dir := t.TempDir()

	w, err := Create(logger, dir, &Options{FileSizeBytes: 1024 * 1024})
	assert.NoError(t, err)
	for range 20 {
		assert.NoError(t, w.Append(make([]byte, 1024), false))
	}
	assert.NoError(t, w.Close())

	names, err := dirReadWalNames(dir)
	assert.NoError(t, err)
	last := filepath.Join(dir, names[0])

	r, err := NewLogRecordReader(logger, last)
	assert.NoError(t, err)
	_, err = r.Read() // R0
	assert.NoError(t, err)
	off, err := r.logFile.Seek(0, io.SeekCurrent)
	assert.NoError(t, err)
	assert.NoError(t, r.Close())

	f, err := os.OpenFile(last, os.O_RDWR, walFilePermPrivateRW)
	assert.NoError(t, err)
	b := make([]byte, 1)
	_, err = f.ReadAt(b, off+5)
	assert.NoError(t, err)
	b[0] ^= 1
	_, err = f.WriteAt(b, off+5) // corrupt R1's CRC
	assert.NoError(t, err)
	fi, err := f.Stat()
	assert.NoError(t, err)
	assert.NoError(t, f.Truncate(fi.Size()-100)) // tear R19
	fi, err = f.Stat()
	assert.NoError(t, err)
	damagedSize := fi.Size()
	assert.NoError(t, f.Close())

	assert.ErrorIs(t, Repair(logger, dir), ErrNotRepairable)

	// the corrupt record is in the middle of the file: nothing may be truncated
	fi, err = os.Stat(last)
	assert.NoError(t, err)
	assert.Equal(t, damagedSize, fi.Size())
}

// TestRepairMidCorruptOneFollowingRecord covers a corrupt record followed by a
// single readable record. Truncating at the corrupt record would drop it.
func TestRepairMidCorruptOneFollowingRecord(t *testing.T) {
	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)

	logger := basicLog.Sugar()

	for _, testCase := range []struct {
		name   string
		offset int64
	}{
		{name: "corrupt payload", offset: int64(recordHeaderSize)},
		{name: "corrupt header CRC", offset: 5},
		{name: "corrupt header length", offset: 2},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			dir := t.TempDir()

			w, err := Create(logger, dir, &Options{FileSizeBytes: 1024 * 1024})
			assert.NoError(t, err)
			for range 5 {
				assert.NoError(t, w.Append(make([]byte, 1024), false))
			}
			assert.NoError(t, w.Close())

			names, err := dirReadWalNames(dir)
			assert.NoError(t, err)
			last := filepath.Join(dir, names[0])

			// R3 is the corrupt record; only R4 follows it.
			r, err := NewLogRecordReader(logger, last)
			assert.NoError(t, err)
			for range 3 {
				_, err = r.Read()
				assert.NoError(t, err)
			}
			recordStart, err := r.logFile.Seek(0, io.SeekCurrent)
			assert.NoError(t, err)
			assert.NoError(t, r.Close())

			f, err := os.OpenFile(last, os.O_RDWR, walFilePermPrivateRW)
			assert.NoError(t, err)
			b := make([]byte, 1)
			_, err = f.ReadAt(b, recordStart+testCase.offset)
			assert.NoError(t, err)
			b[0] ^= 1
			_, err = f.WriteAt(b, recordStart+testCase.offset)
			assert.NoError(t, err)
			assert.NoError(t, f.Close())

			fi, err := os.Stat(last)
			assert.NoError(t, err)
			sizeBefore := fi.Size()

			assert.ErrorIs(t, Repair(logger, dir), ErrNotRepairable)

			fi, err = os.Stat(last)
			assert.NoError(t, err)
			assert.Equal(t, sizeBefore, fi.Size(), "the file must be left untouched")
		})
	}
}

func arrayToSet(array []string) map[string]bool {
	set := make(map[string]bool)
	for _, n := range array {
		set[n] = true
	}

	return set
}

// create 8 wal files, 000000000000000000001.wal - 000000000000000000008.wal.
func make8LogFiles(t *testing.T, logger api.Logger, testDir string) {
	wal, err := Create(logger, testDir, &Options{FileSizeBytes: 2048})
	assert.NoError(t, err)
	assert.NotNil(t, wal)

	if wal == nil {
		return
	}

	rec1 := &smartbftprotos.LogRecord{
		Type:       smartbftprotos.LogRecord_ENTRY,
		TruncateTo: false,
		Data:       make([]byte, 512),
	}

	for range 30 {
		err = wal.Append(rec1.Data, rec1.TruncateTo)
		assert.NoError(t, err)
	}

	_ = wal.Close()
}
