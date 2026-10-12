// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package wal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/hyperledger/SmartBFT/pkg/api"
	"github.com/hyperledger/SmartBFT/smartbftprotos"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

func TestWriteAheadLogFile_Create(t *testing.T) {
	testDir := t.TempDir()

	basicLog, err := zap.NewDevelopment()
	require.NoError(t, err)

	logger := basicLog.Sugar()

	t.Run("Good", func(t *testing.T) {
		dirPath := filepath.Join(testDir, "good")

		wal, err := Create(logger, dirPath, nil)
		require.NoError(t, err)
		require.NotNil(t, wal)

		if wal == nil {
			return
		}

		dataItems, err := wal.ReadAll()
		require.EqualError(t, err, ErrWriteOnly.Error())
		require.Nil(t, dataItems)

		crc := wal.CRC()
		err = wal.Close()
		require.NoError(t, err)

		expectedFileName := fmt.Sprintf(walFileTemplate, 1)
		verifyFirstFileCreation(t, logger, dirPath, expectedFileName, crc)
	})

	t.Run("Good - with options", func(t *testing.T) {
		dirPath := filepath.Join(testDir, "good-w-options")
		err := os.MkdirAll(dirPath, walDirPermPrivateRWX)
		require.NoError(t, err)

		wal, err := Create(logger, dirPath, &Options{FileSizeBytes: 100 * 1024, BufferSizeBytes: 1024})
		require.NoError(t, err)
		require.NotNil(t, wal)

		var crc uint32
		if wal != nil {
			err = wal.Close()
			require.NoError(t, err)
			crc = wal.CRC()
		}

		expectedFileName := fmt.Sprintf(walFileTemplate, 1)
		verifyFirstFileCreation(t, logger, dirPath, expectedFileName, crc)
	})

	t.Run("Bad - already exist", func(t *testing.T) {
		dirPath := filepath.Join(testDir, "bad-exist")
		err := os.MkdirAll(dirPath, walDirPermPrivateRWX)
		require.NoError(t, err)
		f, err := os.Create(filepath.Join(dirPath, "0000000000000008.wal"))
		require.NoError(t, err)
		require.NotNil(t, f)
		err = f.Close()
		require.NoError(t, err)

		wal, err := Create(logger, dirPath, nil)
		require.Error(t, err)
		require.Equal(t, err, ErrWALAlreadyExists)
		require.Nil(t, wal)
	})
}

func TestWriteAheadLogFile_Open(t *testing.T) {
	testDir := t.TempDir()

	basicLog, err := zap.NewDevelopment()
	require.NoError(t, err)

	logger := basicLog.Sugar()

	t.Run("Good", func(t *testing.T) {
		dirPath := filepath.Join(testDir, "good")
		err := os.MkdirAll(dirPath, walDirPermPrivateRWX)
		require.NoError(t, err)

		wal, err := Create(logger, dirPath, &Options{FileSizeBytes: 4 * 1024, BufferSizeBytes: 2048})
		require.NoError(t, err)
		require.NotNil(t, wal)
		if wal == nil {
			return
		}

		const NumBytes = 1024
		const NumRec = 20
		for m := range NumRec {
			data1 := make([]byte, NumBytes)
			for n := range NumBytes {
				data1[n] = byte(n % (m + 1))
			}
			err = wal.Append(data1, false)
			require.NoError(t, err)
		}

		err = wal.Close()
		require.NoError(t, err)

		wal, err = Open(logger, dirPath, nil)
		require.NoError(t, err)
		require.NotNil(t, wal)
		if wal == nil {
			return
		}

		err = wal.Append([]byte{1, 2, 3, 4}, false)
		require.EqualError(t, err, "wal: in READ mode")
		err = wal.TruncateTo()
		require.EqualError(t, err, "wal: in READ mode")

		err = wal.Close()
		require.NoError(t, err)
	})

	t.Run("Bad - does not exist", func(t *testing.T) {
		dirPath := filepath.Join(testDir, "bad-not-exist")

		wal, err := Open(logger, dirPath, nil)
		require.Error(t, err)
		if err != nil {
			require.Contains(t, err.Error(), "no such file or directory")
		}
		require.Nil(t, wal)
	})

	t.Run("Bad - no files", func(t *testing.T) {
		dirPath := filepath.Join(testDir, "bad-no-files")
		err := os.MkdirAll(dirPath, walDirPermPrivateRWX)
		require.NoError(t, err)

		wal, err := Open(logger, dirPath, nil)
		require.Error(t, err)
		if err != nil {
			require.Contains(t, err.Error(), "file does not exist")
		}
		require.Nil(t, wal)
	})
}

func TestWriteAheadLogFile_Close(t *testing.T) {
	testDir := t.TempDir()

	basicLog, err := zap.NewDevelopment()
	require.NoError(t, err)

	logger := basicLog.Sugar()

	t.Run("Idempotent", func(t *testing.T) {
		dirPath := filepath.Join(testDir, "idempotent")

		wal, err := Create(logger, dirPath, nil)
		require.NoError(t, err)
		require.NotNil(t, wal)

		if wal == nil {
			return
		}

		crc := wal.CRC()
		err = wal.Close()
		require.NoError(t, err)
		err = wal.Close()
		require.NoError(t, err)
		require.Equal(t, crc, wal.CRC())

		expectedFileName := fmt.Sprintf(walFileTemplate, 1)
		verifyFirstFileCreation(t, logger, dirPath, expectedFileName, crc)
	})

	t.Run("Cannot Append", func(t *testing.T) {
		dirPath := filepath.Join(testDir, "cannot-append")

		wal, err := Create(logger, dirPath, nil)
		require.NoError(t, err)
		require.NotNil(t, wal)

		if wal == nil {
			return
		}

		crc := wal.CRC()
		err = wal.Close()
		require.NoError(t, err)
		err = wal.Append([]byte{1, 2, 3, 4}, true)
		require.EqualError(t, err, os.ErrClosed.Error())

		expectedFileName := fmt.Sprintf(walFileTemplate, 1)
		verifyFirstFileCreation(t, logger, dirPath, expectedFileName, crc)
	})
}

func TestWriteAheadLogFile_Append(t *testing.T) {
	testDir := t.TempDir()

	basicLog, err := zap.NewDevelopment()
	require.NoError(t, err)

	logger := basicLog.Sugar()

	t.Run("Good", func(t *testing.T) {
		dirPath := filepath.Join(testDir, "good")

		wal, err := Create(logger, dirPath, nil)
		require.NoError(t, err)
		require.NotNil(t, wal)
		if wal == nil {
			return
		}

		rec1 := &smartbftprotos.LogRecord{
			Type:       smartbftprotos.LogRecord_ENTRY,
			TruncateTo: true,
			Data:       []byte{1, 2, 3, 4},
		}
		err = wal.Append(rec1.GetData(), rec1.GetTruncateTo())
		require.NoError(t, err)

		rec2 := &smartbftprotos.LogRecord{
			Type:       smartbftprotos.LogRecord_ENTRY,
			TruncateTo: false,
			Data:       []byte{5, 6, 7, 8, 9, 10, 11, 12},
		}
		err = wal.Append(rec2.GetData(), rec2.GetTruncateTo())
		require.NoError(t, err)

		err = wal.Append(nil, false)
		require.Error(t, err)

		err = wal.Append([]byte{}, false)
		require.Error(t, err)

		crc := wal.CRC()
		err = wal.Close()
		require.NoError(t, err)

		expectedFileName := fmt.Sprintf(walFileTemplate, 1)
		verifyAppend(t, logger, dirPath, expectedFileName, crc, rec1, rec2)
	})

	t.Run("File switch", func(t *testing.T) {
		dirPath := filepath.Join(testDir, "switch")

		wal, err := Create(logger, dirPath, &Options{FileSizeBytes: 10 * 1024, BufferSizeBytes: 2048})
		require.NoError(t, err)
		require.NotNil(t, wal)
		if wal == nil {
			return
		}

		const NumBytes = 1024
		const NumRec = 20
		records := make([]*smartbftprotos.LogRecord, NumRec)
		var crc1, crc2 uint32
		for m := range NumRec {
			data1 := make([]byte, NumBytes)
			for n := range NumBytes {
				data1[n] = byte(n % (m + 1))
			}

			rec := &smartbftprotos.LogRecord{
				Type:       smartbftprotos.LogRecord_ENTRY,
				TruncateTo: false,
				Data:       data1,
			}
			if m == 0 {
				rec.TruncateTo = true
			}

			records[m] = rec

			err = wal.Append(rec.GetData(), rec.GetTruncateTo())
			require.NoError(t, err)

			if m == 9 {
				crc1 = wal.CRC()
			}
		}
		crc2 = wal.CRC()

		err = wal.Close()
		require.NoError(t, err)

		expectedFileName := fmt.Sprintf(walFileTemplate, 1)
		verifyAppend(t, logger, dirPath, expectedFileName, crc1, records[:10]...)
		expectedFileName = fmt.Sprintf(walFileTemplate, 2)
		verifyAppend(t, logger, dirPath, expectedFileName, crc2, records[10:]...)
	})

	t.Run("File recycle", func(t *testing.T) {
		dirPath := filepath.Join(testDir, "recycle")

		wal, err := Create(logger, dirPath, &Options{FileSizeBytes: 10 * 1024, BufferSizeBytes: 2048})
		require.NoError(t, err)
		require.NotNil(t, wal)
		if wal == nil {
			return
		}

		const NumBytes = 1024
		const NumRec = 41
		records := make([]*smartbftprotos.LogRecord, NumRec)
		var crc1, crc2 uint32
		for m := range NumRec {
			data1 := make([]byte, NumBytes)
			for n := range NumBytes {
				data1[n] = byte(n % (m + 1))
			}

			rec := &smartbftprotos.LogRecord{
				Type:       smartbftprotos.LogRecord_ENTRY,
				TruncateTo: false,
				Data:       data1,
			}
			if m%3 == 0 {
				rec.TruncateTo = true
			}

			records[m] = rec

			err = wal.Append(rec.GetData(), rec.GetTruncateTo())
			require.NoError(t, err)

			names, err := dirReadWalNames(dirPath)
			require.NoError(t, err)
			require.LessOrEqual(t, len(names), 2)

			if m == 39 {
				crc1 = wal.CRC()
			}
		}
		crc2 = wal.CRC()

		err = wal.Close()
		require.NoError(t, err)

		expectedFileName := fmt.Sprintf(walFileTemplate, 4)
		verifyAppend(t, logger, dirPath, expectedFileName, crc1, records[30:40]...)
		expectedFileName = fmt.Sprintf(walFileTemplate, 5)
		verifyAppend(t, logger, dirPath, expectedFileName, crc2, records[40:]...)
	})

	t.Run("TruncateTo", func(t *testing.T) {
		dirPath := filepath.Join(testDir, "TruncateTo")

		wal, err := Create(logger, dirPath, nil)
		require.NoError(t, err)
		require.NotNil(t, wal)
		if wal == nil {
			return
		}

		rec1 := &smartbftprotos.LogRecord{
			Type:       smartbftprotos.LogRecord_ENTRY,
			TruncateTo: false,
			Data:       []byte{1, 2, 3, 4},
		}
		err = wal.Append(rec1.GetData(), rec1.GetTruncateTo())
		require.NoError(t, err)

		rec2 := &smartbftprotos.LogRecord{
			Type:       smartbftprotos.LogRecord_CONTROL,
			TruncateTo: true,
		}
		err = wal.TruncateTo()
		require.NoError(t, err)

		rec3 := &smartbftprotos.LogRecord{
			Type:       smartbftprotos.LogRecord_ENTRY,
			TruncateTo: false,
			Data:       []byte{5, 6, 7, 8, 9, 10, 11, 12},
		}
		err = wal.Append(rec3.GetData(), rec3.GetTruncateTo())
		require.NoError(t, err)

		err = wal.Append(nil, false)
		require.Error(t, err)

		err = wal.Append([]byte{}, false)
		require.Error(t, err)

		crc := wal.CRC()
		err = wal.Close()
		require.NoError(t, err)

		expectedFileName := fmt.Sprintf(walFileTemplate, 1)
		verifyAppend(t, logger, dirPath, expectedFileName, crc, rec1, rec2, rec3)
	})
}

func TestWriteAheadLogFile_ReadAll(t *testing.T) {
	testDir := t.TempDir()

	basicLog, err := zap.NewDevelopment()
	require.NoError(t, err)

	logger := basicLog.Sugar()

	t.Run("Good - one empty file", func(t *testing.T) {
		dirPath := filepath.Join(testDir, "good-1-empty")

		wal, err := Create(logger, dirPath, nil)
		require.NoError(t, err)
		require.NotNil(t, wal)
		if wal == nil {
			return
		}

		err = wal.Close()
		require.NoError(t, err)

		wal, err = Open(logger, dirPath, nil)
		require.NoError(t, err)
		require.NotNil(t, wal)

		dataItems, err := wal.ReadAll()
		require.NoError(t, err)
		require.NotNil(t, dataItems)
		require.Empty(t, dataItems)

		dataItems, err = wal.ReadAll()
		require.EqualError(t, err, ErrWriteOnly.Error())
		require.Nil(t, dataItems)
	})

	t.Run("Good - 1 file", func(t *testing.T) {
		dirPath := filepath.Join(testDir, "good-1-file")

		wal, err := Create(logger, dirPath, nil)
		require.NoError(t, err)
		require.NotNil(t, wal)
		if wal == nil {
			return
		}

		data1 := []byte{1, 2, 3, 4}
		data2 := []byte{5, 6, 7, 8}
		err = wal.Append(data1, false)
		require.NoError(t, err)
		err = wal.Append(data2, false)
		require.NoError(t, err)

		err = wal.Close()
		require.NoError(t, err)

		wal, err = Open(logger, dirPath, nil)
		require.NoError(t, err)
		require.NotNil(t, wal)

		dataItems, err := wal.ReadAll()
		require.NoError(t, err)
		require.NotNil(t, dataItems)
		require.Len(t, dataItems, 2)
		require.True(t, bytes.Equal(data1, dataItems[0]))
		require.True(t, bytes.Equal(data2, dataItems[1]))
	})

	t.Run("Good - many files", func(t *testing.T) {
		dirPath := filepath.Join(testDir, "good-many")
		err := os.MkdirAll(dirPath, walDirPermPrivateRWX)
		require.NoError(t, err)

		wal, err := Create(logger, dirPath, &Options{FileSizeBytes: 10 * 1024, BufferSizeBytes: 2048})
		require.NoError(t, err)
		require.NotNil(t, wal)
		if wal == nil {
			return
		}

		const NumBytes = 1024
		const NumRec = 100
		data1 := make([]byte, NumBytes)
		for m := range NumRec {
			for n := range NumBytes {
				data1[n] = byte(m)
			}
			err = wal.Append(data1, false)
			require.NoError(t, err)
		}

		err = wal.Close()
		require.NoError(t, err)

		logger.Infof(">>> Open #1")

		wal, err = Open(logger, dirPath, &Options{FileSizeBytes: 10 * 1024, BufferSizeBytes: 2048})
		require.NoError(t, err)
		require.NotNil(t, wal)
		if wal == nil {
			return
		}

		logger.Infof(">>> ReadAll #1")

		dataItems, err := wal.ReadAll()
		require.NoError(t, err)
		require.NotNil(t, dataItems)
		require.Len(t, dataItems, NumRec)
		for i, data := range dataItems {
			require.Equal(t, byte(i), data[0])
		}

		// continue to write
		logger.Infof(">>> Continue to write")

		for m := range NumRec {
			for n := range NumBytes {
				data1[n] = byte(m)
			}

			if m == NumRec/2 {
				err = wal.Append(data1, true)
			} else {
				err = wal.Append(data1, false)
			}
			require.NoError(t, err)
		}

		err = wal.Close()
		require.NoError(t, err)

		logger.Infof(">>> Open #2")

		wal, err = Open(logger, dirPath, &Options{FileSizeBytes: 10 * 1024, BufferSizeBytes: 2048})
		require.NoError(t, err)
		require.NotNil(t, wal)
		if wal == nil {
			return
		}

		logger.Infof(">>> ReadAll #2")

		dataItems, err = wal.ReadAll()
		require.NoError(t, err)
		require.NotNil(t, dataItems)
		require.Len(t, dataItems, NumRec/2)
		for i, data := range dataItems {
			require.Equal(t, byte(i+NumRec/2), data[0])
		}

		err = wal.Close()
		require.NoError(t, err)
	})
}

// TestWriteAheadLogFile_OpenRemovesAnchorOnlyFiles covers the files that store nothing: a
// CRC-anchor and nothing else. They are what a node that never accepts a proposal leaves behind on
// every restart, and nothing else ever deletes them.
func TestWriteAheadLogFile_OpenRemovesAnchorOnlyFiles(t *testing.T) {
	basicLog, err := zap.NewDevelopment()
	require.NoError(t, err)

	logger := basicLog.Sugar()

	walFile := func(index int) string {
		return fmt.Sprintf(walFileTemplate, index)
	}

	t.Run("Removes the anchor-only tail", func(t *testing.T) {
		testDir := t.TempDir()

		dirPath := filepath.Join(testDir, "anchor-only-tail")

		wal, err := Create(logger, dirPath, nil)
		require.NoError(t, err)
		require.NoError(t, wal.Append([]byte{1, 2, 3, 4}, false))
		require.NoError(t, wal.Close())

		// One restart of a node that writes nothing: ReadAll() always creates the file the WAL
		// switches to write in, and in this case it holds nothing but its anchor.
		wal, err = Open(logger, dirPath, nil)
		require.NoError(t, err)
		_, err = wal.ReadAll()
		require.NoError(t, err)
		require.NoError(t, wal.Close())

		names, err := dirReadWalNames(dirPath)
		require.NoError(t, err)
		require.Equal(t, []string{walFile(1), walFile(2)}, names)

		wal, err = Open(logger, dirPath, nil)
		require.NoError(t, err)
		require.NoError(t, wal.Close())

		names, err = dirReadWalNames(dirPath)
		require.NoError(t, err)
		require.Equal(t, []string{walFile(1)}, names, "the anchor-only tail file must be gone")

		// The sequence left behind is still continuous.
		indexes, err := checkWalFiles(logger, dirPath, names)
		require.NoError(t, err)
		require.Equal(t, []uint64{1}, indexes)
	})

	t.Run("Removes every consecutive anchor-only tail", func(t *testing.T) {
		testDir := t.TempDir()

		dirPath := filepath.Join(testDir, "many-anchor-only-tails")

		wal, err := Create(logger, dirPath, nil)
		require.NoError(t, err)
		require.NoError(t, wal.Append([]byte{1, 2, 3, 4}, false))
		require.NoError(t, wal.Close())

		// Three files with nothing in them, the way a node that writes no record used to leave
		// one behind on every restart.
		writeAnchorOnlyFiles(t, logger, dirPath, 2, 3, 4)

		names, err := dirReadWalNames(dirPath)
		require.NoError(t, err)
		require.Equal(t, []string{walFile(1), walFile(2), walFile(3), walFile(4)}, names)

		wal, err = Open(logger, dirPath, nil)
		require.NoError(t, err)

		names, err = dirReadWalNames(dirPath)
		require.NoError(t, err)
		require.Equal(t, []string{walFile(1)}, names, "only the file holding records may survive")

		indexes, err := checkWalFiles(logger, dirPath, names)
		require.NoError(t, err)
		require.Equal(t, []uint64{1}, indexes)

		_, err = wal.ReadAll()
		require.NoError(t, err)
		require.NoError(t, wal.Close())
	})

	t.Run("Keeps a tail that holds records", func(t *testing.T) {
		testDir := t.TempDir()

		dirPath := filepath.Join(testDir, "tail-with-records")

		wal, err := Create(logger, dirPath, &Options{FileSizeBytes: 4096})
		require.NoError(t, err)

		// Rotate onto a few files, then write one more record, so that the tail file is not the
		// empty one a rotation just created.
		for {
			require.NoError(t, wal.Append(make([]byte, 1000), false))

			names, err := dirReadWalNames(dirPath)
			require.NoError(t, err)
			if len(names) == 3 {
				break
			}
		}

		require.NoError(t, wal.Append(make([]byte, 1000), false))
		require.NoError(t, wal.Close())

		namesBefore, err := dirReadWalNames(dirPath)
		require.NoError(t, err)
		require.Greater(t, len(namesBefore), 1, "the fixture must rotate onto more than one file")

		wal, err = Open(logger, dirPath, nil)
		require.NoError(t, err)
		require.NoError(t, wal.Close())

		namesAfter, err := dirReadWalNames(dirPath)
		require.NoError(t, err)
		require.Equal(t, namesBefore, namesAfter, "a file holding records is never deleted")
	})

	t.Run("Keeps the last file of an empty WAL", func(t *testing.T) {
		testDir := t.TempDir()

		dirPath := filepath.Join(testDir, "only-anchor")

		wal, err := Create(logger, dirPath, nil)
		require.NoError(t, err)
		require.NoError(t, wal.Close())

		wal, err = Open(logger, dirPath, nil)
		require.NoError(t, err)

		names, err := dirReadWalNames(dirPath)
		require.NoError(t, err)
		require.Equal(t, []string{walFile(1)}, names, "an empty WAL is still a WAL of one file")

		items, err := wal.ReadAll()
		require.NoError(t, err)
		require.Empty(t, items)
		require.NoError(t, wal.Close())
	})
}

// TestWriteAheadLogFile_RestartDoesNotGrowFiles is the audit measurement: restarts of a node that
// writes records but never marks any of them as a truncation point used to add a file each.
func TestWriteAheadLogFile_RestartDoesNotGrowFiles(t *testing.T) {
	basicLog, err := zap.NewDevelopment()
	require.NoError(t, err)

	logger := basicLog.Sugar()

	testDir := t.TempDir()

	dirPath := filepath.Join(testDir, "restarts")

	const NumRecords = 5

	wal, err := Create(logger, dirPath, nil)
	require.NoError(t, err)

	for m := range NumRecords {
		require.NoError(t, wal.Append([]byte{byte(m + 1)}, false))
	}

	require.NoError(t, wal.Close())

	var names []string

	for cycle := 1; cycle <= 20; cycle++ {
		wal, err = Open(logger, dirPath, nil)
		require.NoError(t, err)

		items, err := wal.ReadAll()
		require.NoError(t, err)
		require.Len(t, items, NumRecords)
		require.NoError(t, wal.Close())

		names, err = dirReadWalNames(dirPath)
		require.NoError(t, err)
		require.LessOrEqual(t, len(names), 2, "cycle %d left %d files behind", cycle, len(names))

		if cycle == 1 {
			continue
		}

		require.Len(t, names, 2, "cycle %d changed the number of files", cycle)
	}
}

// TestWriteAheadLogFile_TruncationKeepsNumberOfFiles is the regression from the brief: records
// without a truncation point followed by one with it.
func TestWriteAheadLogFile_TruncationKeepsNumberOfFiles(t *testing.T) {
	basicLog, err := zap.NewDevelopment()
	require.NoError(t, err)

	logger := basicLog.Sugar()

	testDir := t.TempDir()

	dirPath := filepath.Join(testDir, "truncation")

	options := &Options{FileSizeBytes: 4096}

	wal, err := Create(logger, dirPath, options)
	require.NoError(t, err)

	for range 20 {
		require.NoError(t, wal.Append(make([]byte, 1000), false))
	}

	names, err := dirReadWalNames(dirPath)
	require.NoError(t, err)
	beforeTruncation := len(names)
	require.Greater(t, beforeTruncation, 1)

	truncationRecord := make([]byte, 1000)
	require.NoError(t, wal.Append(truncationRecord, true))

	names, err = dirReadWalNames(dirPath)
	require.NoError(t, err)
	afterTruncationRecord := len(names)
	require.Equal(t, beforeTruncation, afterTruncationRecord, "the truncation point deletes nothing on its own")

	// Write on until the rotation that follows the truncation point: that is where the files
	// preceding it are dropped.
	var recordsAfterTruncation int

	for range 100 {
		require.NoError(t, wal.Append(make([]byte, 1000), false))
		recordsAfterTruncation++

		names, err = dirReadWalNames(dirPath)
		require.NoError(t, err)
		if len(names) < afterTruncationRecord {
			break
		}
	}
	afterTruncation := len(names)
	require.Less(t, afterTruncation, beforeTruncation, "the files before the truncation point must be gone")

	// Write on well past that rotation: nothing is deleted any more, the number of files only
	// grows with the records.
	const moreRecords = 40

	for range moreRecords {
		require.NoError(t, wal.Append(make([]byte, 1000), false))
		recordsAfterTruncation++

		names, err = dirReadWalNames(dirPath)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(names), afterTruncation, "no file may be deleted after the truncation point")
	}

	require.NoError(t, wal.Close())

	names, err = dirReadWalNames(dirPath)
	require.NoError(t, err)
	rotated := len(names)

	wal, err = Open(logger, dirPath, options)
	require.NoError(t, err)

	items, err := wal.ReadAll()
	require.NoError(t, err)
	require.NoError(t, wal.Close())

	names, err = dirReadWalNames(dirPath)
	require.NoError(t, err)
	require.Len(t, names, rotated, "a restart must not add a file")

	// The truncation point and everything written after it survived the restart.
	require.Len(t, items, recordsAfterTruncation+1)
	require.Equal(t, truncationRecord, items[0])
}

// writeAnchorOnlyFiles adds log files that hold nothing but their CRC-anchor, chained to the CRC
// the WAL is currently at. It is how a node that writes no record used to leave one such file
// behind on every restart.
func writeAnchorOnlyFiles(t *testing.T, logger api.Logger, dirPath string, indexes ...uint64) {
	lastFilePath := filepath.Join(dirPath, fmt.Sprintf(walFileTemplate, indexes[0]-1))

	r, err := NewLogRecordReader(logger, lastFilePath)
	require.NoError(t, err)

	for {
		_, readErr := r.Read()
		if readErr != nil {
			require.ErrorIs(t, readErr, io.EOF)
			break
		}
	}

	crc := r.CRC()
	require.NoError(t, r.Close())

	for _, index := range indexes {
		writer := &WriteAheadLogFile{
			dirName:    dirPath,
			logger:     logger,
			headerBuff: make([]byte, recordHeaderSize),
			crc:        crc,
		}

		filePath := filepath.Join(dirPath, fmt.Sprintf(walFileTemplate, index))

		writer.logFile, err = os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY, walFilePermPrivateRW)
		require.NoError(t, err)
		require.NoError(t, writer.saveCRC())
		require.NoError(t, writer.logFile.Close())
	}
}

func TestOptions_String(t *testing.T) {
	require.Equal(t,
		fmt.Sprintf("{FileSizeBytes: %d, BufferSizeBytes: %d}",
			FileSizeBytesDefault, BufferSizeBytesDefault),
		DefaultOptions().String(),
	)
}

func TestWriteAheadLogFile_Repair(t *testing.T) {
	testDir := t.TempDir()

	basicLog, err := zap.NewDevelopment()
	require.NoError(t, err)

	logger := basicLog.Sugar()

	t.Run("with a tail", func(t *testing.T) {
		dirPath := filepath.Join(testDir, "tail")
		err := os.MkdirAll(dirPath, walDirPermPrivateRWX)
		require.NoError(t, err)

		wal, err := Create(logger, dirPath, &Options{FileSizeBytes: 10 * 1024, BufferSizeBytes: 2048})
		require.NoError(t, err)
		require.NotNil(t, wal)
		if wal == nil {
			return
		}

		const NumBytes = 1024
		const NumRec = 102
		data1 := make([]byte, NumBytes)
		for m := range NumRec {
			for n := range NumBytes {
				data1[n] = byte(m)
			}
			err = wal.Append(data1, false)
			require.NoError(t, err)
		}

		err = wal.Close()
		require.NoError(t, err)

		// add tail to last file
		names, err := dirReadWalNames(dirPath)
		require.NoError(t, err)
		lastFile := filepath.Join(dirPath, names[len(names)-1])
		f, err := os.OpenFile(lastFile, os.O_RDWR, walFilePermPrivateRW)
		require.NoError(t, err)
		_, err = f.Seek(0, io.SeekEnd)
		require.NoError(t, err)
		_, err = f.Write(make([]byte, 64))
		require.NoError(t, err)
		err = f.Close()
		require.NoError(t, err)

		assertTestRepair(t, logger, dirPath, NumRec)
	})

	t.Run("broken record", func(t *testing.T) {
		dirPath := filepath.Join(testDir, "broken")
		err := os.MkdirAll(dirPath, walDirPermPrivateRWX)
		require.NoError(t, err)

		wal, err := Create(logger, dirPath, &Options{FileSizeBytes: 10 * 1024, BufferSizeBytes: 2048})
		require.NoError(t, err)
		require.NotNil(t, wal)
		if wal == nil {
			return
		}

		const NumBytes = 1024
		const NumRec = 102
		data1 := make([]byte, NumBytes)
		for m := range NumRec {
			for n := range NumBytes {
				data1[n] = byte(m)
			}
			err = wal.Append(data1, false)
			require.NoError(t, err)
		}

		err = wal.Close()
		require.NoError(t, err)

		// truncate last file
		names, err := dirReadWalNames(dirPath)
		require.NoError(t, err)
		lastFile := filepath.Join(dirPath, names[len(names)-1])
		f, err := os.OpenFile(lastFile, os.O_RDWR, walFilePermPrivateRW)
		require.NoError(t, err)
		offset, err := f.Seek(-1, io.SeekEnd)
		require.NoError(t, err)
		err = f.Truncate(offset)
		require.NoError(t, err)
		err = f.Close()
		require.NoError(t, err)

		assertTestRepair(t, logger, dirPath, NumRec-1)
	})

	t.Run("unknown record type at the tail", func(t *testing.T) {
		dirPath := filepath.Join(testDir, "unknown-type")
		err := os.MkdirAll(dirPath, walDirPermPrivateRWX)
		require.NoError(t, err)

		wal, err := Create(logger, dirPath, &Options{FileSizeBytes: 10 * 1024, BufferSizeBytes: 2048})
		require.NoError(t, err)
		require.NotNil(t, wal)
		if wal == nil {
			return
		}

		const NumBytes = 1024
		const NumRec = 5
		data1 := make([]byte, NumBytes)
		for m := range NumRec {
			for n := range NumBytes {
				data1[n] = byte(m)
			}
			err = wal.Append(data1, false)
			require.NoError(t, err)
		}

		err = wal.Close()
		require.NoError(t, err)

		// append a well-formed record whose type the reader does not know
		names, err := dirReadWalNames(dirPath)
		require.NoError(t, err)
		lastFile := filepath.Join(dirPath, names[len(names)-1])

		rec, err := proto.Marshal(&smartbftprotos.LogRecord{Type: smartbftprotos.LogRecord_Type(7), Data: []byte{1, 2, 3}})
		require.NoError(t, err)
		_, pad := getPadBytes(len(rec))
		header := make([]byte, recordHeaderSize)
		binary.LittleEndian.PutUint64(header, uint64(len(rec)))

		f, err := os.OpenFile(lastFile, os.O_RDWR, walFilePermPrivateRW)
		require.NoError(t, err)
		_, err = f.Seek(0, io.SeekEnd)
		require.NoError(t, err)
		_, err = f.Write(append(append(header, rec...), pad...))
		require.NoError(t, err)
		require.NoError(t, f.Close())

		wal, err = Open(logger, dirPath, &Options{FileSizeBytes: 10 * 1024, BufferSizeBytes: 2048})
		require.NoError(t, err)
		_, err = wal.ReadAll()
		require.ErrorIs(t, err, io.ErrUnexpectedEOF)
		require.NoError(t, wal.Close())

		assertTestRepair(t, logger, dirPath, NumRec)
	})
}

func TestWriteAheadLogFile_InitializeAndReadAll(t *testing.T) {
	testDir := t.TempDir()

	basicLog, err := zap.NewProduction()
	require.NoError(t, err)

	logger := basicLog.Sugar()

	// Create
	wal, entries, err := InitializeAndReadAll(logger, testDir, DefaultOptions())
	require.NoError(t, err)
	require.NotNil(t, wal)
	require.Empty(t, entries)

	crc := wal.CRC()
	err = wal.Close()
	require.NoError(t, err)

	expectedFileName := fmt.Sprintf(walFileTemplate, 1)
	verifyFirstFileCreation(t, logger, testDir, expectedFileName, crc)

	// Open
	options := &Options{FileSizeBytes: 4 * 1024, BufferSizeBytes: 2048}
	wal, entries, err = InitializeAndReadAll(logger, testDir, options)
	require.NoError(t, err)
	require.NotNil(t, wal)
	require.Empty(t, entries)

	const (
		NumBytes = 1024
		NumRec   = 20
	)

	for m := range NumRec {
		data1 := make([]byte, NumBytes)
		for n := range NumBytes {
			data1[n] = byte(n % (m + 1))
		}

		err = wal.Append(data1, false)
		require.NoError(t, err)
	}

	err = wal.Close()
	require.NoError(t, err)

	wal, entries, err = InitializeAndReadAll(logger, testDir, options)
	require.NoError(t, err)
	require.NotNil(t, wal)
	require.NotNil(t, entries)
	require.Len(t, entries, NumRec)

	rec1 := []byte{1, 2, 3, 4}
	err = wal.Append(rec1, true)
	require.NoError(t, err)
	err = wal.Close()
	require.NoError(t, err)

	// Corrupt, add tail to last file
	names, err := dirReadWalNames(testDir)
	require.NoError(t, err)

	lastFile := filepath.Join(testDir, names[len(names)-1])
	f, err := os.OpenFile(lastFile, os.O_RDWR, walFilePermPrivateRW)
	require.NoError(t, err)
	_, err = f.Seek(0, io.SeekEnd)
	require.NoError(t, err)
	_, err = f.Write(make([]byte, 64))
	require.NoError(t, err)
	err = f.Close()
	require.NoError(t, err)

	// Repair
	wal, entries, err = InitializeAndReadAll(logger, testDir, options)
	require.NoError(t, err)
	require.NotNil(t, wal)
	require.NotNil(t, entries)
	require.Len(t, entries, 1)
	require.Equal(t, rec1, entries[0])

	rec2 := []byte{5, 6, 7, 8}
	err = wal.Append(rec2, false)
	require.NoError(t, err)
	err = wal.Close()
	require.NoError(t, err)

	// One last time
	wal, entries, err = InitializeAndReadAll(logger, testDir, options)
	require.NoError(t, err)
	require.NotNil(t, wal)
	require.NotNil(t, entries)
	require.Len(t, entries, 2)
	require.Equal(t, rec1, entries[0])
	require.Equal(t, rec2, entries[1])

	err = wal.Close()
	require.NoError(t, err)
}

func TestWriteAheadLogFile_InitializeAndReadAll_TornLastAnchor(t *testing.T) {
	testDir := t.TempDir()

	basicLog, err := zap.NewDevelopment()
	require.NoError(t, err)

	logger := basicLog.Sugar()

	options := &Options{FileSizeBytes: 10 * 1024, BufferSizeBytes: 2048}

	wal, _, err := InitializeAndReadAll(logger, testDir, options)
	require.NoError(t, err)

	const NumRec = 5
	for m := range NumRec {
		err = wal.Append([]byte{byte(m), 1, 2, 3}, false)
		require.NoError(t, err)
	}
	require.NoError(t, wal.Close())

	// Simulate a crash while the next file was being prepared: the new last file
	// exists but its CRC-Anchor was never written.
	names, err := dirReadWalNames(testDir)
	require.NoError(t, err)
	lastIndex, err := parseWalFileName(names[len(names)-1])
	require.NoError(t, err)
	tornFile := filepath.Join(testDir, fmt.Sprintf(walFileTemplate, lastIndex+1))
	require.NoError(t, os.WriteFile(tornFile, nil, walFilePermPrivateRW))

	// Open surfaces the repairable error, instead of reading the state.
	_, err = Open(logger, testDir, options)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)

	wal, entries, err := InitializeAndReadAll(logger, testDir, options)
	require.NoError(t, err)
	require.NotNil(t, wal)
	require.Len(t, entries, NumRec)
	for i, e := range entries {
		require.Equal(t, byte(i), e[0])
	}
	require.NoError(t, wal.Close())

	// the torn file was removed by the repair (a copy is kept for operators),
	// and ReadAll then created a fresh file for writing.
	_, err = os.Stat(tornFile + ".copy")
	require.NoError(t, err, "the repair must keep a copy of the torn file")
}

func TestRepairCorruptAnchorOfNonEmptyLastFile(t *testing.T) {
	basicLog, err := zap.NewDevelopment()
	require.NoError(t, err)

	logger := basicLog.Sugar()
	dir := t.TempDir()
	options := &Options{FileSizeBytes: 64 * 1024}

	wal, err := Create(logger, dir, options)
	require.NoError(t, err)
	for range 100 {
		require.NoError(t, wal.Append(make([]byte, 1024), false))
	}
	require.NoError(t, wal.Close())

	names, err := dirReadWalNames(dir)
	require.NoError(t, err)
	require.Len(t, names, 2)
	lastFile := filepath.Join(dir, names[len(names)-1])

	// Corrupt the CRC-Anchor of a file that holds records.
	f, err := os.OpenFile(lastFile, os.O_RDWR, walFilePermPrivateRW)
	require.NoError(t, err)
	b := make([]byte, 1)
	_, err = f.ReadAt(b, int64(recordHeaderSize))
	require.NoError(t, err)
	b[0] ^= 0xFF
	_, err = f.WriteAt(b, int64(recordHeaderSize))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	sizeBefore, err := os.Stat(lastFile)
	require.NoError(t, err)

	_, _, err = InitializeAndReadAll(logger, dir, options)
	require.ErrorIs(t, err, ErrNotRepairable)

	fi, err := os.Stat(lastFile)
	require.NoError(t, err)
	require.Equal(t, sizeBefore.Size(), fi.Size(), "the file must be left untouched")
}

func assertTestRepair(t *testing.T, logger api.Logger, dirPath string, numRec int) {
	logger.Infof(">>> Open #1")
	wal, err := Open(logger, dirPath, &Options{FileSizeBytes: 10 * 1024, BufferSizeBytes: 2048})
	require.NoError(t, err)
	require.NotNil(t, wal)

	if wal == nil {
		return
	}

	logger.Infof(">>> ReadAll #1 - will fail")

	_, err = wal.ReadAll()
	require.Error(t, err)
	err = wal.Close()
	require.NoError(t, err)

	names, err := dirReadWalNames(dirPath)
	require.NoError(t, err)

	logger.Infof(">>> Repair")
	err = Repair(logger, dirPath)
	require.NoError(t, err)

	logger.Infof(">>> Open #2")
	wal, err = Open(logger, dirPath, &Options{FileSizeBytes: 10 * 1024, BufferSizeBytes: 2048})
	require.NoError(t, err)
	require.NotNil(t, wal)

	if wal == nil {
		return
	}

	logger.Infof(">>> ReadAll #2 OK")

	dataItems, err := wal.ReadAll()
	require.NoError(t, err)
	require.NotNil(t, dataItems)
	require.Len(t, dataItems, numRec)

	for i, data := range dataItems {
		require.Equal(t, byte(i), data[0])
	}

	err = wal.Close()
	require.NoError(t, err)

	cp, err := os.Open(filepath.Join(dirPath, names[len(names)-1]) + ".copy")
	require.NoError(t, err)

	_ = cp.Close()
}

func verifyFirstFileCreation(
	t *testing.T,
	logger api.Logger,
	dirPath string,
	expectedFileName string,
	expectedCRC uint32,
) {
	names, err := dirReadWalNames(dirPath)
	require.NoError(t, err)
	require.Len(t, names, 1)
	require.Equal(t, expectedFileName, names[0])

	r, err := NewLogRecordReader(logger, filepath.Join(dirPath, expectedFileName))
	require.NoError(t, err)
	require.NotNil(t, r)

	if r != nil {
		defer r.Close()
		record, err := r.Read()
		require.Error(t, err, "no more records")
		require.Nil(t, record)
		require.Equal(t, expectedCRC, r.CRC())
	}
}

func verifyAppend(
	t *testing.T,
	logger api.Logger,
	dirPath string,
	expectedFileName string,
	expectedCRC uint32,
	records ...*smartbftprotos.LogRecord,
) {
	r, err := NewLogRecordReader(logger, filepath.Join(dirPath, expectedFileName))
	require.NoError(t, err)
	require.NotNil(t, r)

	if r == nil {
		return
	}
	defer r.Close()

	for _, expectedRecord := range records {
		record, err := r.Read()
		require.NoError(t, err)
		require.NotNil(t, record)
		require.Equal(t, expectedRecord.GetData(), record.GetData())
		require.Equal(t, expectedRecord.GetType(), record.GetType())
		require.Equal(t, expectedRecord.GetTruncateTo(), record.GetTruncateTo())
	}

	record, err := r.Read()
	require.Error(t, err, "no more records")
	require.Nil(t, record)
	require.Equal(t, expectedCRC, r.CRC())
}
