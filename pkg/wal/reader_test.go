// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package wal

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/hyperledger/SmartBFT/smartbftprotos"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

func TestLogRecordReader(t *testing.T) {
	testDir := t.TempDir()

	basicLog, err := zap.NewDevelopment()
	require.NoError(t, err)

	logger := basicLog.Sugar()

	wal, err := Create(logger, testDir, nil)
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
	err = wal.Append(rec1.Data, rec1.TruncateTo)
	require.NoError(t, err)

	crc1 := wal.CRC()
	rec2 := &smartbftprotos.LogRecord{
		Type:       smartbftprotos.LogRecord_ENTRY,
		TruncateTo: false,
		Data:       []byte{5, 6, 7, 8, 9, 10, 11, 12},
	}

	err = wal.Append(rec2.Data, rec2.TruncateTo)
	require.NoError(t, err)

	crc2 := wal.CRC()
	rec3 := &smartbftprotos.LogRecord{
		Type:       smartbftprotos.LogRecord_ENTRY,
		TruncateTo: false,
		Data:       []byte{13, 14, 15, 16, 17, 18, 19, 20},
	}
	err = wal.Append(rec3.Data, rec3.TruncateTo)
	require.NoError(t, err)

	crc3 := wal.CRC()
	wal.Close()

	fileName := filepath.Join(testDir, fmt.Sprintf(walFileTemplate, 1))

	t.Run("Create and Read till EOF", func(t *testing.T) {
		r, err := NewLogRecordReader(logger, fileName)
		require.NoError(t, err)
		require.NotNil(t, r)
		require.Equal(t, walCRCSeed, r.CRC())

		assertReadRecord(t, r, rec1, crc1)
		assertReadRecord(t, r, rec2, crc2)
		assertReadRecord(t, r, rec3, crc3)
		record, err := r.Read()
		require.EqualError(t, err, io.EOF.Error())
		require.Nil(t, record)
	})

	t.Run("bad anchor", func(t *testing.T) {
		testFile := filepath.Join(testDir, "test1.wal")
		err = copyFile(fileName, testFile)
		require.NoError(t, err)
		f, err := os.OpenFile(testFile, os.O_RDWR, walFilePermPrivateRW)
		require.NoError(t, err)
		_, err = f.Seek(0, io.SeekStart)
		require.NoError(t, err)
		_, err = f.Write([]byte{0, 0, 0, 0, 0, 0, 0, 0})
		require.NoError(t, err)
		err = f.Close()
		require.NoError(t, err)

		r, err := NewLogRecordReader(logger, testFile)
		require.Contains(t, err.Error(), "failed reading CRC-Anchor from log file:")
		require.Nil(t, r)
	})

	t.Run("corrupt anchor crc", func(t *testing.T) {
		testFile := filepath.Join(testDir, "test2.wal")
		err = copyFile(fileName, testFile)
		require.NoError(t, err)
		f, err := os.OpenFile(fileName, os.O_RDWR, walFilePermPrivateRW)
		require.NoError(t, err)
		h := make([]byte, 8)
		_, err = f.Read(h)
		require.NoError(t, err)
		err = f.Close()
		require.NoError(t, err)

		f, err = os.OpenFile(testFile, os.O_RDWR, walFilePermPrivateRW)
		require.NoError(t, err)
		_, err = f.Seek(0, io.SeekStart)
		require.NoError(t, err)
		h[7] = ^h[7] // a CRC byte
		_, err = f.Write(h)
		require.NoError(t, err)
		err = f.Close()
		require.NoError(t, err)

		r, err := NewLogRecordReader(logger, testFile)
		require.NoError(t, err)
		require.NotNil(t, r)

		record, err := r.Read()
		require.EqualError(t, err, ErrCRC.Error())
		require.Nil(t, record)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("file with tail", func(t *testing.T) {
		testFile := filepath.Join(testDir, "test3.wal")
		err = copyFile(fileName, testFile)
		require.NoError(t, err)
		f, err := os.OpenFile(testFile, os.O_RDWR, walFilePermPrivateRW)
		require.NoError(t, err)
		_, err = f.Seek(0, io.SeekEnd)
		require.NoError(t, err)
		_, err = f.Write([]byte{0, 1, 2, 3})
		require.NoError(t, err)
		err = f.Close()
		require.NoError(t, err)

		r, err := NewLogRecordReader(logger, testFile)
		require.NoError(t, err)
		require.NotNil(t, r)

		assertReadRecord(t, r, rec1, crc1)
		assertReadRecord(t, r, rec2, crc2)
		assertReadRecord(t, r, rec3, crc3)

		record, err := r.Read()
		require.EqualError(t, err, io.ErrUnexpectedEOF.Error())
		require.Nil(t, record)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("partial record", func(t *testing.T) {
		testFile := filepath.Join(testDir, "test4.wal")
		err = copyFile(fileName, testFile)
		require.NoError(t, err)
		f, err := os.OpenFile(testFile, os.O_RDWR, walFilePermPrivateRW)
		require.NoError(t, err)
		offset, err := f.Seek(-1, io.SeekEnd)
		require.NoError(t, err)
		err = f.Truncate(offset)
		require.NoError(t, err)
		err = f.Close()
		require.NoError(t, err)

		r, err := NewLogRecordReader(logger, testFile)
		require.NoError(t, err)
		require.NotNil(t, r)

		assertReadRecord(t, r, rec1, crc1)
		assertReadRecord(t, r, rec2, crc2)

		record, err := r.Read()
		require.EqualError(t, err, io.ErrUnexpectedEOF.Error())
		require.Nil(t, record)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("corrupt record pad", func(t *testing.T) {
		testFile := filepath.Join(testDir, "test5.wal")
		err = copyFile(fileName, testFile)
		require.NoError(t, err)
		r, err := NewLogRecordReader(logger, fileName)
		require.NoError(t, err)
		require.NotNil(t, r)
		assertReadRecord(t, r, rec1, crc1)
		assertReadRecord(t, r, rec2, crc2)
		length, _, err := r.readHeader()
		require.NoError(t, err)
		require.Positive(t, getPadSize(int(length)))
		_ = r.Close()

		f, err := os.OpenFile(testFile, os.O_RDWR, walFilePermPrivateRW)
		require.NoError(t, err)
		_, err = f.Seek(-1, io.SeekEnd)
		require.NoError(t, err)
		b := []byte{0}
		_, err = f.Read(b)
		require.NoError(t, err)
		b[0] ^= 0x01
		_, err = f.Seek(-1, io.SeekEnd)
		require.NoError(t, err)
		_, err = f.Write(b)
		require.NoError(t, err)
		err = f.Close()
		require.NoError(t, err)

		r, err = NewLogRecordReader(logger, testFile)
		require.NoError(t, err)
		require.NotNil(t, r)

		assertReadRecord(t, r, rec1, crc1)
		assertReadRecord(t, r, rec2, crc2)

		record, err := r.Read()
		require.EqualError(t, err, ErrCRC.Error())
		require.Nil(t, record)
	})

	t.Run("corrupt record", func(t *testing.T) {
		testFile := filepath.Join(testDir, "test6.wal")
		err = copyFile(fileName, testFile)
		require.NoError(t, err)
		r, err := NewLogRecordReader(logger, fileName)
		require.NoError(t, err)
		assertReadRecord(t, r, rec1, crc1)
		assertReadRecord(t, r, rec2, crc2)
		length, _, err := r.readHeader()
		require.NoError(t, err)
		require.Greater(t, int(length), 8)
		offset, err := r.logFile.Seek(0, io.SeekCurrent)
		require.NoError(t, err)
		err = r.Close()
		require.NoError(t, err)

		f, err := os.OpenFile(testFile, os.O_RDWR, walFilePermPrivateRW)
		require.NoError(t, err)
		_, err = f.Seek(offset, io.SeekStart)
		require.NoError(t, err)
		b := []byte{0, 1, 2, 3, 4, 5, 6, 7}
		_, err = f.Write(b)
		require.NoError(t, err)
		err = f.Close()
		require.NoError(t, err)

		r, err = NewLogRecordReader(logger, testFile)
		require.NoError(t, err)
		require.NotNil(t, r)

		assertReadRecord(t, r, rec1, crc1)
		assertReadRecord(t, r, rec2, crc2)

		record, err := r.Read()
		require.Contains(t, err.Error(), "wal: failed to unmarshal payload")
		require.Nil(t, record)
	})

	t.Run("record length in header is bounded", func(t *testing.T) {
		fileName := filepath.Join(testDir, "test7.wal")
		sourceName := filepath.Join(testDir, fmt.Sprintf(walFileTemplate, 1))
		require.NoError(t, copyFile(sourceName, fileName))

		r, err := NewLogRecordReader(logger, fileName)
		require.NoError(t, err)
		assertReadRecord(t, r, rec1, crc1)
		assertReadRecord(t, r, rec2, crc2)
		offset, err := r.logFile.Seek(0, io.SeekCurrent)
		require.NoError(t, err)
		err = r.Close()
		require.NoError(t, err)

		f, err := os.OpenFile(fileName, os.O_RDWR, walFilePermPrivateRW)
		require.NoError(t, err)
		_, err = f.Seek(offset, io.SeekStart)
		require.NoError(t, err)
		// a header claiming a record of 0xFFFFFFFF bytes, i.e. ~4GB
		header := make([]byte, recordHeaderSize)
		binary.LittleEndian.PutUint64(header, 0xFFFFFFFF)
		_, err = f.Write(header)
		require.NoError(t, err)
		err = f.Close()
		require.NoError(t, err)

		r, err = NewLogRecordReader(logger, fileName)
		require.NoError(t, err)
		assertReadRecord(t, r, rec1, crc1)
		assertReadRecord(t, r, rec2, crc2)

		record, err := r.Read()
		require.ErrorIs(t, err, ErrRecordTooBig)
		require.Nil(t, record)
		require.NoError(t, r.Close())
	})

	t.Run("oversized record header", func(t *testing.T) {
		dirPath := filepath.Join(testDir, "oversized")
		require.NoError(t, os.MkdirAll(dirPath, walDirPermPrivateRWX))

		wal, err := Create(logger, dirPath, &Options{FileSizeBytes: 10 * 1024, BufferSizeBytes: 2048})
		require.NoError(t, err)

		const NumBytes = 1024
		const NumRec = 102
		data1 := make([]byte, NumBytes)
		for m := range NumRec {
			for n := range NumBytes {
				data1[n] = byte(m)
			}
			require.NoError(t, wal.Append(data1, false))
		}
		require.NoError(t, wal.Close())

		// append a header claiming a ~4GB record to the last file
		names, err := dirReadWalNames(dirPath)
		require.NoError(t, err)
		lastFile := filepath.Join(dirPath, names[len(names)-1])
		f, err := os.OpenFile(lastFile, os.O_RDWR, walFilePermPrivateRW)
		require.NoError(t, err)
		_, err = f.Seek(0, io.SeekEnd)
		require.NoError(t, err)
		header := make([]byte, recordHeaderSize)
		binary.LittleEndian.PutUint64(header, 0xFFFFFFFF)
		_, err = f.Write(header)
		require.NoError(t, err)
		require.NoError(t, f.Close())

		wal, err = Open(logger, dirPath, &Options{FileSizeBytes: 10 * 1024, BufferSizeBytes: 2048})
		require.NoError(t, err)
		_, err = wal.ReadAll()
		require.ErrorIs(t, err, io.ErrUnexpectedEOF)
		require.NoError(t, wal.Close())

		assertTestRepair(t, logger, dirPath, NumRec)
	})

	t.Run("truncated payload after an intact header", func(t *testing.T) {
		dirPath := filepath.Join(testDir, "truncated-payload")
		require.NoError(t, os.MkdirAll(dirPath, walDirPermPrivateRWX))

		wal, err := Create(logger, dirPath, &Options{FileSizeBytes: 10 * 1024, BufferSizeBytes: 2048})
		require.NoError(t, err)

		const NumBytes = 1024
		const NumRec = 5
		data1 := make([]byte, NumBytes)
		for m := range NumRec {
			for n := range NumBytes {
				data1[n] = byte(m)
			}
			require.NoError(t, wal.Append(data1, false))
		}
		require.NoError(t, wal.Close())

		// Append a header that declares a payload, without writing the payload.
		// The header is intact and claims fewer bytes than the whole file, so the
		// reader must report a torn tail, not a clean end of file.
		names, err := dirReadWalNames(dirPath)
		require.NoError(t, err)
		lastFile := filepath.Join(dirPath, names[len(names)-1])
		f, err := os.OpenFile(lastFile, os.O_RDWR, walFilePermPrivateRW)
		require.NoError(t, err)
		_, err = f.Seek(0, io.SeekEnd)
		require.NoError(t, err)
		header := make([]byte, recordHeaderSize)
		binary.LittleEndian.PutUint64(header, 100)
		_, err = f.Write(header)
		require.NoError(t, err)
		require.NoError(t, f.Close())

		wal, err = Open(logger, dirPath, &Options{FileSizeBytes: 10 * 1024, BufferSizeBytes: 2048})
		require.NoError(t, err)
		_, err = wal.ReadAll()
		require.ErrorIs(t, err, io.ErrUnexpectedEOF)
		require.NoError(t, wal.Close())

		assertTestRepair(t, logger, dirPath, NumRec)
	})

	t.Run("record bound follows the file size, not the default", func(t *testing.T) {
		fileName := filepath.Join(testDir, "test8.wal")
		sourceName := filepath.Join(testDir, fmt.Sprintf(walFileTemplate, 1))
		require.NoError(t, copyFile(sourceName, fileName))

		fileInfo, err := os.Stat(fileName)
		require.NoError(t, err)

		r, err := NewLogRecordReader(logger, fileName)
		require.NoError(t, err)
		require.Equal(t, fileInfo.Size(), r.fileSizeBytes)

		assertReadRecord(t, r, rec1, crc1)
		assertReadRecord(t, r, rec2, crc2)
		offset, err := r.logFile.Seek(0, io.SeekCurrent)
		require.NoError(t, err)
		require.NoError(t, r.Close())

		// A header claiming more than the file holds, as after a torn write.
		f, err := os.OpenFile(fileName, os.O_RDWR, walFilePermPrivateRW)
		require.NoError(t, err)
		_, err = f.Seek(offset, io.SeekStart)
		require.NoError(t, err)
		header := make([]byte, recordHeaderSize)
		binary.LittleEndian.PutUint64(header, uint64(fileInfo.Size()))
		_, err = f.Write(header)
		require.NoError(t, err)
		require.NoError(t, f.Close())

		r, err = NewLogRecordReader(logger, fileName)
		require.NoError(t, err)
		assertReadRecord(t, r, rec1, crc1)
		assertReadRecord(t, r, rec2, crc2)

		record, err := r.Read()
		require.ErrorIs(t, err, ErrRecordTooBig)
		require.Nil(t, record)
		require.NoError(t, r.Close())
	})
}

func assertReadRecord(t *testing.T, r *LogRecordReader, expRec *smartbftprotos.LogRecord, expCRC uint32) {
	record, err := r.Read()
	require.NoError(t, err)
	require.NotNil(t, record)
	require.True(t, proto.Equal(record, expRec))
	require.Equal(t, expCRC, r.CRC())
}
