// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package wal

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/hyperledger/SmartBFT/smartbftprotos"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

func TestLogRecordReader(t *testing.T) {
	testDir := t.TempDir()

	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)

	logger := basicLog.Sugar()

	wal, err := Create(logger, testDir, nil)
	assert.NoError(t, err)
	assert.NotNil(t, wal)

	if wal == nil {
		return
	}

	rec1 := &smartbftprotos.LogRecord{
		Type:       smartbftprotos.LogRecord_ENTRY,
		TruncateTo: true,
		Data:       []byte{1, 2, 3, 4},
	}
	err = wal.Append(rec1.Data, rec1.TruncateTo)
	assert.NoError(t, err)

	crc1 := wal.CRC()
	rec2 := &smartbftprotos.LogRecord{
		Type:       smartbftprotos.LogRecord_ENTRY,
		TruncateTo: false,
		Data:       []byte{5, 6, 7, 8, 9, 10, 11, 12},
	}

	err = wal.Append(rec2.Data, rec2.TruncateTo)
	assert.NoError(t, err)

	crc2 := wal.CRC()
	rec3 := &smartbftprotos.LogRecord{
		Type:       smartbftprotos.LogRecord_ENTRY,
		TruncateTo: false,
		Data:       []byte{13, 14, 15, 16, 17, 18, 19, 20},
	}
	err = wal.Append(rec3.Data, rec3.TruncateTo)
	assert.NoError(t, err)

	crc3 := wal.CRC()
	wal.Close()

	fileName := filepath.Join(testDir, fmt.Sprintf(walFileTemplate, 1))

	t.Run("Create and Read till EOF", func(t *testing.T) {
		r, err := NewLogRecordReader(logger, fileName)
		assert.NoError(t, err)
		assert.NotNil(t, r)
		assert.Equal(t, walCRCSeed, r.CRC())

		assertReadRecord(t, r, rec1, crc1)
		assertReadRecord(t, r, rec2, crc2)
		assertReadRecord(t, r, rec3, crc3)
		record, err := r.Read()
		assert.EqualError(t, err, io.EOF.Error())
		assert.Nil(t, record)
	})

	t.Run("bad anchor", func(t *testing.T) {
		testFile := filepath.Join(testDir, "test1.wal")
		err = copyFile(fileName, testFile)
		assert.NoError(t, err)
		f, err := os.OpenFile(testFile, os.O_RDWR, walFilePermPrivateRW)
		assert.NoError(t, err)
		_, err = f.Seek(0, io.SeekStart)
		assert.NoError(t, err)
		_, err = f.Write([]byte{0, 0, 0, 0, 0, 0, 0, 0})
		assert.NoError(t, err)
		err = f.Close()
		assert.NoError(t, err)

		r, err := NewLogRecordReader(logger, testFile)
		assert.Contains(t, err.Error(), "failed reading CRC-Anchor from log file:")
		assert.Nil(t, r)
	})

	t.Run("corrupt anchor crc", func(t *testing.T) {
		testFile := filepath.Join(testDir, "test2.wal")
		err = copyFile(fileName, testFile)
		assert.NoError(t, err)
		f, err := os.OpenFile(fileName, os.O_RDWR, walFilePermPrivateRW)
		assert.NoError(t, err)
		h := make([]byte, 8)
		_, err = f.Read(h)
		assert.NoError(t, err)
		err = f.Close()
		assert.NoError(t, err)

		f, err = os.OpenFile(testFile, os.O_RDWR, walFilePermPrivateRW)
		assert.NoError(t, err)
		_, err = f.Seek(0, io.SeekStart)
		assert.NoError(t, err)
		h[7] = ^h[7] // a CRC byte
		_, err = f.Write(h)
		assert.NoError(t, err)
		err = f.Close()
		assert.NoError(t, err)

		r, err := NewLogRecordReader(logger, testFile)
		assert.NoError(t, err)
		assert.NotNil(t, r)

		record, err := r.Read()
		assert.EqualError(t, err, ErrCRC.Error())
		assert.Nil(t, record)

		err = r.Close()
		assert.NoError(t, err)
	})

	t.Run("file with tail", func(t *testing.T) {
		testFile := filepath.Join(testDir, "test3.wal")
		err = copyFile(fileName, testFile)
		assert.NoError(t, err)
		f, err := os.OpenFile(testFile, os.O_RDWR, walFilePermPrivateRW)
		assert.NoError(t, err)
		_, err = f.Seek(0, io.SeekEnd)
		assert.NoError(t, err)
		_, err = f.Write([]byte{0, 1, 2, 3})
		assert.NoError(t, err)
		err = f.Close()
		assert.NoError(t, err)

		r, err := NewLogRecordReader(logger, testFile)
		assert.NoError(t, err)
		assert.NotNil(t, r)

		assertReadRecord(t, r, rec1, crc1)
		assertReadRecord(t, r, rec2, crc2)
		assertReadRecord(t, r, rec3, crc3)

		record, err := r.Read()
		assert.EqualError(t, err, io.ErrUnexpectedEOF.Error())
		assert.Nil(t, record)

		err = r.Close()
		assert.NoError(t, err)
	})

	t.Run("partial record", func(t *testing.T) {
		testFile := filepath.Join(testDir, "test4.wal")
		err = copyFile(fileName, testFile)
		assert.NoError(t, err)
		f, err := os.OpenFile(testFile, os.O_RDWR, walFilePermPrivateRW)
		assert.NoError(t, err)
		offset, err := f.Seek(-1, io.SeekEnd)
		assert.NoError(t, err)
		err = f.Truncate(offset)
		assert.NoError(t, err)
		err = f.Close()
		assert.NoError(t, err)

		r, err := NewLogRecordReader(logger, testFile)
		assert.NoError(t, err)
		assert.NotNil(t, r)

		assertReadRecord(t, r, rec1, crc1)
		assertReadRecord(t, r, rec2, crc2)

		record, err := r.Read()
		assert.EqualError(t, err, io.ErrUnexpectedEOF.Error())
		assert.Nil(t, record)

		err = r.Close()
		assert.NoError(t, err)
	})

	t.Run("corrupt record pad", func(t *testing.T) {
		testFile := filepath.Join(testDir, "test5.wal")
		err = copyFile(fileName, testFile)
		assert.NoError(t, err)
		r, err := NewLogRecordReader(logger, fileName)
		assert.NoError(t, err)
		assert.NotNil(t, r)
		assertReadRecord(t, r, rec1, crc1)
		assertReadRecord(t, r, rec2, crc2)
		length, _, err := r.readHeader()
		assert.NoError(t, err)
		assert.True(t, getPadSize(int(length)) > 0)
		_ = r.Close()

		f, err := os.OpenFile(testFile, os.O_RDWR, walFilePermPrivateRW)
		assert.NoError(t, err)
		_, err = f.Seek(-1, io.SeekEnd)
		assert.NoError(t, err)
		b := []byte{0}
		_, err = f.Read(b)
		assert.NoError(t, err)
		b[0] ^= 0x01
		_, err = f.Seek(-1, io.SeekEnd)
		assert.NoError(t, err)
		_, err = f.Write(b)
		assert.NoError(t, err)
		err = f.Close()
		assert.NoError(t, err)

		r, err = NewLogRecordReader(logger, testFile)
		assert.NoError(t, err)
		assert.NotNil(t, r)

		assertReadRecord(t, r, rec1, crc1)
		assertReadRecord(t, r, rec2, crc2)

		record, err := r.Read()
		assert.EqualError(t, err, ErrCRC.Error())
		assert.Nil(t, record)
	})

	t.Run("corrupt record", func(t *testing.T) {
		testFile := filepath.Join(testDir, "test6.wal")
		err = copyFile(fileName, testFile)
		assert.NoError(t, err)
		r, err := NewLogRecordReader(logger, fileName)
		assert.NoError(t, err)
		assertReadRecord(t, r, rec1, crc1)
		assertReadRecord(t, r, rec2, crc2)
		length, _, err := r.readHeader()
		assert.NoError(t, err)
		assert.True(t, int(length) > 8)
		offset, err := r.logFile.Seek(0, io.SeekCurrent)
		assert.NoError(t, err)
		err = r.Close()
		assert.NoError(t, err)

		f, err := os.OpenFile(testFile, os.O_RDWR, walFilePermPrivateRW)
		assert.NoError(t, err)
		_, err = f.Seek(offset, io.SeekStart)
		assert.NoError(t, err)
		b := []byte{0, 1, 2, 3, 4, 5, 6, 7}
		_, err = f.Write(b)
		assert.NoError(t, err)
		err = f.Close()
		assert.NoError(t, err)

		r, err = NewLogRecordReader(logger, testFile)
		assert.NoError(t, err)
		assert.NotNil(t, r)

		assertReadRecord(t, r, rec1, crc1)
		assertReadRecord(t, r, rec2, crc2)

		record, err := r.Read()
		assert.Contains(t, err.Error(), "wal: failed to unmarshal payload")
		assert.Nil(t, record)
	})
}

func assertReadRecord(t *testing.T, r *LogRecordReader, expRec *smartbftprotos.LogRecord, expCRC uint32) {
	record, err := r.Read()
	assert.NoError(t, err)
	assert.NotNil(t, record)
	assert.True(t, proto.Equal(record, expRec))
	assert.Equal(t, expCRC, r.CRC())
}
