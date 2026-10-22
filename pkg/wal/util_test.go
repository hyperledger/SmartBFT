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

	"github.com/hyperledger/SmartBFT/pkg/api"
	"github.com/hyperledger/SmartBFT/smartbftprotos"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
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

		// repair is good
		err = scanRepairFile(logger, lastFile)
		assert.NoError(t, err)
		names, err = dirReadWalNames(testDir)
		assert.NoError(t, err)
		assert.Equal(t, len(names), 7)
		err = scanVerifyFiles(logger, testDir, names)
		assert.NoError(t, err)
	})
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
