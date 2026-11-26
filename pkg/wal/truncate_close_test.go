// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0

package wal

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestTruncateCloseFileClosesOnFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")
	if err := os.WriteFile(path, []byte("data"), 0600); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })
	if err := truncateCloseFile(f, 0); err == nil {
		t.Fatal("truncate of read-only file succeeded")
	}
	if _, err := f.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("file remains open after failed truncate: %v", err)
	}
}
