//go:build linux

package storage

import (
	"os"
	"syscall"
)

func preAllocate(f *os.File, size int64) {
	syscall.Fallocate(int(f.Fd()), 0x01, 0, size) //nolint:errcheck
}
