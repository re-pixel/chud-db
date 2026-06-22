//go:build !linux

package storage

import "os"

func preAllocate(f *os.File, size int64) {}
