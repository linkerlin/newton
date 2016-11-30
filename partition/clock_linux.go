package partition

import (
	"syscall"
	"time"
	"unsafe"
)

const ClockMonotonicRaw uintptr = 4

func clockMonotonicRaw() int64 {
	var ts syscall.Timespec
	syscall.Syscall(syscall.SYS_CLOCK_GETTIME, ClockMonotonicRaw, uintptr(unsafe.Pointer(&ts)), 0)
	sec, nsec := ts.Unix()
	return time.Unix(sec, nsec).UnixNano()
}
