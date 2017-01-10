// +build darwin
package partition

import "time"

func clockMonotonicRaw() int64 {
	// OSX doesn't have a monotonic clock, directly at least. We use 'wall clock' under that operating system.
	// This can be dangerous at production. Please use Linux or FreeBSD to deploy something important!
	return time.Now().UnixNano()
}
