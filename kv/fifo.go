package kv

import (
	"errors"
	"sync"
)

// fifo defines a struct which provides a fifo queue implementation.
// It's backed by an array which's created by mmap(2) syscall.
// So the allocated memory by fifo is managed by hand. Please notice
// that the mmap(2) backed array is not backed by any file. Its content
// is initialized to zero. For more info, see malloc function and mmap(2)
// man page.
type fifo struct {
	mu sync.Mutex

	offset           uint64
	emptyBytes       uint64
	correctionFactor uint64
	array            []byte
}

// newFIFO creates and returns a new fifo queue.
func newFIFO() (*fifo, error) {
	// Allocate 2KB initially.
	array, err := malloc(2048)
	if err != nil {
		return nil, err
	}
	return &fifo{
		array: array,
	}, nil
}

// resize creates a new memory-mapped file in memory and moves data to the new one.
func (f *fifo) resize() error {
	newArray, err := malloc(len(f.array[f.emptyBytes:]) * 2)
	if err != nil {
		return err
	}
	copy(newArray[0:], f.array[f.emptyBytes:])
	err = free(f.array)
	if err != nil {
		return err
	}
	f.array = newArray
	f.correctionFactor = f.emptyBytes
	f.emptyBytes = 0
	return nil
}

// add adds a new item to the fifo instance.
func (f *fifo) add(item []byte) (uint64, error) {
	itemLen := uint64(len(item))
	if itemLen >= 256 {
		return 0, errors.New("item is too long")
	}
	dataLen := itemLen + 1
	end := f.offset + dataLen
	if uint64(len(f.array)) <= end {
		err := f.resize()
		if err != nil {
			return 0, err
		}
	}
	sizeHeader := []byte{uint8(itemLen)}
	copy(f.array[f.offset:f.offset+1], sizeHeader)
	copy(f.array[f.offset+1:end], item)
	pos := f.offset
	f.offset += dataLen
	return pos + f.correctionFactor, nil
}

// remove drops an element from fifo with given item and pos.
func (f *fifo) remove(item []byte, pos uint64) {
	dataLen := uint64(len(item)) + 1
	end := pos + dataLen
	garb := make([]byte, dataLen)
	copy(f.array[pos:end], garb)
	if pos == f.emptyBytes {
		f.emptyBytes += dataLen
	}
}

// pushBack adds a fresh item to the fifo instance.
func (f *fifo) pushBack(item []byte) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.add(item)
}

// moveToBack removes the given item with the given pos and adds it again to back of the queue.
func (f *fifo) moveToBack(item []byte, pos uint64) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.remove(item, pos)
	return f.add(item)
}

// removes an element from the front of the queue.
func (f *fifo) pop() []byte {
	f.mu.Lock()
	defer f.mu.Unlock()

	rawSize := f.array[f.emptyBytes]
	size := uint64(rawSize)
	if size == 0 {
		return nil
	}
	item := f.array[f.emptyBytes+1 : f.emptyBytes+size+1]
	ret := make([]byte, len(item))
	copy(ret, item)
	f.remove(item, f.emptyBytes)
	return ret
}
