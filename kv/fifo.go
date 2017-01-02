package kv

import (
	"sync"
	"errors"
)

type fifo struct {
	mu sync.Mutex

	offset           uint64
	emptyBytes       uint64
	correctionFactor uint64
	array            []byte
}

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

func (f *fifo) drop(item []byte, pos uint64) {
	dataLen := uint64(len(item)) + 1
	end := pos + dataLen
	garb := make([]byte, dataLen)
	copy(f.array[pos:end], garb)
	if pos == f.emptyBytes {
		f.emptyBytes += dataLen
	}
}

func (f *fifo) pushBack(item []byte) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.add(item)
}

func (f *fifo) moveToBack(item []byte, pos uint64) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.drop(item, pos)
	return f.add(item)
}

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
	f.drop(item, f.emptyBytes)
	return ret
}
