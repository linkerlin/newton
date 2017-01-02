package kv

import "sync"

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

func (f *fifo) add(key []byte) (uint64, error) {
	keyLen := uint64(len(key))
	end := f.offset + keyLen
	if uint64(len(f.array)) <= end {
		err := f.resize()
		if err != nil {
			return 0, err
		}
	}
	copy(f.array[f.offset:end], key)
	f.offset += keyLen
	return f.offset + f.correctionFactor, nil
}

func (f *fifo) delete(key []byte, pos uint64) {
	keyLen := uint64(len(key))
	end := pos + keyLen
	garb := make([]byte, keyLen)
	copy(f.array[f.offset:end], garb)
	if pos == f.emptyBytes {
		f.emptyBytes += pos
	}
}

func (f *fifo) pushBack(key []byte) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.add(key)
}

func (f *fifo) moveToBack(key []byte, pos uint64) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.delete(key, pos)
	return f.add(key)
}
