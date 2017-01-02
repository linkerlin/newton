package kv

import "syscall"

func malloc(size int) ([]byte, error) {
	flags := syscall.MAP_ANON | syscall.MAP_PRIVATE
	prot := syscall.PROT_READ | syscall.PROT_WRITE
	array, err := syscall.Mmap(-1, 0, size, prot, flags)
	if err != nil {
		return nil, err
	}
	return array, nil
}

func free(array []byte) error {
	return syscall.Munmap(array)
}
