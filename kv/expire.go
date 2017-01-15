package kv

import (
	"encoding/binary"
	"time"
)

const (
	ExpireEnabled  byte = 1
	ExpireDisabled byte = 0
)

func (k *KV) enableExpireOnValue(expireTime int64, value []byte) []byte {
	bd := make([]byte, 8)
	binary.LittleEndian.PutUint64(bd, uint64(expireTime))
	bd = append(bd, []byte{ExpireEnabled}...)
	value = append(value, bd...)
	return value
}

func (k *KV) disableExpireOnValue(value []byte) []byte {
	value = append(value, []byte{ExpireDisabled}...)
	return value
}

func (k *KV) isValueExpired(value []byte) (bool, error) {
	set := value[len(value)-1:]
	if set[0] != ExpireEnabled {
		return false, nil
	}
	now, err := k.time.now()
	if err != nil {
		return false, err
	}
	bd := value[len(value)-9:]
	ttl := binary.LittleEndian.Uint64(bd)
	return int64(ttl) <= now, nil
}

func (k *KV) expireKeysFromKV() {
	defer k.waitGroup.Done()
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	partitionID := 0
	for {
		select {
		case <-ticker.C:
		case <-k.done:
			return
		}
	}
}
