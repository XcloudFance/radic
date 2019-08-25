package util

import (
	"bytes"
	"encoding/binary"
)

func Uint32ToBytes(ele uint32) ([]byte, error) {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.BigEndian, ele)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
