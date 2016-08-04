package kafka_client

import "encoding/binary"

func readInt16(in []byte, pos int) (int16, int) {
	return int16(binary.BigEndian.Uint16(in[pos:pos + 2])), pos + 2
}

func readInt32(in []byte, pos int) (int32, int) {
	return int32(binary.BigEndian.Uint32(in[pos:pos + 4])), pos + 4
}

func readInt64(in []byte, pos int) (int64, int) {
	return int64(binary.BigEndian.Uint64(in[pos:pos + 8])), pos + 8
}

func readString(in []byte, pos int) (string, int) {
	length, pos := readInt16(in, pos)
	return string(in[pos:pos + int(length)]), pos + int(length)
}
