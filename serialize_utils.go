package kafka_client

import "encoding/binary"

func readInt32Array(in []byte, pos int) ([]int32, int) {
	result := make([]int32, 0)
	arrayLen, pos := readInt32(in, pos)
	var number int32
	for i := 0; i < int(arrayLen); i++ {
		number, pos = readInt32(in, pos)
		result = append(result, number)
	}
	return result, pos
}

func readInt64Array(in []byte, pos int) ([]int64, int) {
	result := make([]int64, 0)
	arrayLen, pos := readInt32(in, pos)
	var number int64
	for i := 0; i < int(arrayLen); i++ {
		number, pos = readInt64(in, pos)
		result = append(result, number)
	}
	return result, pos
}

func int8ToBytes(in int8) []byte {
	return []byte{byte(in)}
}

func int16ToBytes(in int16) []byte {
	result := make([]byte, 2)
	binary.BigEndian.PutUint16(result, uint16(in))
	return result
}

func int32ToBytes(in int32) []byte {
	result := make([]byte, 4)
	binary.BigEndian.PutUint32(result, uint32(in))
	return result
}

func int64ToBytes(in int64) []byte {
	result := make([]byte, 8)
	binary.BigEndian.PutUint64(result, uint64(in))
	return result
}

func stringToBytes(in string) []byte {
	result := []byte(in)
	s := int16ToBytes(int16(len(in)))
	result = append(s, result...)
	return result
}

func bytesToBytes(in []byte) []byte {
	result := in
	s := int32ToBytes(int32(len(in)))
	result = append(s, result...)
	return result
}


