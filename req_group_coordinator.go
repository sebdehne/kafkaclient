package kafka_client

func ParseGroupCoordinatorResponse(in []byte, header ResponseMessage) GroupCoordinatorResponse {
	var nextElementPos int

	var errorCode int16
	var coordinatorId, coordinatorPort int32
	var coordinatorHost string

	errorCode, nextElementPos = readInt16(in, nextElementPos)
	coordinatorId, nextElementPos = readInt32(in, nextElementPos)
	coordinatorHost, nextElementPos = readString(in, nextElementPos)
	coordinatorPort, nextElementPos = readInt32(in, nextElementPos)

	return GroupCoordinatorResponse{
		Header:header,
		ErrorCode:errorCode,
		CoordinatorId:coordinatorId,
		CoordinatorHost:coordinatorHost,
		CoordinatorPort:coordinatorPort}
}

type GroupCoordinatorRequest struct {
	Header  RequestMessage
	GroupId string
}

func (r GroupCoordinatorRequest) Bytes() []byte {
	buf := r.Header.headerToBytes(10, 0)

	buf.Write(stringToBytes(r.GroupId))

	return buf.Bytes()
}

type GroupCoordinatorResponse struct {
	Header          ResponseMessage
	ErrorCode       int16
	CoordinatorId   int32
	CoordinatorHost string
	CoordinatorPort int32
}
