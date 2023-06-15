package clouddb

const (
	PktGetInfo     = 0x00 // GetInfo()
	PktGetLogs     = 0x01 // GetLogs(<peer>, <epoch>, <bloom...>)
	PktGetInfoResp = 0x80 // GetInfo()|Response
	PktLogPush     = 0x40 // pushed log entry

	ResponseFlag = 0x80 // response flag
)
