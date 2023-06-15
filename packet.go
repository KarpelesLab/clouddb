package clouddb

const (
	PktGetInfo     = 0x00 // GetInfo()
	PktGetLogIds   = 0x01 // GetLogs(<peer>, <epoch>)
	PktFetchLog    = 0x02 // FetchLog(<key>) uses no response packet id, response is always inline
	PktGetInfoResp = 0x80 // GetInfo()|Response
	PktLogPush     = 0x40 // pushed log entry
	PktLogIdsPush  = 0x41 // LogIdPush(<peer>, ...<logid>)

	ResponseFlag = 0x80 // response flag
)
