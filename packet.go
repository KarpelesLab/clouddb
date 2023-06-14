package clouddb

const (
	PktGetInfo     = 0x00 // GetInfo()
	PktGetInfoResp = 0x80 // GetInfo()|Response

	ResponseFlag = 0x80 // response flag
)
