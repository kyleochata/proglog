package log

//widths define the number of bytes that make up each index entry
var (
	offWidth uint64 = 4
	posWidth uint64 = 8
	entWidth        = offWidth + posWidth
)
