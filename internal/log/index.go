package log

import (
	"io"
	"os"

	mmap "github.com/edsrzf/mmap-go"
)

// widths define the number of bytes that make up each index entry
// offset = nth item
// position = where nth record can be found in the store file.
var (
	offWidth uint64 = 4
	posWidth uint64 = 8
	entWidth        = offWidth + posWidth
)

//index defines the index file, which comporises of a persisted file and a memory-mapped file.
//Size is the size of the index and where to write the next entry appended to the index

type index struct {
	file *os.File
	mmap mmap.MMap
	size uint64
}

// newIndex(*os.File) creates an index for the given file. Updates current size of the index to track the amount of data in the index file as entries are added.
func newIndex(f *os.File, c Config) (*index, error) {
	idx := &index{
		file: f,
	}

	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	idx.size = uint64(fi.Size())
	if err = os.Truncate(
		f.Name(), int64(c.Segment.MaxIndexBytes),
	); err != nil {
		return nil, err
	}
	//memory map the file with read-write access
	if idx.mmap, err = mmap.Map(f, mmap.RDWR, 0); err != nil {
		return nil, err
	}
	return idx, nil
}

// Read(int64) takes in an offset and returns the associated record's position in the store file.
func (i *index) Read(in int64) (out uint32, pos uint64, err error) {
	if i.size == 0 {
		return 0, 0, io.EOF
	}
	//if -1 assume getting last record set.
	if in == -1 {
		out = uint32((i.size / entWidth) - 1)
	} else {
		out = uint32(in)
	}
	//find the record in the index file. 12 bytes total
	pos = uint64(out) * entWidth
	if i.size < pos+entWidth {
		return 0, 0, io.EOF
	}

	//get offset (out) of record; get position of record in store file.
	//decode as int32 the first 4 bytes to get offset.
	out = enc.Uint32(i.mmap[pos : pos+offWidth])
	// decode as int64 the last 8 bytes of the record to get position in store file
	pos = enc.Uint64(i.mmap[pos+offWidth : pos+entWidth])

	return out, pos, nil
}

// Write(off uint32, pos uint32) appends the given offset and position to the index.
func (i *index) Write(off uint32, pos uint64) error {
	//validate space to write entry
	if uint64(len(i.mmap)) < i.size+entWidth {
		return io.EOF
	}
	enc.PutUint32(i.mmap[i.size:i.size+offWidth], off)
	enc.PutUint64(i.mmap[i.size+offWidth:i.size+entWidth], pos)
	i.size += uint64(entWidth)
	return nil
}

func (i *index) Name() string {
	return i.file.Name()
}

// Close() ensures memory-mapped file has synced its data to the persisted file
// and that the persisted file has flushed its content to stable storage.
// Close() then truncates the persisted file to the amount of data that's actually in it and closes the file.
func (i *index) Close() error {
	//ensure changes are written to the underlying file
	if err := i.mmap.Flush(); err != nil {
		return err
	}
	//Sync file to ensure all pending data in systems's internal buffer for the file are written to the disk
	if err := i.file.Sync(); err != nil {
		return err
	}
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}
	return i.file.Close()
}
