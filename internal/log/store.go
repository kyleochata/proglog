package log

/*
	Record - data stored in log
	Store - file to keep records in
	Index - file to keep index entries
	Segment - abstraction that ties a Store and Index together
	Log - abstraction that ties a store and index together
*/
import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

// enc defines the encoding that persist record sizes and index entries in.
var (
	enc = binary.BigEndian
)

// lenWidth defines the number of bytes used to store the record's length.
const (
	lenWidth = 8
)

// store is a wrapper around a file with two APIs to append and read bytes to and from the file.
type store struct {
	*os.File
	mu   sync.Mutex //don't use RWMutex. buf.Flush() needs a RW lock
	buf  *bufio.Writer
	size uint64
}

// newStore creates a store for the given file. Get files size with os.Stat in case of recreating the store from a file that has existing data.
func newStore(f *os.File) (*store, error) {
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	size := uint64(fi.Size())
	return &store{
		File: f,
		size: size,
		buf:  bufio.NewWriter(f),
	}, nil
}

// Append([]byte) persists the given bytes to the store. Returns number of bytes written and the position where the Store holds the Record in its file. Segment will use this position when it creates an associated index entry for this record.
// binary.Write - writes the length of []byte as a uint64(8 bytes) to s.buf.
// after s.buf.Write(p); s.buf -- [Length of p (8 bytes)][Content of p (len(p) bytes)]
func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pos = s.size
	if err := binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		return 0, 0, err
	}
	w, err := s.buf.Write(p) //write to the buffered writer to reduce num of sys calls
	if err != nil {
		return 0, 0, nil
	}
	w += lenWidth
	s.size += uint64(w)
	return uint64(w), pos, nil
}

// Read(uint64) returns the record stored at the given position.
func (s *store) Read(pos uint64) ([]byte, error) {
	// s.mu.Lock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}
	size := make([]byte, lenWidth)
	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}
	b := make([]byte, enc.Uint64(size))
	if _, err := s.File.ReadAt(b, int64(pos+lenWidth)); err != nil {
		return nil, err
	}
	return b, nil
}

// ReadAt(p []byte, off int64) reads len(p) bytes into p beginning at the off offset in the store's file
func (s *store) ReadAt(p []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return 0, err
	}
	return s.File.ReadAt(p, off)
}

// Close persists any buffered data before closing the file.
func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.buf.Flush()
	if err != nil {
		return err
	}
	return s.File.Close()
}
