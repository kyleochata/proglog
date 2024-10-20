package log

import (
	"fmt"
	"os"
	"path"

	api "github.com/kyleochata/proglog/api/v1"
	"google.golang.org/protobuf/proto"
)

// Want to transition the lock from the log to the segment. Reason: only 1 active segment should ever be RW. All other segments only contain Reads because they are at their MaxBytes defined by Config. Only need to lock the Log when creating a new active segment.
type segment struct {
	store      *store
	index      *index
	baseOffset uint64 //relative offset for where this segment begins.
	nextOffset uint64 //absolute offset for position within the entire log
	config     Config
}

// newSegment() is called when the log needs to create a new segment when the current active segment reaches its max size.
func newSegment(dir string, baseOffset uint64, c Config) (*segment, error) {
	s := &segment{
		baseOffset: baseOffset,
		config:     c,
	}

	var err error
	storeFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".store")),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, err
	}
	if s.store, err = newStore(storeFile); err != nil {
		return nil, err
	}

	indexFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".index")),
		os.O_RDWR|os.O_CREATE,
		0644,
	)
	if err != nil {
		return nil, err
	}
	if s.index, err = newIndex(indexFile, c); err != nil {
		return nil, err
	}
	if off, _, err := s.index.Read(-1); err != nil {
		s.nextOffset = baseOffset
	} else {
		s.nextOffset = baseOffset + uint64(off) + 1
	}
	return s, nil
}

// Append() writes the record to the segment and returns the newly appended record's offset. Appends data to the store then to the index.
func (s *segment) Append(record *api.Record) (offset uint64, err error) {
	cur := s.nextOffset
	record.Offset = cur
	p, err := proto.Marshal(record)
	if err != nil {
		return 0, err
	}
	_, pos, err := s.store.Append(p)
	if err != nil {
		return 0, err
	}
	if err = s.index.Write(
		//calc the relative offset of the next record within this segment. nextOffset == absolute offset for entire log. Baseoffset == absolute offset for when this particular segment starts
		uint32(s.nextOffset-uint64(s.baseOffset)),
		pos,
	); err != nil {
		return 0, err
	}
	s.nextOffset++ //increment for next append call
	return cur, nil
}

// Read(off uint64) returns the record for the offset received.
func (s *segment) Read(off uint64) (*api.Record, error) {
	//translate absolute position to relative position within current segment
	_, pos, err := s.index.Read(int64(off - s.baseOffset))
	if err != nil {
		return nil, err
	}
	p, err := s.store.Read(pos)
	if err != nil {
		return nil, err
	}
	record := &api.Record{}
	err = proto.Unmarshal(p, record)
	return record, err
}

// IsMaxed() returns whether the segment has reached its max size. Full index file or store file.
func (s *segment) IsMaxed() bool {
	return s.store.size >= s.config.Segment.MaxStoreBytes || s.index.size >= s.config.Segment.MaxIndexBytes
}

// Remove() closes the segment and removes the index and store files
func (s *segment) Remove() error {
	if err := s.Close(); err != nil {
		return err
	}
	if err := os.Remove(s.index.Name()); err != nil {
		return err
	}
	if err := os.Remove(s.store.Name()); err != nil {
		return err
	}
	return nil
}

// Close() closes the index file and store files respectively
func (s *segment) Close() error {
	if err := s.index.Close(); err != nil {
		return err
	}
	if err := s.store.Close(); err != nil {
		return err
	}
	return nil
}

// nearestMultiple(j uint64, k uint64) returns the nearest and lesser multiple of k in j.
// nearestMultiple(9, 4) == 8
func nearestMultiple(j, k uint64) uint64 {
	if j >= 0 {
		return (j / k) * k
	}
	return ((j - k + 1) / k) * k
}
