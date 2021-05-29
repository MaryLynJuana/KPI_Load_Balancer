package datastore

import (
	"bufio"
	"os"
)

type Segment struct {
	size   uint64
	file   *os.File
	offset uint64
	index  hashIndex
}

// Creates a segment object and a corresponding file
func (s *Segment) Create(size uint64, path string) (*Segment, error) {
	s = new(Segment)
	s.size = size
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	// defer f.Close() // check if it's necessary
	s.file = f
	s.index = make(hashIndex)
	return s, nil
}

// Gets the value for the key from the segment
func (s *Segment) Get(key string) (string, error) {
	position, ok := s.index[key]
	if !ok {
		return "", ErrNotFound
	}

	file, err := os.Open(s.file.Name())
	if err != nil {
		return "", err
	}
	defer file.Close()

	_, err = file.Seek(int64(position), 0)
	if err != nil {
		return "", err
	}

	reader := bufio.NewReader(file)
	value, err := readValue(reader)
	if err != nil {
		return "", err
	}
	return value, nil
}

// Write an entry to a segment file
func (s *Segment) Put(key string, e []byte) error {
	// If the entry is bigger than segment
	if uint64(len(e)) > s.size {
		return ErrBiggerThanSegment
	}

	n, err := s.file.Write(e)
	if err == nil {
		s.index[key] = s.offset
		s.offset += uint64(n)
	}
	return err
}
