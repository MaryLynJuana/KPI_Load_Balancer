package datastore

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
)

var ErrNotFound = fmt.Errorf("record does not exist")
var ErrBiggerThanSegment = fmt.Errorf("record bigger than segment size")

type hashIndex map[string]uint64
type segmentIndex map[string]uint64

const segmentSize = 1024 * 1024 * 10 // 10 MB

type Db struct {
	path     string       // directory for segments
	segIndex segmentIndex // hash table of elements in segments
	segments []*Segment   // array of segments(files)
	mutex    *sync.Mutex
	channel  chan []byte
}

// Creates new DB
func NewDb(dir string) (*Db, error) {
	s := new(Segment)
	s, err := s.Create(segmentSize, filepath.Join(dir, "0"))
	if err != nil {
		return nil, err
	}
	var mut = &sync.Mutex{}

	db := &Db{
		path:     dir,
		segIndex: make(segmentIndex),
		segments: []*Segment{s},
		mutex:    mut,
	}

	err = db.recover()
	if err != nil && err != io.EOF {
		return nil, err
	}
	return db, nil
}

const bufSize = 8192

func (db *Db) recover() error {
	for _, segment := range db.segments {
		input, err := os.Open(segment.file.Name())
		if err != nil {
			return err
		}
		defer input.Close()

		var buf [bufSize]byte
		in := bufio.NewReaderSize(input, bufSize)
		for err == nil {
			var (
				header, data []byte
				n            int
			)
			header, err = in.Peek(bufSize)
			if err == io.EOF {
				if len(header) == 0 {
					return err
				}
			} else if err != nil {
				return err
			}
			size := binary.LittleEndian.Uint32(header)

			if size < bufSize {
				data = buf[:size]
			} else {
				data = make([]byte, size)
			}
			n, err = in.Read(data)

			if err == nil {
				if n != int(size) {
					return fmt.Errorf("corrupted file")
				}

				var e entry
				e.Decode(data)
				db.mutex.Lock()
				db.segIndex[e.key], _ = strconv.ParseUint(filepath.Base(segment.file.Name()), 10, 64)
				segment.index[e.key] = segment.offset
				segment.offset += uint64(n)
				db.mutex.Unlock()
			}
		}
		return err
	}
	return nil
}

func (db *Db) Close() error {
	for _, segment := range db.segments {
		err := segment.file.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func (db *Db) Get(key string) (string, error) {
	db.mutex.Lock()
	segmentIndex, ok := db.segIndex[key]
	if !ok {
		db.mutex.Unlock()
		return "", ErrNotFound
	}

	value, err := db.segments[segmentIndex].Get(key)
	db.mutex.Unlock()
	if err != nil {
		return "", err
	}
	return value, nil
}

func (db *Db) GetInt64(key string) (int64, error) {
	db.mutex.Lock()
	stringVal, err := db.Get(key)
	if err != nil {
		db.mutex.Unlock()
		return 0, err
	}
	value, err := strconv.ParseInt(stringVal, 10, 64)
	db.mutex.Unlock()
	if err != nil {
		return 0, fmt.Errorf("wrong type of value")
	}
	return value, nil
}

func (db *Db) Put(key, value string) error {
	e := entry{
		key:   key,
		vtype: "string",
		value: value,
	}
	encodedEntry := e.Encode()

	if len(encodedEntry) > segmentSize {
		return ErrBiggerThanSegment
	}

	// The segment to write to
	segment := db.segments[len(db.segments)-1]

	db.mutex.Lock()
	err := segment.Put(key, encodedEntry)
	db.mutex.Unlock()
	if err != nil {
		if err == io.EOF {
			// if the segment ran out of memory - create a new one
			fileName := strconv.Itoa(len(db.segments))
			s := new(Segment)
			db.mutex.Lock()
			s, err := s.Create(segmentSize, filepath.Join(db.path, fileName))
			db.mutex.Unlock()
			if err != nil {
				return err
			}
			db.segments = append(db.segments, s)

			// and try to write again
			db.Put(key, value)
		} else {
			return err
		}
	}
	db.mutex.Lock()
	db.segIndex[key], err = strconv.ParseUint(filepath.Base(segment.file.Name()), 10, 64)
	db.mutex.Unlock()
	return err
}

func (db *Db) PutInt64(key string, value int64) error {
	e := entry{
		key:   key,
		vtype: "int64",
		value: strconv.FormatInt(value, 10),
	}
	encodedEntry := e.Encode()

	if len(encodedEntry) > segmentSize {
		return ErrBiggerThanSegment
	}

	// The segment to write to
	db.mutex.Lock()
	segment := db.segments[len(db.segments)-1]
	db.mutex.Unlock()

	err := db.segments[len(db.segments)-1].Put(key, encodedEntry)
	if err != nil {
		if err == io.EOF {
			// if the segment ran out of memory - create a new one
			fileName := strconv.Itoa(len(db.segments))
			s := new(Segment)
			db.mutex.Lock()
			s, err := s.Create(segmentSize, filepath.Join(db.path, fileName))
			db.mutex.Unlock()
			if err != nil {
				return err
			}
			db.segments = append(db.segments, s)

			// and try to write again
			db.PutInt64(key, value)
		} else {
			return err
		}
	}
	db.mutex.Lock()
	db.segIndex[key], err = strconv.ParseUint(filepath.Base(segment.file.Name()), 10, 64)
	db.mutex.Unlock()
	return err
}
