package datastore

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
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
}

// Creates new DB
func NewDb(dir string) (*Db, error) {
	s := new(Segment)
	s, err := s.Create(segmentSize, filepath.Join(dir, "0"))
	if err != nil {
		return nil, err
	}

	db := &Db{
		path:     dir,
		segIndex: make(segmentIndex),
		segments: []*Segment{s},
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
				db.segIndex[e.key], _ = strconv.ParseUint(filepath.Base(segment.file.Name()), 10, 64)
				segment.index[e.key] = segment.offset
				segment.offset += uint64(n)
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
	segmentIndex, ok := db.segIndex[key]
	if !ok {
		return "", ErrNotFound
	}

	value, err := db.segments[segmentIndex].Get(key)
	if err != nil {
		return "", err
	}
	return value, nil
}

func (db *Db) GetInt64(key string) (int64, error) {
	stringVal, err := db.Get(key)
	if err != nil {
		return 0, err
	}
	value, err := strconv.ParseInt(stringVal, 10, 64)
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

	err := segment.Put(key, encodedEntry)
	if err != nil {
		if err == io.EOF {
			// if the segment ran out of memory - create a new one
			fileName := strconv.Itoa(len(db.segments))
			s := new(Segment)
			s, err := s.Create(segmentSize, filepath.Join(db.path, fileName))
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
	db.segIndex[key], err = strconv.ParseUint(filepath.Base(segment.file.Name()), 10, 64)
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
	segment := db.segments[len(db.segments)-1]

	err := db.segments[len(db.segments)-1].Put(key, encodedEntry)
	if err != nil {
		if err == io.EOF {
			// if the segment ran out of memory - create a new one
			fileName := strconv.Itoa(len(db.segments))
			s := new(Segment)
			s, err := s.Create(segmentSize, filepath.Join(db.path, fileName))
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
	db.segIndex[key], err = strconv.ParseUint(filepath.Base(segment.file.Name()), 10, 64)
	return err
}

// func main() {
// 	dir, _ := ioutil.TempDir("", "test-db")
// 	defer os.RemoveAll(dir)

// 	db, _ := NewDb(dir)
// 	defer db.Close()

// 	println(dir)
// 	println(db.segments[0].file)

// 	pairs := [][]string{
// 		{"key1", "value1"},
// 		{"key2", "value2"},
// 		{"key3", "value3"},
// 	}

// 	pairsInt64 := [][]string{
// 		{"kek1", "111"},
// 		{"kek2", "222"},
// 		{"kek3", "333"},
// 	}

// 	// put/get simple
// 	for _, pair := range pairs {
// 		err := db.Put(pair[0], pair[1])
// 		if err != nil {
// 			log.Fatal("Cannot put %s: %s", pairs[0], err)
// 		}
// 		value, err := db.Get(pair[0])
// 		if err != nil {
// 			log.Fatal("Cannot get %s: %s", pairs[0], err)
// 		}
// 		if value != pair[1] {
// 			log.Fatal("Bad value returned. Expected %s, got %s", pair[1], value)
// 		}
// 	}

// 	// put/get int64
// 	for _, pair := range pairsInt64 {
// 		val, err := strconv.ParseInt(pair[1], 10, 64)
// 		if err != nil {
// 			log.Fatal(err)
// 		}
// 		err = db.PutInt64(pair[0], val)
// 		if err != nil {
// 			log.Fatal("Cannot put %s: %s", pairs[0], err)
// 		}
// 		value, err := db.GetInt64(pair[0])
// 		if err != nil {
// 			log.Fatal("Cannot get %s: %s", pairs[0], err)
// 		}
// 		if value != val {
// 			log.Fatal("Bad value returned. Expected %s, got %s", pair[1], strconv.FormatInt(value, 10))
// 		}
// 	}

// 	// new process
// 	if err := db.Close(); err != nil {
// 		log.Fatal(err)
// 	}
// 	db, err := NewDb(dir)
// 	if err != nil {
// 		log.Fatal(err)
// 	}

// 	// simple
// 	for _, pair := range pairs {
// 		value, err := db.Get(pair[0])
// 		if err != nil {
// 			log.Fatal("Cannot get %s: %s", pairs[0], err)
// 		}
// 		if value != pair[1] {
// 			log.Fatal("Bad value returned. Expected %s, got %s", pair[1], value)
// 		}
// 	}

// 	// int64
// 	for _, pair := range pairsInt64 {
// 		val, _ := strconv.ParseInt(pair[1], 10, 64)
// 		value, err := db.GetInt64(pair[0])
// 		if err != nil {
// 			log.Fatal("Cannot get %s: %s", pairs[0], err)
// 		}
// 		if value != val {
// 			log.Fatal("Bad value returned. Expected %s, got %s", pair[1], strconv.FormatInt(value, 10))
// 		}
// 	}

// }
