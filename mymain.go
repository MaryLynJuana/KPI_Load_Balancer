package main

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

type segmentIndex map[string]uint32
type hashIndex map[string]int64

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
	f.Close() // check if it's necessary
	if err != nil {
		return nil, err
	}
	s.file = f
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

	_, err = file.Seek(position, 0)
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

const segmentSize = 8192

type Db struct {
	path     string
	segIndex segmentIndex
	segments []*Segment
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
	input, err := os.Open(db.out[len(db.out)-1].Name())
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
			db.segIndex[e.key] = db.outOffset
			db.outOffset += int64(n)
		}
	}
	return err
}

func (db *Db) Close() error {
	for _, file := range db.out {
		err := file.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func (db *Db) Get(key string) (string, error) {
	// position, ok := db.segIndex[key]
	// if !ok {
	return "", ErrNotFound
	// }

	// for _, file := range db.out {

	// }

	// file, err := os.Open(db.outPath)
	// if err != nil {
	// 	return "", err
	// }
	// defer file.Close()

	// _, err = file.Seek(position, 0)
	// if err != nil {
	// 	return "", err
	// }

	// reader := bufio.NewReader(file)
	// value, err := readValue(reader)
	// if err != nil {
	// 	return "", err
	// }
	// return value, nil
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
	n, err := db.out[len(db.out)-1].Write(e.Encode())
	if err == nil {
		db.segIndex[key] = db.outOffset
		db.outOffset += int64(n)
	}
	return err
}

func (db *Db) PutInt64(key string, value int64) error {
	e := entry{
		key:   key,
		vtype: "int64",
		value: strconv.FormatInt(value, 10),
	}
	n, err := db.out[len(db.out)-1].Write(e.Encode())
	if err == nil {
		db.segIndex[key] = db.outOffset
		db.outOffset += int64(n)
	}
	return err
}

type entry struct {
	key, vtype, value string
}

func (e *entry) Encode() []byte {
	kl := len(e.key)
	tl := len(e.vtype)
	vl := len(e.value)
	size := kl + tl + vl + 16
	res := make([]byte, size)
	binary.LittleEndian.PutUint32(res, uint32(size))
	binary.LittleEndian.PutUint32(res[4:], uint32(kl))
	copy(res[8:], e.key)
	binary.LittleEndian.PutUint32(res[kl+8:], uint32(tl))
	copy(res[kl+12:], e.vtype)
	binary.LittleEndian.PutUint32(res[kl+tl+12:], uint32(vl))
	copy(res[kl+tl+16:], e.value)
	return res
}

func (e *entry) Decode(input []byte) {
	kl := binary.LittleEndian.Uint32(input[4:])
	keyBuf := make([]byte, kl)
	copy(keyBuf, input[8:kl+8])
	e.key = string(keyBuf)

	tl := binary.LittleEndian.Uint32(input[kl+8:])
	typeBuf := make([]byte, tl)
	copy(typeBuf, input[kl+12:kl+12+tl])
	e.vtype = string(typeBuf)

	vl := binary.LittleEndian.Uint32(input[kl+tl+12:])
	valBuf := make([]byte, vl)
	copy(valBuf, input[kl+tl+16:kl+tl+16+vl])
	e.value = string(valBuf)
}

// Reads the value in the file based on the headers
// containing the size of key, type, and value put there
// by `Encode()`
func readValue(in *bufio.Reader) (string, error) {
	header, err := in.Peek(8)
	if err != nil {
		return "", err
	}
	keySize := int(binary.LittleEndian.Uint32(header[4:]))
	_, err = in.Discard(keySize + 4)
	if err != nil {
		return "", err
	}

	header, err = in.Peek(8)
	if err != nil {
		return "", err
	}
	typeSize := int(binary.LittleEndian.Uint32(header[4:]))
	_, err = in.Discard(typeSize + 8)
	if err != nil {
		return "", err
	}

	header, err = in.Peek(4)
	if err != nil {
		return "", err
	}

	valSize := int(binary.LittleEndian.Uint32(header))
	_, err = in.Discard(4)
	if err != nil {
		return "", err
	}

	data := make([]byte, valSize)
	n, err := in.Read(data)
	if err != nil {
		return "", err
	}
	if n != valSize {
		return "", fmt.Errorf("can't read value bytes (read %d, expected %d)", n, valSize)
	}
	return string(data), nil
}

func main() {
	e := entry{
		key:   "key",
		vtype: "int64",
		value: strconv.FormatInt(123, 10),
	}
	thing := e.Encode()
	println(len(thing))
	println(thing)
	// println(e.Decode(thing))
	// println(e)
}
