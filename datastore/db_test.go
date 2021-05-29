package datastore

import (
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"testing"
)

func TestDb_Put(t *testing.T) {
	dir, err := ioutil.TempDir("", "test-db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := NewDb(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	pairs := [][]string{
		{"key1", "value1"},
		{"key2", "value2"},
		{"key3", "value3"},
	}

	pairsInt64 := [][]string{
		{"kek1", "111"},
		{"kek2", "222"},
		{"kek3", "333"},
	}

	outFile, err := os.Open(db.segments[0].file.Name())
	if err != nil {
		t.Fatal(err)
	}

	t.Run("put/get", func(t *testing.T) {
		for _, pair := range pairs {
			err := db.Put(pair[0], pair[1])
			if err != nil {
				t.Errorf("Cannot put %s: %s", pairs[0], err)
			}
			value, err := db.Get(pair[0])
			if err != nil {
				t.Errorf("Cannot get %s: %s", pairs[0], err)
			}
			if value != pair[1] {
				t.Errorf("Bad value returned. Expected %s, got %s", pair[1], value)
			}
		}
	})

	t.Run("putInt64/getInt64", func(t *testing.T) {
		for _, pair := range pairsInt64 {
			val, err := strconv.ParseInt(pair[1], 10, 64)
			if err != nil {
				log.Fatal(err)
			}
			err = db.PutInt64(pair[0], val)
			if err != nil {
				t.Errorf("Cannot put %s: %s", pairs[0], err)
			}
			value, err := db.GetInt64(pair[0])
			if err != nil {
				t.Errorf("Cannot get %s: %s", pairs[0], err)
			}
			if value != val {
				t.Errorf("Bad value returned. Expected %s, got %s", pair[1], strconv.FormatInt(value, 10))
			}
		}
	})

	t.Run("getInt64wrongtype", func(t *testing.T) {
		notInt64pair := pairs[0]
		res, err := db.GetInt64(notInt64pair[0])
		if err == nil {
			t.Errorf("Expected error for key %s, but got %s", notInt64pair[0], strconv.FormatInt(res, 10))
		}
	})

	outInfo, err := outFile.Stat()
	if err != nil {
		t.Fatal(err)
	}
	size1 := outInfo.Size()

	t.Run("file growth", func(t *testing.T) {
		for _, pair := range pairs {
			err := db.Put(pair[0], pair[1])
			if err != nil {
				t.Errorf("Cannot put %s: %s", pairs[0], err)
			}
		}
		for _, pair := range pairsInt64 {
			val, err := strconv.ParseInt(pair[1], 10, 64)
			if err != nil {
				log.Fatal(err)
			}
			err = db.PutInt64(pair[0], val)
			if err != nil {
				t.Errorf("Cannot put %s: %s", pairs[0], err)
			}
		}
		outInfo, err := outFile.Stat()
		if err != nil {
			t.Fatal(err)
		}
		if size1*2 != outInfo.Size() {
			t.Errorf("Unexpected size (%d vs %d)", size1, outInfo.Size())
		}
	})

	t.Run("new db process", func(t *testing.T) {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		db, err = NewDb(dir)
		if err != nil {
			t.Fatal(err)
		}

		for _, pair := range pairs {
			value, err := db.Get(pair[0])
			if err != nil {
				t.Errorf("Cannot get %s: %s", pairs[0], err)
			}
			if value != pair[1] {
				t.Errorf("Bad value returned. Expected %s, got %s", pair[1], value)
			}
		}
		for _, pair := range pairsInt64 {
			val, _ := strconv.ParseInt(pair[1], 10, 64)
			value, err := db.GetInt64(pair[0])
			if err != nil {
				t.Errorf("Cannot get %s: %s", pairs[0], err)
			}
			if value != val {
				t.Errorf("Bad value returned. Expected %s, got %s", pair[1], strconv.FormatInt(value, 10))
			}
		}
	})

}
