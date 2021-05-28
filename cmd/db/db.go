package main

import (
	"net/http"
	"flag"
	"encoding/json"
	"strconv"
	"log"
	"strings"
	"io/ioutil"

	"github.com/MaryLynJuana/KPI_Load_Balancer/httptools"
	"github.com/MaryLynJuana/KPI_Load_Balancer/datastore"
	"github.com/MaryLynJuana/KPI_Load_Balancer/signal"
)

type valueString struct {
	Value string `json:"value"`
}
type valueInt64 struct {
	Value int64 `json:"value"`
}

var port = flag.Int("port", 8079, "database port")

func main() {
	db, err := datastore.NewDb("/tmp")
	if err != nil {
		log.Fatalf("Error creating database: %s", err)
	}

	h := new(http.ServeMux)

	h.HandleFunc("/db/", func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("content-type", "application/json")
		key := strings.Split(r.URL.Path, "/")[1]

		if r.Method == "POST" {
			body, err := ioutil.ReadAll(r.Body)
			if err != nil {
				log.Fatalf("Error reading request data: %s", err)
				rw.WriteHeader(http.StatusBadRequest)
				return
			}
			var vi valueInt64
			err = json.Unmarshal(body, &vi)
			if err == nil {
				log.Println(strconv.FormatInt(vi.Value, 10))
				err = db.PutInt64(key, vi.Value)
				if err != nil {
					rw.WriteHeader(http.StatusNotFound)
					return
				}
			} else {

				var vs valueString
				err = json.Unmarshal(body, &vs)
				log.Println(vs.Value)
				if err != nil {
					log.Fatalf("Error decoding request data: %s", err)
					rw.WriteHeader(http.StatusBadRequest)
					return
				}
				err = db.Put(key, vs.Value)
				if err != nil {
					rw.WriteHeader(http.StatusNotFound)
					return
				}
			}
			rw.WriteHeader(http.StatusOK)
		} else if r.Method == "GET" {
			t := "string"
			types, ok := r.URL.Query()["type"]
			if ok && len(types[0]) > 0 {
				t = types[0]
			}
			if t == "int64" {
				value, err := db.GetInt64(key)
				if err != nil {
					rw.WriteHeader(http.StatusNotFound)
					return
				}
				rw.WriteHeader(http.StatusOK)
				_ = json.NewEncoder(rw).Encode(struct {
					Key string `json:"key"`
					Type string `json:"type"`
					Value int64 `json:"value"`
				} {
					Key: key,
					Type: t,
					Value: value,
				})
			} else {
				value, err := db.Get(key)
				if err != nil {
					rw.WriteHeader(http.StatusNotFound)
					return
				}
				rw.WriteHeader(http.StatusOK)
				_ = json.NewEncoder(rw).Encode(struct {
					Key string `json:"key"`
					Type string `json:"type"`
					Value string `json:"value"`
				} {
					Key: key,
					Type: t,
					Value: value,
				})
			}
		}
	})
	server := httptools.CreateServer(*port, h)
	server.Start()
	signal.WaitForTerminationSignal()
}