package main

import (
	"flag"
	"net/http"
	"os"
	"strconv"
	"time"
	"io/ioutil"
	"log"
	"bytes"
	"fmt"

	"github.com/MaryLynJuana/KPI_Load_Balancer/httptools"
	"github.com/MaryLynJuana/KPI_Load_Balancer/signal"
)

var port = flag.Int("port", 8080, "server port")

var db = flag.String("db", "http://database:8079/db/", "database url")

const confResponseDelaySec = "CONF_RESPONSE_DELAY_SEC"
const confHealthFailure = "CONF_HEALTH_FAILURE"

func main() {
	flag.Parse()
	currentDate := time.Now().Format("2006-01-02")
	postBody := []byte(fmt.Sprintf(`{"value": "%s"}`, currentDate))
	reqBody := bytes.NewBuffer(postBody)
	resp, err := http.Post(*db + "oymate", "application/json", reqBody)
	if err != nil {
		log.Fatalf("Error putting data to db: %s", err)
		return
	}
	if resp.StatusCode != http.StatusOK {
		log.Fatalf("Error putting data to db: %s", strconv.Itoa(resp.StatusCode))
	}

	h := new(http.ServeMux)

	h.HandleFunc("/health", func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("content-type", "text/plain")
		if failConfig := os.Getenv(confHealthFailure); failConfig == "true" {
			rw.WriteHeader(http.StatusInternalServerError)
			_, _ = rw.Write([]byte("FAILURE"))
		} else {
			rw.WriteHeader(http.StatusOK)
			_, _ = rw.Write([]byte("OK"))
		}
	})

	report := make(Report)

	h.HandleFunc("/api/v1/some-data", func(rw http.ResponseWriter, r *http.Request) {
		respDelayString := os.Getenv(confResponseDelaySec)
		if delaySec, parseErr := strconv.Atoi(respDelayString); parseErr == nil && delaySec > 0 && delaySec < 300 {
			time.Sleep(time.Duration(delaySec) * time.Second)
		}

		report.Process(r)

		keys, ok := r.URL.Query()["key"]

		if !ok || len(keys[0]) < 1 {
			rw.WriteHeader(http.StatusBadRequest)
			log.Println("Url Param 'key' is missing")
			return
		}
		key := keys[0]

		t := "string"
		types, ok := r.URL.Query()["type"]
		if ok && len(types[0]) > 0 {
			t = types[0]
		}

		log.Println("Key: " + key + ", type: " + t)

		resp, err := http.Get(*db + key + "?type=" + t)
		if err != nil {
			log.Fatalf("Error getting data from db: %s", err)
			rw.WriteHeader(http.StatusInternalServerError)
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			rw.WriteHeader(resp.StatusCode)
			return
		}

		buf, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Fatalf("Error reading response data: %s", err)
			rw.WriteHeader(http.StatusInternalServerError)
			return
		}

		rw.Header().Set("content-type", "application/json")
		rw.WriteHeader(http.StatusOK)
		_, err = rw.Write(buf)
		if err != nil {
			log.Fatalf("Error sending response data: %s", err)
		}
	})

	h.Handle("/report", report)

	server := httptools.CreateServer(*port, h)
	server.Start()
	signal.WaitForTerminationSignal()
}
