package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"
	"strconv"
	"strings"
	"errors"

	"github.com/MaryLynJuana/KPI_Load_Balancer/httptools"
	"github.com/MaryLynJuana/KPI_Load_Balancer/signal"
)

var (
	port = flag.Int("port", 8090, "load balancer port")
	timeoutSec = flag.Int("timeout-sec", 3, "request timeout time in seconds")
	https = flag.Bool("https", false, "whether backends support HTTPs")

	traceEnabled = flag.Bool("trace", false, "whether to include tracing information into responses")
)

var (
	timeout = time.Duration(*timeoutSec) * time.Second
	serversPool = []string{
		"server1:8080",
		"server2:8080",
		"server3:8080",
	}
	serverHealths = []bool{ true, true, true, }
)

func scheme() string {
	if *https {
		return "https"
	}
	return "http"
}

func health(dst string) bool {
	ctx, _ := context.WithTimeout(context.Background(), timeout)
	req, _ := http.NewRequestWithContext(ctx, "GET",
		fmt.Sprintf("%s://%s/health", scheme(), dst), nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	if resp.StatusCode != http.StatusOK {
		return false
	}
	return true
}

func forward(dst string, rw http.ResponseWriter, r *http.Request) error {
	ctx, _ := context.WithTimeout(r.Context(), timeout)
	fwdRequest := r.Clone(ctx)
	fwdRequest.RequestURI = ""
	fwdRequest.URL.Host = dst
	fwdRequest.URL.Scheme = scheme()
	fwdRequest.Host = dst

	resp, err := http.DefaultClient.Do(fwdRequest)
	if err == nil {
		for k, values := range resp.Header {
			for _, value := range values {
				rw.Header().Add(k, value)
			}
		}
		if *traceEnabled {
			rw.Header().Set("lb-from", dst)
		}
		log.Println("fwd", resp.StatusCode, resp.Request.URL)
		rw.WriteHeader(resp.StatusCode)
		defer resp.Body.Close()
		_, err := io.Copy(rw, resp.Body)
		if err != nil {
			log.Printf("Failed to write response: %s", err)
		}
		return nil
	} else {
		log.Printf("Failed to get response from %s: %s", dst, err)
		rw.WriteHeader(http.StatusServiceUnavailable)
		return err
	}
}

func hashAddress(addr string) int {
	ha := strings.Split(strings.Join(strings.Split(addr, "."), ""), ":")[0]
	hs, err := strconv.Atoi(ha)
	if (err != nil) {
		panic(err)
	}
	return hs
}

func filterHealthy() []string {
	healthyServersPool := []string{}
	for i := range serversPool {
		if (serverHealths[i] == true) {
			healthyServersPool = append(healthyServersPool, serversPool[i])
		}
	}
	return healthyServersPool
}

func balanceRequest(addr string) (string, error) {
	healthyServersPool := filterHealthy()
	if (len(healthyServersPool) == 0) {
		return "", errors.New("No servers available")
	}
	addrHash := hashAddress(addr)
	serverIndex := addrHash % len(healthyServersPool)
	//log.Println(addr, serverIndex, healthyServersPool[serverIndex])
	return healthyServersPool[serverIndex], nil
}

func handleRequest(rw http.ResponseWriter, r *http.Request) {
	server, err := balanceRequest(r.RemoteAddr)
	if (err != nil) {
		rw.WriteHeader(http.StatusServiceUnavailable)
		_, _ = rw.Write([]byte("FAILURE"))
		return
	}
	forward(server, rw, r)
}

func main() {
	flag.Parse()
	for i, server := range serversPool {
		server := server
		go func() {
			for range time.Tick(10 * time.Second) {
				serverHealths[i] = health(server)
				log.Println(server, health(server))
			}
		}()
	}

	frontend := httptools.CreateServer(*port, http.HandlerFunc(handleRequest))

	log.Println("Starting load balancer...NYA!")
	log.Printf("Tracing support enabled: %t", *traceEnabled)
	frontend.Start()
	signal.WaitForTerminationSignal()
}
