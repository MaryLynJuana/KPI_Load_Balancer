package integration

import (
	"fmt"
	"net/http"
	"testing"
	"time"
	"strconv"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
)

const baseAddress = "http://balancer:8090"
const key = "oymate"

var client = http.Client{
	Timeout: 3 * time.Second,
}

func TestBalancer(t *testing.T) {
	for i := 0; i < 3; i++ {
		route := fmt.Sprintf("%s/api/v1/some-data?key=%s", baseAddress, key)
		resp, err := client.Get(route)
		assert.Nil(t, err)
		compare := resp.Header.Get("lb-from")
		for j := 0; j < 5; j++ {
			resp, err = client.Get(route)
			assert.Equal(t, compare, resp.Header.Get("lb-from"))
			body, _ := ioutil.ReadAll(resp.Body)
			assert.NotEmpty(t, string(body))
			assert.Nil(t, err)
		}
	}
}

func BenchmarkBalancer(b *testing.B) {
	var timeForQueries int64 = 0
	iterations := b.N
	for i := 0; i < 3; i++ {
		route := fmt.Sprintf("%s/api/v1/some-data?key=%s", baseAddress, key)
		resp, err := client.Get(route)
		assert.Nil(b, err)
		compare := resp.Header.Get("lb-from")
		for j := 0; j < iterations; j++ {
			start := time.Now()
			resp, err = client.Get(route)
			timeForQueries += time.Since(start).Nanoseconds()
			assert.Equal(b, compare, resp.Header.Get("lb-from"))
			assert.Nil(b, err)
		}
	}
	fmt.Printf("\naverage query time: %s\n", strconv.Itoa(int(timeForQueries)/iterations))
}
