package main

import (
	"testing"
	"fmt"
)

var (
	baseAddress = "172.19.0."
	expectedServers = []string{
		"server1:8080",
		"server2:8080",
		"server3:8080",
		"server1:8080",
	}
)

func TestBalancer(t *testing.T) {
	// TODO: Реалізуйте юніт-тест для балансувальникка.
	for i := 0; i <= 3; i++ {
		addr := fmt.Sprintf("%s%d", baseAddress, i + 1)
		for j := 0; j <= 3; j++ {
			server, err := balanceRequest(addr)
			if err != nil {
				t.Fatal(err)
			}
			expected := expectedServers[i]
			if server != expected {
				t.Errorf(
					"Balancing algorithm returned wrong server: expected %s, got %s",
					expected, server)
			}
		}
	}
}