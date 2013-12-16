package main

import (
	"fmt"
)

func main() {
	var m map[string]bool
	m = make(map[string]bool)
	m["1"] = true
	m["3"] = true
	for k := range m {
		fmt.Println(k)
	}
}
