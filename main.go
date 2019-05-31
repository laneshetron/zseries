package main

import (
	"C"
	"github.com/laneshetron/zseries"
)

var z zseries.ZSeries

func init() {
	z = zseries.ZSeries{}
}

//export Write
func Write(key string, data []byte) int {
	i, _ := z.Write(key, data)
	return i
}

func main() {}
