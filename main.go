package main

import (
	"C"
	"github.com/laneshetron/zseries/zseries"
)

var z *zseries.ZSeries

func init() {
	z = zseries.NewZSeries()
}

//export Write
func Write(key string, data []byte) int {
	i, _ := z.Write(key, data)
	return i
}

//export Close
func Close() {
	z.Close()
}

func main() {}
