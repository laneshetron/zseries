package zseries

import (
	"math/rand"
	"testing"
)

func BenchmarkTestData(b *testing.B) {
	// XXX Should write to tmp dir and purge after
	z := NewZSeries()
	defer z.Close()
	for n := 0; n < b.N; n++ {
		size := rand.Intn(1024)
		randBytes := make([]byte, size)
		_, err := rand.Read(randBytes)
		if err != nil {
			panic(err)
		}
		_, err = z.Write("testdata", randBytes)
		if err != nil {
			panic(err)
		}
	}
}
