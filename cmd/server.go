package main

import (
	"log"
	"syscall"

	zseries "github.com/laneshetron/zseries/zseries"
	zmq "github.com/pebbe/zmq4"
)

func main() {
	sock, err := zmq.NewSocket(zmq.PULL)
	if err != nil {
		panic(err)
	}
	err = sock.Bind("tcp://*:1337")
	if err != nil {
		panic(err)
	}
	log.Println("ZSeries bound on *:1337")

	z := zseries.NewZSeries()
	defer z.Close()

	for {
		data, err := sock.RecvMessageBytes(0)
		if err != nil {
			switch zmq.AsErrno(err) {
			case zmq.Errno(syscall.EINTR):
				// ignore
			default:
				log.Fatal("Error encountered while reading from socket:", err)
			}
		}
		if len(data) == 2 {
			_, err = z.Write(string(data[0]), data[1])
			if err != nil {
				log.Fatal("Error while writing to ZSeries:", err)
			}
		}
	}
}
