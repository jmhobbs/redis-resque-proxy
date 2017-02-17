package main

import (
	"flag"
	"fmt"
	"net"
	"os"

	log "github.com/Sirupsen/logrus"
)

var localAddr *string = flag.String("listen", "127.0.0.1:9000", "local address")
var remoteAddr *string = flag.String("redis", "127.0.0.1:6379", "redis address")
var authKey *string = flag.String("auth", "", "AUTH password")

func handleConn(in <-chan *net.TCPConn, out chan<- *net.TCPConn) {
	for conn := range in {
		proxyConn(conn)
		out <- conn
	}
}

func closeConn(in <-chan *net.TCPConn) {
	for conn := range in {
		conn.Close()
	}
}

func init() {
	log.SetOutput(os.Stdout)
	log.SetLevel(log.InfoLevel)
}

func main() {
	flag.Parse()

	fmt.Printf("Listening On: %v\n Proxying To: %v\n\n", *localAddr, *remoteAddr)

	addr, err := net.ResolveTCPAddr("tcp", *localAddr)
	if err != nil {
		panic(err)
	}

	listener, err := net.ListenTCP("tcp", addr)
	if err != nil {
		panic(err)
	}

	pending, complete := make(chan *net.TCPConn), make(chan *net.TCPConn)

	for i := 0; i < 5; i++ {
		go handleConn(pending, complete)
	}
	go closeConn(complete)

	for {
		conn, err := listener.AcceptTCP()
		if err != nil {
			panic(err)
		}
		pending <- conn
	}
}
