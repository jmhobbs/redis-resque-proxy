package main

import (
	"flag"
	"fmt"
	"net"
	"os"

	"github.com/Sirupsen/logrus"
)

var localAddr *string = flag.String("listen", "127.0.0.1:9000", "local address")
var remoteAddr *string = flag.String("redis", "127.0.0.1:6379", "redis address")
var authKey *string = flag.String("auth", "", "AUTH password")
var verbose *bool = flag.Bool("verbose", false, "Be noisy.")

func init() {
	logrus.SetOutput(os.Stdout)
	logrus.SetLevel(logrus.InfoLevel)
}

func main() {
	flag.Parse()

	if *verbose {
		logrus.SetLevel(logrus.DebugLevel)
	}
	fmt.Printf("Listening On: %v\n Proxying To: %v\n\n", *localAddr, *remoteAddr)

	addr, err := net.ResolveTCPAddr("tcp", *localAddr)
	if err != nil {
		panic(err)
	}

	listener, err := net.ListenTCP("tcp", addr)
	if err != nil {
		panic(err)
	}

	for {
		conn, err := listener.AcceptTCP()
		if err != nil {
			panic(err)
		}
		go proxyConn(conn)
	}
}
