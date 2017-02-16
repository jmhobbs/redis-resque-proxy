package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/jmhobbs/redis-resque-proxy/redis"
)

var localAddr *string = flag.String("listen", "127.0.0.1:9000", "local address")
var remoteAddr *string = flag.String("redis", "127.0.0.1:6379", "redis address")
var authKey *string = flag.String("auth", "", "AUTH password")

// 1. If remote connection is active, run as is
// 2. If it's down, proxy LIST commands and fake empty returns
// 3. Profit?

// TODO: loop on this to prevent disconnects
func proxyConn(conn *net.TCPConn) {
	fmt.Printf("[%s] Connect\n", conn.RemoteAddr())

	d := &net.Dialer{Deadline: time.Now().Add(time.Millisecond * 500)}
	rConn, err := d.Dial("tcp", *remoteAddr)
	if err != nil {
		fmt.Printf("[%s] Failed to dial Redis: %s\n", conn.RemoteAddr(), err)
	} else {
		defer rConn.Close()
	}

	//	fmt.Printf("[%s] Waiting for client\n", conn.RemoteAddr())
	buf := &bytes.Buffer{}
	for {
		data := make([]byte, 256)
		// TODO: 5s too much?
		conn.SetReadDeadline(time.Now().Add(time.Second * 5))
		n, err := conn.Read(data)
		if err != nil {
			panic(err)
		}
		fmt.Printf("[%s] -> %d bytes\n", conn.RemoteAddr(), n)
		buf.Write(data[:n])
		if n < 256 {
			break
		}
	}
	// TODO: Parse command here...
	c, err := redis.Parse(buf.Bytes())
	if err != nil {
		fmt.Printf("[%s] Error parsing command: %v\n", conn.RemoteAddr(), err)
	} else {
		fmt.Printf("[%s] -> %s %s\n", conn.RemoteAddr(), c.Command, c.Arguments)
		if rConn == nil {
			// These are largely pulled from calls here:
			// https://github.com/getflywheel/resque/blob/master/lib/resque/data_store.rb
			switch uc := strings.ToUpper(c.Command); uc {
			case "AUTH":
				if *authKey == "" {
					conn.Write([]byte("-ERR Client sent AUTH, but no password is set\r\n"))
				} else if *authKey == c.Arguments[0] {
					conn.Write([]byte("+OK\r\n"))
				} else {
					conn.Write([]byte("-ERR invalid password\r\n"))
				}
			case "LPOP":
				conn.Write([]byte("$-1\r\n"))
			case "LLEN":
				conn.Write([]byte(":0\r\n"))
			case "BLPOP":
				sleep, err := strconv.ParseInt(c.Arguments[len(c.Arguments)-1], 10, 0)
				if err == nil && sleep > 0 {
					time.Sleep(time.Second * time.Duration(sleep))
				}
				conn.Write([]byte("*-1\r\n"))
			default:
				fmt.Printf("[%s] Unknown Command: %s\n", conn.RemoteAddr(), uc)
				conn.Write([]byte("-ERR Redis Server Unavailable\r\n"))
			}
			return
		}
	}

	if rConn == nil {
		return
	}

	//	fmt.Printf("[%s] Writing to Redis\n", conn.RemoteAddr())
	if _, err := rConn.Write(buf.Bytes()); err != nil {
		panic(err)
	}

	//	fmt.Printf("[%s} Reading from Redis\n", conn.RemoteAddr())

	for {
		data := make([]byte, 256)
		rConn.SetReadDeadline(time.Now().Add(time.Millisecond * 500))
		n, err := rConn.Read(data)
		if err != nil {
			if err != io.EOF {
				panic(err)
			} else {
				fmt.Printf("[%s] Error: %s", conn.RemoteAddr(), err)
			}
		}
		fmt.Printf("[%s] <- %d bytes\n", conn.RemoteAddr(), n)
		fmt.Printf("%s", data[:n])
		conn.Write(data[:n])
		if n < 256 {
			break
		}
	}

}

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
