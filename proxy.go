package main

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/jmhobbs/redis-resque-proxy/redis"
)

func proxyConn(conn *net.TCPConn) {
	cLog := log.WithFields(log.Fields{
		"client": conn.RemoteAddr(),
	})
	cLog.Info("Connect")

	var rConn net.Conn
	var err error

	rConn = nil

	for {
		if rConn == nil {
			cLog.Debug("Dialing Redis")
			d := &net.Dialer{Deadline: time.Now().Add(time.Millisecond * 50)}
			rConn, err = d.Dial("tcp", *remoteAddr)
			if err != nil {
				cLog.Error("Failed to dial Redis")
			} else {
				defer rConn.Close()
			}
		}

		cLog.Info("Waiting for client")
		buf := &bytes.Buffer{}
		for {
			data := make([]byte, 256)
			// TODO: 5s too much?
			//conn.SetReadDeadline(time.Now().Add(time.Second * 5))
			n, err := conn.Read(data)
			if err != nil {
				if err == io.EOF {
					cLog.Info("Disconnect")
					return
				}
				panic(err)
			}
			cLog.WithFields(log.Fields{
				"bytes": n,
			}).Info("Read From Client")
			buf.Write(data[:n])
			if n < 256 {
				break
			}
		}

		c, err := redis.Parse(buf.Bytes())
		if err != nil {
			cLog.Error(err)
		} else {
			cLog.Info(fmt.Sprintf("%s %s", c.Command, c.Arguments))
			if rConn == nil {
				conn.Write(fakeResponse(*c))
				continue
			}
		}

		if rConn == nil {
			continue
		}

		var n int
		var nw int

		cLog.Info("Writing To Redis")
		if n, err = rConn.Write(buf.Bytes()); err != nil {
			panic(err)
		}
		cLog.WithFields(log.Fields{
			"bytes": n,
		}).Info("Wrote Redis")

		for {
			cLog.Info("Waiting for Redis")
			data := make([]byte, 256)
			rConn.SetReadDeadline(time.Now().Add(time.Millisecond * 100))
			n, err = rConn.Read(data)
			if err != nil {
				if err == io.EOF {
					cLog.Error("Redis Disconnected")
					return
				} else if err.(*net.OpError).Timeout() {
					// So, this is kind of weak.  I don't want to parse
					// responses from Redis to know how many bytes to read
					// because that's a pain, so I'm abusing read timeouts
					// and the synchronous nature of the redis protocol
					cLog.Info("Timed out, read complete...?")
					break
				} else {
					cLog.Error(err)
					return
				}
			}
			cLog.WithFields(log.Fields{
				"bytes": n,
			}).Info("Read Redis")

			fmt.Printf("%s", hex.Dump(data[:n]))

			cLog.Info("Writing To Client")
			nw, err = conn.Write(data[:n])
			if err != nil {
				panic(err)
			}

			cLog.WithFields(log.Fields{
				"bytes": nw,
			}).Info("Wrote Client")

			/*
				if n < 256 {
					break
				}
			*/
		}
	}
	cLog.Error("Broke out of loop!")
}
