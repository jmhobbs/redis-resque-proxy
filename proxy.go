package main

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"github.com/Sirupsen/logrus"
	redisCommands "github.com/jmhobbs/redis-resque-proxy/redis"
)

func readFromClient(proxy *context) ([]byte, error) {
	proxy.Log.Debug("Waiting For Client")

	buf := &bytes.Buffer{}
	for {
		data := make([]byte, 256)
		// TODO: client.SetReadDeadline(time.Now().Add(time.Second * 5))
		bytes_read, err := proxy.Client.Read(data)
		if err != nil {
			if err == io.EOF {
				proxy.Log.Info("Client Disconnected")
			} else {
				proxy.Log.Error(err.Error())
			}
			return nil, err
		}

		proxy.Log.WithFields(logrus.Fields{"bytes": bytes_read}).Debug("Read From Client")

		buf.Write(data[:bytes_read])

		if bytes_read < 256 {
			break
		}
	}
	return buf.Bytes(), nil
}

func readFromRedis(proxy *context, write bool) (success bool) {
	for {
		proxy.Log.Debug("Waiting for Redis")
		data := make([]byte, 256)
		proxy.Redis.SetReadDeadline(time.Now().Add(time.Millisecond * 100))
		bytes_read, err := proxy.Redis.Read(data)
		if err != nil {
			if err == io.EOF {
				proxy.Log.Error("Redis Disconnected")
				return false
			} else if err.(*net.OpError).Timeout() {
				// So, this is kind of weak.  I don't want to parse
				// responses from Redis to know how many bytes to read
				// because that's a pain, so I'm abusing read timeouts
				// and the synchronous nature of the redis protocol
				proxy.Log.Debug("Read Timed out")
				break
			} else {
				proxy.Log.Error(err)
				return false
			}
		}
		proxy.Log.WithFields(logrus.Fields{"bytes": bytes_read}).Debug("Read Redis")
		proxy.Log.Debug("%s", hex.Dump(data[:bytes_read]))

		if write {
			proxy.Log.Debug("Writing To Client")
			bytes_written, err := proxy.Client.Write(data[:bytes_read])
			if err != nil {
				// TODO: Break outer loop instead of panic
				panic(err)
			}

			proxy.Log.WithFields(logrus.Fields{"bytes": bytes_written}).Debug("Wrote To Client")
		}
	}

	return true
}

func proxyConn(client *net.TCPConn) {
	defer client.Close()

	proxy := context{nil, client, "", logrus.WithFields(logrus.Fields{"client": client.RemoteAddr()})}
	proxy.Log.Info("Connect")

	for {

		msg, err := readFromClient(&proxy)
		if err != nil {
			return
		}

		cmd, err := redisCommands.Parse(msg)
		if err != nil {
			proxy.Log.Error(err)
		} else {
			// Try to connect to upstream
			if proxy.Redis == nil {
				proxy.Log.Debug("Dialing Redis")
				d := &net.Dialer{Deadline: time.Now().Add(time.Millisecond * 50)}
				conn, err := d.Dial("tcp", *remoteAddr)
				if err != nil {
					proxy.Log.Info("Failed to dial Redis")
				} else {
					proxy.Redis = conn.(*net.TCPConn)
					defer proxy.Redis.Close()
					// Try to transparently re-auth if needed
					if proxy.Auth != "" {
						proxy.Log.Info("Transparently Re-Authenticating")
						proxy.Log.Info(fmt.Sprintf("*2\r\n$4\r\nAUTH\r\n$%d\r\n%s\r\n", len(proxy.Auth), proxy.Auth))
						_, err := proxy.Redis.Write([]byte(fmt.Sprintf("*2\r\n$4\r\nAUTH\r\n$%d\r\n%s\r\n", len(proxy.Auth), proxy.Auth)))
						if err != nil {
							proxy.Log.Error(err.Error())
							return
						}
						if !readFromRedis(&proxy, false) {
							// Too broke to safely continue.
							return
						}
					}
				}
			}

			proxy.Log.Info(fmt.Sprintf("%s %s", cmd.Command, cmd.Arguments))
			if proxy.Redis == nil {
				proxy.Client.Write(fakeResponse(*cmd))
			}
			// Save Auth for disconnect/reconnect?
			if strings.ToUpper(cmd.Command) == "AUTH" {
				proxy.Auth = cmd.Arguments[0]
			}
		}

		if proxy.Redis == nil {
			continue
		}

		proxy.Log.Debug("Writing To Redis")
		// TODO: if we error, we should parse and nil out redis?
		bytes_written, err := proxy.Redis.Write(msg)
		if err != nil {
			proxy.Log.Error(err.Error())
			// TODO: Try to write back to the client?
			return
		}
		proxy.Log.WithFields(logrus.Fields{"bytes": bytes_written}).Debug("Wrote To Redis")

		if !readFromRedis(&proxy, true) {
			proxy.Redis = nil
			proxy.Client.Write(fakeResponse(*cmd))
		}
	}
	proxy.Log.Error("Broke out of loop!?")
}
