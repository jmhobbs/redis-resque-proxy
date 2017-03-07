package proxy

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/inconshreveable/log15"
	command "github.com/jmhobbs/redis-resque-proxy/redis"
)

type ProxyConfig struct {
	address string
	auth    *string
}

func NewProxyConfig(address string, auth *string) *ProxyConfig {
	return &ProxyConfig{address, auth}
}

type ProxiedConnection struct {
	config        *ProxyConfig
	client        *net.TCPConn
	redis         *net.TCPConn
	authenticated bool
	log           log15.Logger
	history       *commandlog
}

func ProxyConnection(client *net.TCPConn, parent_log log15.Logger, config *ProxyConfig) {
	defer client.Close()
	pc := ProxiedConnection{config, client, nil, false, parent_log.New("client", client.RemoteAddr()), NewCommandLog(5)}
	pc.Run()
}

func (pc *ProxiedConnection) Run() {
	pc.log.Debug("Client Connected")

	for {
		// 1. Read from Client
		msg, err := pc.readFromClient()
		if err != nil {
			// TODO: Deeper logging  with commandLog if a client disconnected
			pc.log.Info(err.Error())
			break
		}
		pc.log.Debug("Read from client!", "bytes", msg)
		pc.history.Push(msg)

		// 2. Parse
		cmd, err := command.Parse(msg)
		if err != nil {
			pc.log.Error(err.Error())
		} else {
			pc.log.Debug(cmd.Command)
		}

		// 3. Connect if needed
		if pc.redis == nil && pc.connect() {
			defer pc.redis.Close()
		}

		var response []byte

		if pc.redis != nil {
			// 4. Write to Redis
			pc.writeToRedis(msg)
			// 5. Read from Redis
			response, err = pc.readFromRedis()
			if err != nil {
				pc.log.Error(err.Error())
				pc.redis.Close()
				pc.redis = nil
			}
			pc.log.Debug("Read from redis", "bytes", response)

			// 5.1 If it was an AUTH and it worked, we should store that.
			if strings.ToUpper(cmd.Command) == "AUTH" && bytes.Equal(response, []byte("+OK\r\n")) {
				pc.authenticated = true
			}
		}

		// 6. Write to FakeRedis (?)
		if pc.redis == nil {
			pc.log.Debug("Getting offline response")
			response = pc.offlineResponse(cmd)
		}

		// 7. Write to Client
		pc.writeToClient(response)
	}
}

func (pc *ProxiedConnection) readFromClient() ([]byte, error) {
	buf := &bytes.Buffer{}
	for {
		data := make([]byte, CHUNK_SIZE)
		pc.client.SetReadDeadline(time.Now().Add(time.Second * 30))
		read, err := pc.client.Read(data)
		if err != nil {
			if err == io.EOF {
				return nil, &SocketDisconnectError{"Client Disconnected"}
			}
			return nil, &SocketError{err.Error()}
		}
		// TODO: log bytes at debug levels?
		buf.Write(data[:read])
		if read < CHUNK_SIZE {
			break
		}
	}
	return buf.Bytes(), nil
}

func (pc *ProxiedConnection) writeToClient(msg []byte) {
	pc.log.Debug("Writing to Client", "msg", msg)
	_, err := pc.client.Write(msg)
	if err != nil {
		// TODO: Break outer loop instead of panic
		panic(err)
	}
	// TODO: Log?
}

func (pc *ProxiedConnection) readFromRedis() ([]byte, error) {
	pc.log.Debug("Reading From Redis")
	buf := &bytes.Buffer{}
	for {
		data := make([]byte, CHUNK_SIZE)
		pc.redis.SetReadDeadline(time.Now().Add(time.Millisecond * 250))
		read, err := pc.redis.Read(data)
		if err != nil {
			if err == io.EOF {
				return nil, &SocketDisconnectError{"Redis Disconnected"}
			} else if err.(*net.OpError).Timeout() {
				// So, this is kind of weak.  I don't want to parse
				// responses from Redis to know how many bytes to read
				// because that's a pain, so I'm abusing read timeouts
				// and the synchronous nature of the redis protocol
				pc.log.Debug("Redis Read Timeout")
				break
			} else {
				return nil, &SocketError{err.Error()}
			}
		}
		buf.Write(data[:read])
	}
	return buf.Bytes(), nil
}

func (pc *ProxiedConnection) writeToRedis(msg []byte) {
	pc.log.Debug("Writing to Redis", "msg", msg)
	_, err := pc.redis.Write(msg)
	if err != nil {
		// TODO: Break outer loop instead of panic
		panic(err)
	}
}

func (pc *ProxiedConnection) connect() bool {
	pc.log.Debug("Dialing Redis")
	d := &net.Dialer{Deadline: time.Now().Add(time.Millisecond * 50)}
	conn, err := d.Dial("tcp", pc.config.address)
	if err != nil {
		pc.log.Debug("Failed to dial Redis")
		return false
	} else {
		pc.log.Debug("Connected Redis")
		pc.redis = conn.(*net.TCPConn)
		// Try to transparently re-auth if needed
		if pc.authenticated {
			pc.log.Debug("Transparently Re-Authenticating")
			pc.writeToRedis([]byte(fmt.Sprintf("*2\r\n$4\r\nAUTH\r\n$%d\r\n%s\r\n", len(*pc.config.auth), *pc.config.auth)))
			okplease, err := pc.readFromRedis()
			if err != nil {
				// This is probably not safe to recover from.
				pc.redis.Close()
				panic(err)
			}
			if !bytes.Equal(okplease, []byte("+OK\r\n")) {
				pc.redis.Close()
				panic("Bad response to re-authentication.")
			}
		}
	}
	return true
}

func (pc *ProxiedConnection) offlineResponse(c *command.Command) []byte {
	// These are largely pulled from calls here:
	// https://github.com/getflywheel/resque/blob/master/lib/resque/data_store.rb
	switch uc := strings.ToUpper(c.Command); uc {
	case "AUTH":
		if pc.config.auth == nil {
			return []byte("-ERR Client sent AUTH, but no password is set\r\n")
		} else if *pc.config.auth == c.Arguments[0] {
			pc.authenticated = true
			return []byte("+OK\r\n")
		} else {
			return []byte("-ERR invalid password\r\n")
		}
	case "LPOP":
		return []byte("$-1\r\n")
	case "LLEN":
		return []byte(":0\r\n")
	case "BLPOP":
		sleep, err := strconv.ParseInt(c.Arguments[len(c.Arguments)-1], 10, 0)
		if err == nil && sleep > 0 {
			time.Sleep(time.Second * time.Duration(sleep))
		}
		return []byte("*-1\r\n")
	// These are related to worker management
	case "GET":
		return []byte("$-1\r\n")
	case "SET":
		return []byte("+OK\r\n")
	case "DEL":
		return []byte(":0\r\n")
	case "SADD":
		return []byte(":1\r\n")
	case "SREM":
		return []byte(":0\r\n")
	case "SMEMBERS":
		return []byte("*0\r\n")
	case "SISMEMBER":
		return []byte(":0\r\n")
	default:
		//log.Debug(fmt.Sprintf("fakeResponse: Unknown Command, %s %s", uc, c.Arguments))
		return []byte("-ERR Redis Server Unavailable\r\n")
	}
}
