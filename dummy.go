package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/jmhobbs/redis-resque-proxy/redis"
)

func fakeResponse(c redis.Command) []byte {
	// These are largely pulled from calls here:
	// https://github.com/getflywheel/resque/blob/master/lib/resque/data_store.rb
	switch uc := strings.ToUpper(c.Command); uc {
	case "AUTH":
		if *authKey == "" {
			return []byte("-ERR Client sent AUTH, but no password is set\r\n")
		} else if *authKey == c.Arguments[0] {
			// TODO: Save this state for if we re-connect
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
	default:
		log.Debug(fmt.Sprintf("fakeResponse: Unknown Command, %s %s", uc, c.Arguments))
		return []byte("-ERR Redis Server Unavailable\r\n")
	}
}
