package main

import (
	"flag"
	"net"
	"os"

	"github.com/inconshreveable/log15"
	"github.com/jmhobbs/redis-resque-proxy/proxy"
)

var localAddr *string = flag.String("listen", "127.0.0.1:9000", "local address")
var remoteAddr *string = flag.String("redis", "127.0.0.1:6379", "redis address")
var authKey *string = flag.String("auth", "", "AUTH password")
var verbose *bool = flag.Bool("verbose", false, "Be noisy.")
var vverbose *bool = flag.Bool("vverbose", false, "Be really noisy.")

func main() {
	log := log15.New()

	flag.Parse()
	if *vverbose {
		log.SetHandler(log15.LvlFilterHandler(log15.LvlDebug, log15.StreamHandler(os.Stdout, log15.TerminalFormat())))
	} else if *verbose {
		log.SetHandler(log15.LvlFilterHandler(log15.LvlInfo, log15.StreamHandler(os.Stdout, log15.TerminalFormat())))
	} else {
		log.SetHandler(log15.LvlFilterHandler(log15.LvlWarn, log15.StreamHandler(os.Stdout, log15.TerminalFormat())))
	}

	log.Info("Listening", "on", *localAddr)
	log.Info("Proxying", "to", *remoteAddr)

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
		go proxy.ProxyConnection(conn, log, proxy.NewProxyConfig(*remoteAddr, authKey))
	}
}
