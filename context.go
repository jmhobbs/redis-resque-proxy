package main

import (
	"net"

	"github.com/Sirupsen/logrus"
)

type context struct {
	Redis  *net.TCPConn
	Client *net.TCPConn
	Auth   string
	Log    *logrus.Entry
}
