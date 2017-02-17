linux:
	GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o resque-proxy.linux.amd64 main.go proxy.go dummy.go
