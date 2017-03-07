package proxy

type SocketDisconnectError struct {
	msg string
}

func (e SocketDisconnectError) Error() string {
	return e.msg
}

type SocketError struct {
	msg string
}

func (e SocketError) Error() string {
	return e.msg
}
