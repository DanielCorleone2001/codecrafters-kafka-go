package api

import (
	"io"
	"net"
)

type APIHandler interface {
	HandleAPIEvent(req *RequestMetaInfo, bodyReader io.Reader, conn net.Conn)
}

var _ APIHandler = &todoHandler{}

type todoHandler struct {
}

func (t todoHandler) HandleAPIEvent(req *RequestMetaInfo, bodyReader io.Reader, conn net.Conn) {
}
