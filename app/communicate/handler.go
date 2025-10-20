package communicate

import "net"

type APIHandler interface {
	HandleAPIEvent(req *RequestMetaInfo, conn net.Conn)
}

var _ APIHandler = &todoHandler{}

type todoHandler struct {
}

func (t todoHandler) HandleAPIEvent(req *RequestMetaInfo, conn net.Conn) {
	//TODO implement me
	panic("implement me")
}
