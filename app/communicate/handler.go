package communicate

import (
	"github.com/codecrafters-io/kafka-starter-go/app/communicate/api_version"
	"net"
)

func HandleConn(conn net.Conn) {
	mgr := api_version.APIVersionManager(conn)
	for {
		mgr.OnHandle()
	}
}
