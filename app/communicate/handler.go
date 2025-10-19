package communicate

import (
	"net"
)

func HandleConn(conn net.Conn) {
	mgr := ConnManager(conn)
	defer mgr.Close()

	mgr.onHandle()
}
