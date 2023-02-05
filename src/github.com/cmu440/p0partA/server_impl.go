// Implementation of a KeyValueServer. Students should write their code in this file.

package p0partA

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"sync"

	log "github.com/sirupsen/logrus"

	"github.com/cmu440/p0partA/kvstore"
)

type keyValueServer struct {
	// TODO: implement this!
	kvStore  kvstore.KVStore
	listener net.Listener
	ctx      context.Context
	cancel   context.CancelFunc
	conns    map[net.Conn]int
	mtx      sync.Mutex
}

// New creates and returns (but does not start) a new KeyValueServer.
func New(store kvstore.KVStore) KeyValueServer {
	// TODO: implement this!

	ctx, cancel := context.WithCancel(context.TODO())

	r := &keyValueServer{
		kvStore: store,
		conns:   make(map[net.Conn]int, 0),
		ctx:     ctx,
		cancel:  cancel,
	}
	return r
}

func (kvs *keyValueServer) listening() {
	for {
		select {
		case <-kvs.ctx.Done():
			log.Info("exited accept thread !!!")
			return
		default:
		}

		conn, err := kvs.listener.Accept()
		if err != nil {
			log.WithError(err).Error("listener accept error")
			return
		}

		kvs.mtx.Lock()
		kvs.conns[conn] = 1
		kvs.mtx.Unlock()

		go processConn(conn)

	}
}

func processConn(conn net.Conn) {
	for {
		reader := bufio.NewReader(conn)
		var buf [128]byte
		n, err := reader.Read(buf[:]) // 读取数据
		if err != nil {
			fmt.Println("read from client failed, err: ", err)
			break
		}
		recvStr := string(buf[:n])
		conn.Write([]byte(recvStr)) // 发送数据
		log.WithField("recvStr", recvStr).Info("")
	}
}

func (kvs *keyValueServer) Start(port int) error {
	// TODO: implement this!

	serverAddr := fmt.Sprintf("127.0.0.1:%v", port)
	logger := log.WithField("serverAddr", serverAddr)

	listener, err := net.Listen("tcp", serverAddr)
	if err != nil {
		logger.WithError(err).Error("start lsiten failed")
		return err
	}

	kvs.listener = listener

	go kvs.listening()

	logger.Info("started listener ...")
	return nil
}

func (kvs *keyValueServer) Close() {
	// TODO: implement this!
	kvs.cancel()

	for k := range kvs.conns {
		k.Close()
	}
}

func (kvs *keyValueServer) CountActive() int {
	// TODO: implement this!
	return -1
}

func (kvs *keyValueServer) CountDropped() int {
	// TODO: implement this!
	return -1
}

// TODO: add additional methods/functions below!
