// Implementation of a KeyValueServer. Students should write their code in this file.

package p0partA

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"strings"
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
	aliveCnt int
	disCnt   int
	mtx      sync.Mutex
	kvMtx    sync.Mutex
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

		log.WithField("conn remote", conn.RemoteAddr().String()).Info("new conn come")

		kvs.mtx.Lock()
		kvs.conns[conn] = 1
		kvs.aliveCnt += 1
		kvs.mtx.Unlock()

		go kvs.processConn(conn)

	}
}

func (kvs *keyValueServer) processConn(conn net.Conn) {
	for {
		reader := bufio.NewReader(conn)
		var buf [128]byte
		n, err := reader.Read(buf[:]) // 读取数据
		if err == io.EOF {
			kvs.mtx.Lock()
			conn.Close()
			kvs.disCnt += 1
			kvs.aliveCnt -= 1
			kvs.mtx.Unlock()

			log.WithField("alive cnt", kvs.aliveCnt).WithField("dis cnt", kvs.disCnt).Info("")
			return
		}
		if err != nil {
			fmt.Println("read from client failed, err: ", err)
			continue
		}
		recvStr := string(buf[:n])
		recvStr = strings.Trim(recvStr, "\n")

		cmd, err := parseCommand(recvStr)
		if err != nil {
			log.WithError(err).Error("parseCommand failed")
			return
		}

		if cmd.Op == "Get" {
			kvs.kvMtx.Lock()
			vs := kvs.kvStore.Get(cmd.Key)
			kvs.kvMtx.Unlock()

			for i := range vs {
				msg := composeGetMsg(cmd.Key, vs[i])
				conn.Write(msg)
			}
		} else if cmd.Op == "Debug" {
			conn.Write([]byte(recvStr))
		}

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
	return kvs.aliveCnt
}

func (kvs *keyValueServer) CountDropped() int {
	// TODO: implement this!
	return kvs.disCnt
}

// TODO: add additional methods/functions below!
