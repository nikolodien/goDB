package main

import (
	"fmt"
	"strconv"
	"errors"
	"github.com/nikolodien/gokvstore/cluster"
	"net/http"
	"net/rpc"
)

type Broker struct {
}

// Server instance map
var servers map[int]*cluster.CServer

func initialize() {
	servers = make(map[int]*cluster.CServer)
}

func (t *Broker) Add(pidServer *cluster.PidServer, reply *int) error {
	if servers[pidServer.Pid] != nil {
		*reply = 1
		return errors.New("Server with pid "+strconv.Itoa(pidServer.Pid)+" is already up")
	} else {
		servers[pidServer.Pid] = pidServer.Server
		*reply = 0
	}
	servers[pidServer.Pid] = pidServer.Server
	return nil
}

func (t *Broker) Get(pid int, cserver **cluster.CServer) error {
	val, ok := servers[pid]
	if !ok {
		*cserver = nil
		return errors.New("server not present")
	}
	*cserver = val
	return nil
}

func (t *Broker) Del(pid int, reply *int) error {
	if servers[pid] == nil {
		return errors.New("Server with pid "+strconv.Itoa(pid)+" is not present")
		*reply = 1
	} else {
		delete(servers, pid)
		*reply = 0
	}
	return nil
}

func (t *Broker) Count(pid int, reply *int) error {
	*reply = len(servers)
	return nil
}

func main() {
	initialize()
	broker := new(Broker)
	rpc.Register(broker)
	rpc.HandleHTTP()
	err := http.ListenAndServe(":5555", nil)
	if err != nil {
		fmt.Println(err.Error())
	}
}
