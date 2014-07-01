package main

import (
	"fmt"
	"github.com/nikolodien/goDB/cluster"
	"time"
)

func main() {

	server := cluster.New(2,"./config.json")
	for {
		time.Sleep(time.Second)
		server.Outbox() <- &cluster.Envelope{Pid: cluster.BROADCAST, Msg: "hello there"}
		server.Outbox() <- &cluster.Envelope{Pid: 1, Msg: "Hi Server"}
		select {
		case envelope := <-server.Inbox():
			fmt.Printf("Received msg from %d: '%s' \n", envelope.Pid, envelope.Msg)
		case <-time.After(20 * time.Second):
			fmt.Println("Waited and Waited")
			cluster.Close(server)
			return
		}
	}
}
