package main

import (
	"fmt"
	"github.com/nikolodien/goDB/cluster"
	"time"
)

func main() {

	server := cluster.New(1, "./config.json")
	var count int = 0
	for {
		time.Sleep(time.Second)
		server.Outbox() <- &cluster.Envelope{Pid: cluster.BROADCAST, Msg: "hello there"}
		server.Outbox() <- &cluster.Envelope{Pid: 2, Msg: "Hi Server"}
		count++
		if count == 10 {
			cluster.Close(server)
		}
		select {
		case envelope := <-server.Inbox():
			fmt.Printf("Received msg from %d: '%s' \n", envelope.Pid, envelope.Msg)
		case <-time.After(20 * time.Second):
			fmt.Println("Waited and Waited")
			return
		}
	}
}
