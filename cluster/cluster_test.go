package cluster

import (
	"log"
	"os/exec"
	"strconv"
	"testing"
	"time"
)

const (
	LIMIT = 10000
	// Message length becomes 2^12
	MAX_LENGTH = 12
)

// Go routine for sending broadcast messages
func testBroadcastOutgoing(broadcaster *CServer, start int) {
	for id := start; id < start+LIMIT; id++ {
		if id%100 == 0 {
			time.Sleep(time.Millisecond)
		}
		message := strconv.Itoa(id)
		for itr := 0; itr < MAX_LENGTH; itr++ {
			message = message + message
		}
		broadcaster.Outbox() <- &Envelope{Pid: BROADCAST, MsgId: int64(id), Msg: message}
	}
}

// Go routine to test incoming broadcast
func testBroadcastIncoming(broadcastees []*CServer, start int, t *testing.T, quit chan int) {
outer:
	for id := start; id < start+LIMIT; id++ {
		if id%100 == 0 {
			time.Sleep(time.Millisecond)
		}
		message := strconv.Itoa(id)
		for itr := 0; itr < MAX_LENGTH; itr++ {
			message = message + message
		}
		for v := range broadcastees {
			envelope := <-broadcastees[v].Inbox()
			if envelope.Msg != message {
				t.Error("Message not received")
				break outer
			}
		}
	}
	quit <- 1
}

// Test broadcast for each server
func testBroadcast(broadcaster *CServer, broadcastees []*CServer, start int, t *testing.T) {
	quit := make(chan int)
	go testBroadcastOutgoing(broadcaster, start)
	go testBroadcastIncoming(broadcastees, start, t, quit)
	select {
	case <-quit:
		log.Printf("Tested broacasting for %s \n", strconv.Itoa(broadcaster.Pid()))
	}
}

// Go routine to send messages to a peer
func testPointToPointOutgoing(sender *CServer, receiver *CServer, start int) {
	for id := start; id < start+LIMIT; id++ {
		if id%100 == 0 {
			time.Sleep(time.Millisecond)
		}
		sender.Outbox() <- &Envelope{Pid: receiver.Pid(), MsgId: int64(id), Msg: strconv.Itoa(id)}
	}
}

// Go routine to test incoming messages from a peer
func testPointToPointIncoming(sender *CServer, receiver *CServer, start int, t *testing.T, quit chan int) {
outer:
	for id := start; id < start+LIMIT; id++ {
		if id%100 == 0 {
			time.Sleep(time.Millisecond)
		}
		envelope := <-receiver.Inbox()
		if envelope.Msg != strconv.Itoa(id) || envelope.Pid != sender.Pid() {
			t.Error("Message not received")
			break outer
		}
	}
	quit <- 1
}

func testPointToPoint(sender *CServer, receiver *CServer, start int, t *testing.T) {
	quit := make(chan int)
	go testPointToPointOutgoing(sender, receiver, start)
	go testPointToPointIncoming(sender, receiver, start, t, quit)
	select {
	case <-quit:
		log.Printf("Tested point-to-point for pair(sender:%s,receiver:%s) \n", strconv.Itoa(sender.Pid()), strconv.Itoa(receiver.Pid()))
	}
}

func testRoundRobin(servers []*CServer, start int, t *testing.T) {

}

func startBroker() *exec.Cmd {
	// Start the broker first
	log.Printf("Starting the broker")
	cmd := exec.Command("gokvstore", "")
	err := cmd.Start()
	if err != nil {
		log.Fatal(err)
	}
	// Wait for the broker to setup
	time.Sleep(time.Second)
	return cmd
}

func stopBroker(cmd *exec.Cmd) {
	log.Printf("Shutting down the broker")
	cmd.Process.Kill()
}

func TestClusterCount(t *testing.T){
	// Set debug mode
	debug := true

	// Start broker
	cmd := startBroker()

	// Bring up servers
	server1 := New(1, "./config.json",debug)
	server2 := New(2, "./config.json",debug)
	server3 := New(3, "./config.json",debug)
	server4 := New(4, "./config.json",debug)
	// Wait for the servers to setup
	time.Sleep(time.Second)

	if Count() != 4 {
		t.Error("Server count mismatch")
	}

	// Stop one server
	Close(server1,debug)
	// Waiting for the server to shutdown
	time.Sleep(time.Second)

	if Count() != 3 {
		t.Error("Server count mismatch")
	}

	// Restart server1
	server1 = New(1, "./config.json",debug)
	// Wait for the server to start
	time.Sleep(time.Second)

	if Count() != 4 {
		t.Error("Server count mismatch")
	}

	// Now close all the servers
	Close(server1,debug)
	Close(server2,debug)
	Close(server3,debug)
	Close(server4,debug)
	// Waiting for servers to shutdown
	time.Sleep(time.Second)

	if Count() != 0 {
		t.Error("Server count mismatch")
	}

	// Stop broker
	stopBroker(cmd)
}

// Tests for packet drop in the cluster
func TestCluster(t *testing.T) {
	// Set debug mode
	debug := false

	// Start broker
	cmd := startBroker()
	// Bring up servers
	server1 := New(1, "./config.json",debug)
	server2 := New(2, "./config.json",debug)
	server3 := New(3, "./config.json",debug)
	server4 := New(4, "./config.json",debug)

	// Test broacasting for each server one by one
	testBroadcast(server1, []*CServer{server2, server3, server4}, 0, t)
	testBroadcast(server2, []*CServer{server1, server3, server4}, 10000, t)
	testBroadcast(server3, []*CServer{server1, server2, server4}, 20000, t)
	testBroadcast(server4, []*CServer{server1, server2, server3}, 30000, t)

	// Test Point to Point
	testPointToPoint(server1, server2, 0, t)
	testPointToPoint(server1, server3, 10000, t)
	testPointToPoint(server1, server4, 20000, t)
	testPointToPoint(server2, server1, 0, t)
	testPointToPoint(server2, server3, 10000, t)
	testPointToPoint(server2, server4, 20000, t)
	testPointToPoint(server3, server1, 0, t)
	testPointToPoint(server3, server2, 10000, t)
	testPointToPoint(server3, server4, 20000, t)
	testPointToPoint(server4, server1, 0, t)
	testPointToPoint(server4, server2, 10000, t)
	testPointToPoint(server4, server3, 20000, t)

	// Test Round robin
	testRoundRobin([]*CServer{server1, server2, server3, server4}, 0, t)

	// Stop the server
	stopBroker(cmd)
}