package raft

import (
	"fmt"
	"log"
	"os/exec"
	"testing"
	"time"
)

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

// Test for persistence of the term
func TestTermReadPersistence(t *testing.T) {
	debug := false
	// Start the broker
	cmd := startBroker()
	// Proceed with tests
	log.Printf("Testing term Persistence")
	// Start the new server
	log.Printf("Starting the server")

	rserver := New(100, "./termPersistenceConfig.json", debug)
	time.Sleep(time.Second)
	term := rserver.Term()

	// Shutdown the server
	log.Printf("Shutting down the server")
	Close(rserver)
	// Waiting for the server to shutdown
	time.Sleep(time.Second)
	// Restart the server
	log.Printf("Starting the server again")
	rserver = New(100, "./termPersistenceConfig.json", debug)
	// Waiting for the server to setup
	time.Sleep(time.Second)

	if !(rserver.Term() >= term) {
		t.Error("Term not persisted")
	}
	log.Printf("Shutting down the server")
	// Shutdown the server
	Close(rserver)
	// Waiting for the server to shutdown
	time.Sleep(time.Second)
	// Shutdown the broker
	stopBroker(cmd)
}

// Test Proper Shutdown
func TestServerShutdown(t *testing.T) {
	debug := false

	log.Printf("Testing Server Shutdown")

	// Start the broker
	cmd := startBroker()

	log.Printf("Starting Servers")
	server1 := New(1, "./config.json", debug)
	server2 := New(2, "./config.json", debug)
	server3 := New(3, "./config.json", debug)
	server4 := New(4, "./config.json", debug)
	time.Sleep(time.Second)

	log.Printf("Shutting down the servers")
	Close(server1)
	Close(server2)
	Close(server3)
	Close(server4)
	time.Sleep(time.Second)

	if Exists(1, debug) {
		t.Error("Server not properly shutdown")
	}

	if Exists(2, debug) {
		t.Error("Server not properly shutdown")
	}

	if Exists(3, debug) {
		t.Error("Server not properly shutdown")
	}

	if Exists(4, debug) {
		t.Error("Server not properly shutdown")
	}
	// Shutdown the broker
	stopBroker(cmd)
}

func TestBasicLeaderElection(t *testing.T) {
	log.Printf("Testing Leader election")

	debug := false

	// Start the broker
	cmd := startBroker()

	log.Printf("Starting Servers")
	server1 := New(1, "./config.json", debug)
	server2 := New(2, "./config.json", debug)
	server3 := New(3, "./config.json", debug)
	server4 := New(4, "./config.json", debug)
	time.Sleep(time.Second)

	// Wait for leader election
	time.Sleep(time.Second * 10)

	var present bool = false

	rservers := []*rServer{server1, server2, server3, server4}

	for server := range rservers {
		if rservers[server].IsLeader() {
			if !present {
				present = true
			} else {
				t.Error("More than one leader")
			}
		}
	}

	if !present {
		t.Error("No leader elected yet")
	}

	Close(server1)
	Close(server2)
	Close(server3)
	Close(server4)
	time.Sleep(time.Second)

	// Shutdown the broker
	stopBroker(cmd)
}

func testNewLeaderElection(t *testing.T, rserver *rServer, rservers []*rServer) {
	Close(rserver)
	// Wait for election of a new leader
	time.Sleep(time.Second * 4)

	var present bool = false

	for server := range rservers {
		if rservers[server].IsLeader() {
			if !present {
				present = true
			} else {
				t.Error("More than one leader")
			}
		}
	}

	if !present {
		t.Error("No leader elected yet")
	}

	// Now close the remaining servers
	for server := range rservers {
		Close(rservers[server])
	}
	time.Sleep(time.Second)
}

func TestFTLeaderElection(t *testing.T) {
	log.Printf("Testing Leader election under server failure")

	debug := false

	// Start the broker
	cmd := startBroker()

	log.Printf("Starting Servers")
	server1 := New(1, "./config.json", debug)
	server2 := New(2, "./config.json", debug)
	server3 := New(3, "./config.json", debug)
	server4 := New(4, "./config.json", debug)
	time.Sleep(time.Second)

	// Wait for leader election
	time.Sleep(time.Second * 4)

	var present bool = false

	rservers := []*rServer{server1, server2, server3, server4}

	for server := range rservers {
		if rservers[server].IsLeader() {
			if !present {
				present = true
			} else {
				t.Error("More than one leader")
			}
		}
	}

	if !present {
		t.Error("No leader elected yet")
	}

	// Close the leader
	if server1.IsLeader() {
		testNewLeaderElection(t, server1, []*rServer{server2, server3, server4})
	} else if server2.IsLeader() {
		testNewLeaderElection(t, server2, []*rServer{server1, server3, server4})
	} else if server3.IsLeader() {
		testNewLeaderElection(t, server3, []*rServer{server1, server2, server4})
	} else if server4.IsLeader() {
		testNewLeaderElection(t, server4, []*rServer{server1, server2, server3})
	}

	// Shutdown the broker
	stopBroker(cmd)
}

// Log replication testing
func getLeader(rservers []*rServer, t *testing.T) *rServer {
	var leader *rServer = nil
	var present bool = false
	for server := range rservers {
		if rservers[server].IsLeader() {
			if !present {
				leader = rservers[server]
				present = true
			} else {
				t.Error("More than one leader")
			}
		}
	}
	return leader
}

func TestLogReplication(t *testing.T) {
	log.Printf("Testing Log Replication")

	debug := false

	// Start the broker
	cmd := startBroker()

	log.Printf("Starting Servers")
	server1 := New(1, "./config.json", debug)
	server2 := New(2, "./config.json", debug)
	server3 := New(3, "./config.json", debug)
	server4 := New(4, "./config.json", debug)

	// Wait for leader election
	time.Sleep(time.Second * 2)

	rservers := []*rServer{server1, server2, server3, server4}

	var start int64 = time.Now().Unix()
	var end int64 = start + 1000

	var committed [1000]bool
	// Commands to put data in the kv store
	for i, j := start, 0; i < end; i, j = i+1, j+1 {
		time.Sleep(time.Millisecond)
		key := fmt.Sprintf("%v", i)
		var leader *rServer = nil
		inner:for leader == nil {
			leader = getLeader(rservers, t)
			if leader != nil {
				leader.Outbox() <- KVCommand{Command: PUT, Key: key, Value: i}
				select {
				case message := <-leader.Inbox():
					if debug {
						log.Printf("Received commit message from leader (%v)", message)
					}
					committed[j] = true
				case <-time.After(time.Second * 2):
					// Wait 2 seconds for the message to be committed
					// If not, mark as not committed
					committed[j] = false
					break inner
				}
			}
		}
		leader = nil
	}

	time.Sleep(time.Second * 2)

	// Get values directly from StateMachine now but other server
	for i, j := start, 0; i < end; i, j = i+1, j+1 {
		key := fmt.Sprintf("%v", i)
		if committed[j] {
			for server := range rservers {
				if rservers[server].StateMachine[key] == i {
					if debug {
						log.Printf("Success reading from StateMachine %d", rservers[server].Pid)
					}
				} else {
					error := fmt.Sprintf("Error reading from StateMachine %d for key %s", rservers[server].Pid, key)
					t.Error(error)
				}
			}
		}
	}

	log.Printf("Shutting down all the servers")
	// Now close the remaining servers
	for server := range rservers {
		Close(rservers[server])
	}
	time.Sleep(time.Second * 2)

	// Shutdown the broker
	stopBroker(cmd)
}

func TestFTLogReplication(t *testing.T) {
	log.Printf("Testing Persistence of Log Replication")

	debug := false

	// Start the broker
	cmd := startBroker()

	log.Printf("Starting Servers")
	server1 := New(1, "./config.json", debug)
	server2 := New(2, "./config.json", debug)
	server3 := New(3, "./config.json", debug)
	server4 := New(4, "./config.json", debug)
	time.Sleep(time.Second)

	// Wait for leader election
	time.Sleep(time.Second * 2)

	rservers := []*rServer{server1, server2, server3, server4}

	var start int64 = time.Now().Unix()
	var end int64 = start + 100

	var committed [100]bool
	// Commands to put data in the kv store
	inner:for i, j := start, 0; i < end; i, j = i+1, j+1 {
		time.Sleep(time.Millisecond)
		key := fmt.Sprintf("%v", i)
		var leader *rServer = nil
		for leader == nil {
			leader = getLeader(rservers, t)
			if leader != nil {
				leader.Outbox() <- KVCommand{Command: PUT, Key: key, Value: i}
				select {
				case message := <-leader.Inbox():
					if debug {
						log.Printf("Received commit message from leader (%v)", message)
					}
					committed[j] = true
				case <-time.After(time.Second * 2):
					// Wait 2 seconds for the message to be committed
					// If not, mark as not committed
					committed[j] = false
					break inner
				}
			}
		}
		leader = nil
	}

	time.Sleep(time.Second * 2)

	log.Printf("Shutting down all the servers")
	// Now close the remaining servers
	for server := range rservers {
		Close(rservers[server])
	}
	time.Sleep(time.Second * 2)

	// Now start all the servers again and check if the StateMachine contains the data
	log.Printf("Starting Servers Again")
	server1 = New(1, "./config.json", debug)
	server2 = New(2, "./config.json", debug)
	server3 = New(3, "./config.json", debug)
	server4 = New(4, "./config.json", debug)

	time.Sleep(time.Second * 5)

	rservers = []*rServer{server1, server2, server3, server4}

	// Get values directly from StateMachines
	for i, j := start, 0; i < end; i, j = i+1, j+1 {
		key := fmt.Sprintf("%v", i)
		if committed[j] {
			var presence bool = false
			for server := range rservers {
				if rservers[server].StateMachine[key] == i {
					presence = true
					if debug {
						log.Printf("Success reading from StateMachine %d", rservers[server].Pid)
					}
				}
			}
			if !presence {
				t.Error("Log not persisted KVCommand corresponding to key",i)
			}
		}
	}

	log.Printf("Shutting down all the servers")
	// Now close the remaining servers
	for server := range rservers {
		Close(rservers[server])
	}
	time.Sleep(time.Second)

	// Shutdown the broker
	stopBroker(cmd)
}
