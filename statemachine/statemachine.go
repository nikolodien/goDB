package kvstore

import (
	// "fmt"
	"errors"
	"github.com/nikolodien/goDB/raft"
	"log"
	"net/http"
	"net/rpc"
	"os/exec"
	"time"
)

const (
	PORT       = "9999"
	CHAN_LIMIT = 10000
)

// Used for invocation
type KVStore struct {
}

var (
	// channel to send commands to raft
	cmdChannel           chan raft.KVCommand
	entryChannel         chan *raft.LogEntry
	dataInterfaceChannel chan interface{}
	closeChannel         chan bool
)

func init() {
	cmdChannel = make(chan raft.KVCommand, CHAN_LIMIT)
	entryChannel = make(chan *raft.LogEntry, CHAN_LIMIT)
	dataInterfaceChannel = make(chan interface{}, CHAN_LIMIT)
	closeChannel = make(chan bool, 1)
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

func getLeader(servers []raft.RaftServer) raft.RaftServer {
	var leader raft.RaftServer = nil
	var present bool = false
inner:
	for server := range servers {
		if servers[server].IsLeader() {
			if !present {
				leader = servers[server]
				present = true
				break inner
			}
		}
	}
	return leader
}

func startServer() {
	kvstore := new(KVStore)
	rpc.Register(kvstore)
	rpc.HandleHTTP()
	err := http.ListenAndServe(":"+PORT, nil)
	if err != nil {
		log.Println(err.Error())
	}
}

func commandHandler() {
	// debug flag
	var debug bool = false

	// Start the broker
	cmd := startBroker()

	server1 := raft.New(1, "./config.json", debug)
	server2 := raft.New(2, "./config.json", debug)
	server3 := raft.New(3, "./config.json", debug)
	server4 := raft.New(4, "./config.json", debug)

	// Wait for leader election
	time.Sleep(time.Second * 2)

	servers := []raft.RaftServer{raft.RaftServer(server1), raft.RaftServer(server2), raft.RaftServer(server3), raft.RaftServer(server4)}

loop:
	for {
		select {
		case command := <-cmdChannel:
			if debug {
				log.Printf("Received command %v", command)
			}
			// Get the leader
			var leader raft.RaftServer = nil
			for leader == nil {
				leader = getLeader(servers)
			}
			switch command.Command {
			case raft.PUT:
				leader.Outbox() <- command
				select {
				case message := <-leader.Inbox():
					entryChannel <- message
					if debug {
						log.Printf("Received commit message from leader (%v)", message)
					}
				case <-time.After(time.Second * 2):
					// Wait 2 seconds for the message to be committed
					// If not, mark as not committed
					entryChannel <- nil
				}
			case raft.DEL:
				leader.Outbox() <- command
				select {
				case message := <-leader.Inbox():
					entryChannel <- message
					if debug {
						log.Printf("Received commit message from leader (%v)", message)
					}
				case <-time.After(time.Second * 2):
					// Wait 2 seconds for the message to be committed
					// If not, mark as not committed
					entryChannel <- nil
				}
			case raft.GET:
				dataInterfaceChannel <- leader.Store()[command.Key]
			}
		case <-closeChannel:
			break loop
		}
	}

	log.Printf("Closing commandHandler")
	raft.Close(server1)
	raft.Close(server2)
	raft.Close(server3)
	raft.Close(server4)
	// Shutdown the broker
	stopBroker(cmd)
}

func (store *KVStore) Put(keyValue KeyValue, reply *int) error {
	cmdChannel <- raft.KVCommand{Command: raft.PUT, Key: keyValue.Key, Value: keyValue.Value}
	commitEntry := <-entryChannel
	if commitEntry != nil {
		return nil
	} else {
		return errors.New("Put not successful")
	}
}

func (store *KVStore) Del(key string, reply *int) error {
	cmdChannel <- raft.KVCommand{Command: raft.DEL, Key: key, Value: nil}
	commitEntry := <-entryChannel
	if commitEntry != nil {
		return nil
	} else {
		return errors.New("Del not successful")
	}
}

func (store *KVStore) Get(key string, val *Val) error {
	cmdChannel <- raft.KVCommand{Command: raft.GET, Key: key, Value: nil}
	select {
	case data := <-dataInterfaceChannel:
		val.Value = data
	}
	return nil
}

func New() {
	log.Printf("Starting new kvstore server")
	go startServer()
	go commandHandler()
}

func Close() {
	closeChannel <- true
}
