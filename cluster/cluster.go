package cluster

import (
	zmq "github.com/pebbe/zmq4"

	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"strings"
)

const (
	BROADCAST      = -1
	BUFF_SIZE      = 10000
	close          = -2
	chanlimit      = 100
	separator      = "::"
	broker_address = "localhost:5555"
)

type PidServer struct {
	Pid    int
	Server *CServer
}

type Envelope struct {
	// On the sender side, Pid identifies the receiving peer. If instead, Pid is
	// set to cluster.BROADCAST, the message is sent to all peers. On the receiver side, the
	// Id is always set to the original sender. If the Id is not found, the message is silently dropped
	Pid int

	// An id that globally and uniquely identifies the message, meant for duplicate detection at
	// higher levels. It is opaque to this package.
	MsgId int64

	// the actual message.
	Msg interface{}
}

// Register the envelope type with gob
func init() {
	gob.Register(Envelope{})
}

type Server interface {
	// Id of this server
	Pid() int

	// array of other servers' ids in the same cluster
	Peers() []int

	// the channel to use to send messages to other peers
	// Note that there are no guarantees of message delivery, and messages
	// are silently dropped
	Outbox() chan *Envelope

	// the channel to receive messages from other peers.
	Inbox() chan *Envelope
}

// Structure of the Server object
type CServer struct {
	CSPid        int
	CSPeers      []int
	ChannelIn    chan *Envelope
	ChannelOut   chan *Envelope
	ChannelClose chan bool
	Address      string
}

// Implementes the Pid() function
func (cserver *CServer) Pid() int {
	return cserver.CSPid
}

// Implements the Peers() function
func (cserver *CServer) Peers() []int {
	return cserver.CSPeers
}

// Implements the Outbox() function
func (cserver *CServer) Outbox() chan *Envelope {
	return cserver.ChannelOut
}

// Implements the Inbox() function
func (cserver *CServer) Inbox() chan *Envelope {
	return cserver.ChannelIn
}

// Serialization of Envelope object to send over the network
func Serialize(envelope *Envelope) string {
	message := ""
	switch str := envelope.Msg.(type) {
	case string:
		message = str
	case fmt.Stringer:
		message = str.String()
	}
	return strconv.Itoa(envelope.Pid) + separator + strconv.FormatInt(envelope.MsgId, 10) + separator + message
}

// Deserialization of the Envelope object after receiving from network
func DeSerialize(s string) *Envelope {
	ss := strings.Split(s, separator)
	pid, err := strconv.Atoi(ss[0])
	if err != nil {
		log.Println("Error while reading message")
	}
	msgid, err := strconv.Atoi(ss[1])
	if err != nil {
		log.Println("Error while reading message")
	}
	return &Envelope{Pid: pid, MsgId: int64(msgid), Msg: ss[2]}
}

func parse(path string) (map[int]string, error) {
	// Have borrowed this piece of code from my classmate Abhishek Gupta
	// log.Println("Parsing The Configuration File")
	addr := make(map[int]string)
	file, err := os.Open(path)
	if err != nil {
		// log.Printf("Parsing Failed %v", err)
		return nil, err
	}
	dec := json.NewDecoder(file)
	for {
		var v map[string]string
		if err := dec.Decode(&v); err == io.EOF || len(v) == 0 {
			// log.Println("Parsing Done !!!")
			file.Close()
			return addr, nil
		} else if err != nil {
			file.Close()
			return nil, err
		}
		spid := v["pid"]
		pid, _ := strconv.Atoi(spid)
		ip := v["host"]
		port := v["port"]
		addr[pid] = ip + ":" + port
	}
}

// Go routine for handling outgoing messages
func outgoing(pid int, peers []int, configs map[int]string, outbox chan *Envelope, channelClose chan bool, debug bool) {
	gob.Register(Envelope{})
	sockets := make(map[int]*zmq.Socket)
	for v := range peers {
		s_out, err := zmq.NewSocket(zmq.PUSH)
		if err != nil {
			return
		}
		err = s_out.Connect("tcp://" + configs[peers[v]])
		if err != nil {
			return
		}
		sockets[peers[v]] = s_out
	}
outer:
	for {
		select {
		case envelope := <-outbox:
			if envelope.Pid == BROADCAST {
				envelope.Pid = pid
				for p := range sockets {
					socket := sockets[p]
					envelope.Pid = pid
					// Create a new bytes.Buffer for gob encoder
					buffer := new(bytes.Buffer)
					// Create new gob encoder
					enc := gob.NewEncoder(buffer)
					// encode the struct
					err := enc.Encode(envelope)
					if err != nil {
						log.Fatal("encode:", err)
					}
					// Create a bytes array
					sendbuf := make([]byte, BUFF_SIZE)
					// Now read the contents of buffer into a byte array
					_, err = buffer.Read(sendbuf)
					// if debug {
					// 	log.Printf("Wrote %d bytes", n)
					// }
					if err != nil {
						log.Fatal("read", err)
					}
					// Send these bytes over the network
					_, err = socket.SendBytes(sendbuf, 0)
					// if debug {
					// 	log.Printf("Wrote %d bytes", n)
					// }
					if err != nil {
						log.Fatal("read", err)
					}
				}
			} else {
				socket := sockets[envelope.Pid]
				envelope.Pid = pid
				// Create a new bytes.Buffer for gob encoder
				buffer := new(bytes.Buffer)
				// Create new gob encoder
				enc := gob.NewEncoder(buffer)
				// encode the struct
				err := enc.Encode(envelope)
				if err != nil {
					log.Fatal("encode:", err)
				}
				// Create a bytes array
				sendbuf := make([]byte, BUFF_SIZE)
				// Now read the contents of buffer into a byte array
				_, err = buffer.Read(sendbuf)
				// if debug {
				// 	log.Printf("Wrote %d bytes", n)
				// }
				if err != nil {
					log.Fatal("read", err)
				}
				// Send these bytes over the network
				_, err = socket.SendBytes(sendbuf, 0)
				// if debug {
				// 	log.Printf("Wrote %d bytes", n)
				// }
				if err != nil {
					log.Fatal("read", err)
				}
			}
		case <-channelClose:
			break outer
		}
	}
	for v := range peers {
		sockets[peers[v]].Close()
	}
	if debug {
		log.Printf("Server with id %d closed for Outgoing", pid)
	}
}

// Go routine for handling incoming messages
func incoming(pid int, address string, inbox chan *Envelope, channelClose chan bool, debug bool) {
	gob.Register(Envelope{})
	s_in, err := zmq.NewSocket(zmq.PULL)
	if err != nil {
		return
	}
	err = s_in.Bind("tcp://" + address)
	if err != nil {
		if debug {
			log.Printf("Error while binding for server %d at address %s", pid, address)
		}
	}
outer:
	for {
		// Check for close signal every microsecond
		var envelope Envelope
		payload_recved, err := s_in.RecvBytes(0)
		if err != nil {
			log.Fatal("receive:", err)
		}
		if len(payload_recved) == 0 {
			continue
		}
		// if debug {
		// 	log.Printf("Length of payload_recved:%d", len(payload_recved))
		// }
		buffer := new(bytes.Buffer)
		buffer.Write(payload_recved)
		decoder := gob.NewDecoder(buffer)
		// if debug {
		// 	log.Printf("Decoding message")
		// }
		err = decoder.Decode(&envelope)
		if err != nil {
			log.Printf("decode error (%s) for server %d", err, pid)
			return
		}
		message := ""
		switch str := envelope.Msg.(type) {
		case string:
			message = str
		}
		if envelope.Pid == close && message == "quit" {
			if debug {
				log.Printf("Server with id %d received message:%s", pid, message)
			}
			s_in.Close()
			break outer
		}
		inbox <- &envelope
	}
	if debug {
		log.Printf("Server with id %d closed for Incoming", pid)
	}
}

// Start a server with a json config file
// It also starts two go routines to handle the incoming and outgoing traffic for the server
func New(pid int, config string, debug bool) *CServer {
	configs, err := parse(config)
	if err != nil {
		if debug {
			log.Printf("Error while parsing %v", err)
		}
		return nil
	}
	var peers []int
	for k, _ := range configs {
		if k != pid {
			peers = append(peers, k)
		}
	}
	cserver := CServer{pid, peers, make(chan *Envelope, chanlimit), make(chan *Envelope, chanlimit), make(chan bool, chanlimit), configs[pid]}
	go outgoing(cserver.Pid(), cserver.Peers(), configs, cserver.Outbox(), cserver.ChannelClose, debug)
	go incoming(cserver.Pid(), configs[pid], cserver.Inbox(), cserver.ChannelClose, debug)
	addServer(pid, &cserver, debug)
	return &cserver
}

// Used to shutdown a server
func Close(cserver *CServer, debug bool) {
	cserver.ChannelClose <- true
	socket, err := zmq.NewSocket(zmq.PUSH)
	if err != nil {
		return
	}
	err = socket.Connect("tcp://" + cserver.Address)
	if err != nil {
		return
	}
	// Create a new bytes.Buffer for gob encoder
	buffer := new(bytes.Buffer)
	// Create new gob encoder
	enc := gob.NewEncoder(buffer)
	// encode the struct
	err = enc.Encode(&Envelope{Pid: close, MsgId: close, Msg: "quit"})
	if err != nil {
		log.Fatal("encode:", err)
	}
	// Create a bytes array
	sendbuf := make([]byte, BUFF_SIZE)
	// Now read the contents of buffer into a byte array
	_, err = buffer.Read(sendbuf)
	// if debug {
	// 	log.Printf("Wrote %d bytes", n)
	// }
	if err != nil {
		log.Fatal("read", err)
	}
	// Send these bytes over the network
	_, err = socket.SendBytes(sendbuf, 0)
	// if debug {
	// 	log.Printf("Wrote %d bytes", n)
	// }
	if err != nil {
		log.Fatal("read", err)
	}
	delServer(cserver.Pid(), debug)
}

func getBrokerConnection() *rpc.Client {
	client, err := rpc.DialHTTP("tcp", broker_address)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	return client
}

// Internal function to add server to the broker
func addServer(pid int, cserver *CServer, debug bool) {
	broker := getBrokerConnection()
	pidServer := PidServer{Pid: pid, Server: cserver}
	var reply int
	err := broker.Call("Broker.Add", &pidServer, &reply)
	if err != nil {
		if debug {
			log.Printf("server error : %s", err)
		}
	}
	broker.Close()
	if reply == 1 {
		if debug {
			log.Printf("Server already registered with broker")
		}
	} else {
		if debug {
			log.Printf("Registered server instance with broker")
		}
	}
}

// Internal function to get server instance from broker
func getServer(pid int, debug bool) *CServer {
	broker := getBrokerConnection()
	var cserver *CServer
	err := broker.Call("Broker.Get", pid, &cserver)
	if err != nil {
		if debug {
			log.Printf("server error : %s", err)
		}
	}
	broker.Close()
	if debug && cserver != nil {
		log.Printf("Got server instance for server :%d ", cserver.Pid())
	}
	return cserver
}

// Internal server to delete the server entry from the broker
func delServer(pid int, debug bool) {
	broker := getBrokerConnection()
	var reply int
	err := broker.Call("Broker.Del", pid, &reply)
	if err != nil {
		if debug {
			log.Printf("server error : %s", err)
		}
	}
	broker.Close()
}

// Check if server exists
func Exists(pid int, debug bool) bool {
	if getServer(pid, debug) != nil {
		return true
	} else {
		return false
	}
}

// Get count of existing servers
func Count() int {
	broker := getBrokerConnection()
	var reply int
	err := broker.Call("Broker.Count", -1, &reply)
	if err != nil {
		log.Printf("Broker error")
	}
	return reply
}
