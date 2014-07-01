package raft

import (
	"bufio"
	"encoding/gob"
	"fmt"
	"github.com/nikolodien/goDB/cluster"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"
)

const (
	chanlimit = 100
	// election timeout constants in milliseconds
	minTimeout        = 150
	maxTimeout        = 300 // in milliseconds
	heartbeatInterval = 80
	DEFAULT_TERM      = 0
	FOLLOWER          = 1
	LEADER            = 2
	CANDIDATE         = 3
	EXIT              = 4
	INVALID_INDEX     = 0
	buff_size         = 1000
	// Command
	GET    = 0
	PUT    = 1
	DEL = 2
)

type RaftServer interface {
	Term() int

	IsLeader() bool
	// Mailbox for state machine layer above to send commands of any
	// kind, and to have them replicated by raft.  If the server is not
	// the leader, the message will be silently dropped.
	Outbox() chan<- interface{}

	// Mailbox for state machine layer above to receive commands. These
	// are guaranteed to have been replicated on a majority
	Inbox() <-chan *LogEntry

	// Remove items from 0 .. index (inclusive), and reclaim disk
	// space. This is a hint, and there's no guarantee of immediacy since
	// there may be some servers that are lagging behind).
	DiscardUpto(index int64)

	// get the state machine of the server
	Store() map[string]interface{}
}

// Identifies an entry in the log
type LogEntry struct {
	// An index into an abstract 2^64 size array
	Index int64
	// The data that was supplied to raft's inbox
	Command interface{}
}

// Server version of logEntry
type serverLogEntry struct {
	// An index into an abstract 2^64 size array
	Index int64
	// Server Log Data
	ServerLogData serverLogData
}

type serverLogData struct {
	// An index into an abstract 2^64 size array
	Term int
	// The data that was supplied to raft's inbox
	Command interface{}
}

// Structure of the Raft Server
type rServer struct {
	// Identifier of the server
	Pid int

	// Persistent state
	CurrentTerm int
	VotedFor    int
	// This will be stored in the levelDB database
	// Log         []serverLogEntry

	// Volatile state
	CommitIndex int64
	// For startup purpose
	StartIndex  int64
	LastApplied int64

	// Leader and State info
	RSLeader bool
	State    int

	// Channel to close
	ChannelClose chan bool

	// Milliseconds
	Timeout int

	// Outbox for others to send messages
	Outer chan interface{}
	// Inbox to read messages
	Inner chan *LogEntry

	// Channels for the db Handler of each server
	// Write Handle
	Writer chan *serverLogEntry
	// Channel for the server to know whether the writer was successful
	WriteSuccess chan bool
	// Channel to request read from db
	ReaderRequest chan *ReadRequest
	// Channel for the reader to put data
	ReaderData chan *serverLogEntry
	// Channel to close the db Handler
	HandlerClose chan bool

	// State Machine of the server
	StateMachine map[string]interface{}
}

// Reader request
type ReadRequest struct {
	Index int64
}

// State Machine Command
type KVCommand struct {
	Command int
	Key     string
	Value   interface{}
}

// Election message
type RequestVote struct {
	Term         int
	CandidateId  int
	LastLogIndex int64
	LastLogTerm  int
}

type AppendEntries struct {
	Term         int
	LeaderId     int
	PrevLogIndex int64
	PrevLogTerm  int
	Entries      []serverLogEntry
	LeaderCommit int64
}

type ElectionStatusMessage struct {
	Term    int
	Granted bool
}

type AppendEntriesStatus struct {
	Term    int
	Success bool
}

// register gob types
func init() {
	gob.Register(KVCommand{})
	gob.Register(serverLogData{})
	gob.Register(RequestVote{})
	gob.Register(AppendEntries{})
	gob.Register(ElectionStatusMessage{})
	gob.Register(AppendEntriesStatus{})
}

// Implements the Term() function
func (rserver *rServer) Term() int {
	return rserver.CurrentTerm
}

// Implements the isLeader() function
func (rserver *rServer) IsLeader() bool {
	return rserver.RSLeader
}

// Implements the Outbox() function
func (rserver *rServer) Outbox() chan<- interface{} {
	return rserver.Outer
}

// Implements the Inbox() function
func (rserver *rServer) Inbox() <-chan *LogEntry {
	return rserver.Inner
}

// Implements the Inbox() function
func (rserver *rServer) DiscardUpto(index int64) {
	return
}

// Implements the Store() function
func (rserver *rServer) Store() map[string]interface{}{
	return rserver.StateMachine
}

// function to write persistent state to file
func persistData(rserver *rServer) error {
	file, err := os.Create(strconv.Itoa(rserver.Pid) + ".txt")
	if err != nil {
		return err
	}

	defer file.Close()

	w := bufio.NewWriter(file)
	fmt.Fprintln(w, rserver.CurrentTerm)
	fmt.Fprintln(w, rserver.VotedFor)
	fmt.Fprintln(w, rserver.CommitIndex)
	fmt.Fprintln(w, rserver.StartIndex)

	return w.Flush()
}

// function to read persistent state from file
func retrieveData(rserver *rServer) error {
	file, err := os.Open(strconv.Itoa(rserver.Pid) + ".txt")
	if err != nil {
		rserver.CurrentTerm = DEFAULT_TERM
		rserver.CommitIndex = 0
		rserver.VotedFor = -1
		return err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)

	if scanner.Scan() {
		line := scanner.Text()
		rserver.CurrentTerm, err = strconv.Atoi(string(line))
		if err != nil {
			return nil
		}
	}
	if scanner.Scan() {
		line := scanner.Text()
		rserver.VotedFor, err = strconv.Atoi(string(line))
		if err != nil {
			return nil
		}
	}
	if scanner.Scan() {
		line := scanner.Text()
		index, err := strconv.Atoi(string(line))
		rserver.CommitIndex = int64(index)
		if err != nil {
			return nil
		}
	}
	if scanner.Scan() {
		line := scanner.Text()
		startIndex, err := strconv.Atoi(string(line))
		rserver.StartIndex = int64(startIndex)
		if err != nil {
			return nil
		}
	}
	return scanner.Err()
}

// The main server routine
func serverRoutine(pid int, rserver *rServer, config string, channelClose chan bool, debug bool) {

	// Retrieve term,votedFor,start and commit Index for this server
	err := retrieveData(rserver)

	// Apply the commands in the log to the state machine
	for index := rserver.StartIndex; index <= rserver.CommitIndex; index++ {
		rserver.ReaderRequest <- &ReadRequest{Index: index}
		// Wait for the logEntry to be read

		select {
		case serverLogEntry := <-rserver.ReaderData:
			if debug {
				log.Printf("Server %d read logEntry %v for index %v", rserver.Pid, serverLogEntry, index)
			}
			if serverLogEntry != nil {
				if debug {
					log.Printf("Applying %v to StateMachine of server %d", serverLogEntry.ServerLogData.Command, pid)
				}
				kvCommand := serverLogEntry.ServerLogData.Command
				switch cmd := kvCommand.(type) {
				case KVCommand:
					switch cmd.Command {
					case PUT:
						rserver.StateMachine[cmd.Key] = cmd.Value
					case DEL:
						delete(rserver.StateMachine, cmd.Key)
					case GET:
						// No action needs to be performed for GET
					}
				}
				rserver.LastApplied = index
			}
		}
	}
	// Check if server already exists
	if cluster.Exists(pid, debug) {
		if debug {
			log.Printf("Server %d already exists", pid)
		}
		// If a server is already present, then return
		return
	}

	// This will start a new server
	cserver := cluster.New(pid, config, debug)
	// A server starts of as a follower
	rserver.State = FOLLOWER
	if err != nil {
		if debug {
		log.Printf("Error in Server %d: %v", rserver.Pid, err)
	}
	}
	// Print server timeout
	if debug {
		log.Printf("Timeout for server with pid %d is %d", pid, rserver.Timeout)
	}
	if err != nil {
		if debug {
			log.Printf("Error while reading persistant data for %d", pid)
		}
		// Set the term to the default value
		rserver.CurrentTerm = DEFAULT_TERM
	}
outer:
	for {
		// A server will be in one of the three states at any moment
		switch rserver.State {
		// Describes what the server will do when in the follower state
		case FOLLOWER:
			if debug {
				log.Printf("Server %d with term %d is executing as follower", pid, rserver.CurrentTerm)
			}
			followerFunc(pid, rserver, channelClose, cserver.Inbox(), cserver.Outbox(), debug)
		// Describes the behavior of a candidate
		case CANDIDATE:
			if debug {
				log.Printf("Server %d with term %d is executing as candidate", pid, rserver.CurrentTerm)
			}
			candidateFunc(pid, rserver, channelClose, cserver.Inbox(), cserver.Outbox(), cserver.Peers(), debug)
		// Describes the behavior of a leader
		case LEADER:
			if debug {
				log.Printf("Server %d with term %d is executing as leader", pid, rserver.CurrentTerm)
			}
			leaderFunc(pid, rserver, channelClose, cserver.Inbox(), cserver.Outbox(), cserver.Peers(), debug)
		case EXIT:
			if debug {
				log.Printf("Server %d with term %d is preparing to shutdown", pid, rserver.State)
			}
			// close the db writer
			rserver.HandlerClose <- true
			if debug {
				log.Printf("Sending close signal to dbHandler")
			}
			break outer
		}
	}
	err = persistData(rserver)
	if err != nil {
		log.Printf("Error in Server %d: %v", rserver.Pid, err)
	}
	rserver.RSLeader = false
	cluster.Close(cserver, debug)
	// time.Sleep(time.Millisecond)
	if debug {
		log.Printf("Shutting down server with id %d", pid)
	}
}

func New(id int, config string, debug bool) *rServer {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	var timeout int = 0
	for timeout < minTimeout {
		timeout = r.Intn(maxTimeout)
	}
	server := &rServer{Pid: id, RSLeader: false, ChannelClose: make(chan bool, chanlimit), Timeout: timeout, Outer: make(chan interface{}, chanlimit), Inner: make(chan *LogEntry, chanlimit), Writer: make(chan *serverLogEntry, chanlimit), WriteSuccess: make(chan bool, chanlimit), ReaderRequest: make(chan *ReadRequest, chanlimit), ReaderData: make(chan *serverLogEntry, chanlimit), HandlerClose: make(chan bool, chanlimit), StateMachine: make(map[string]interface{})}
	go serverRoutine(id, server, config, server.ChannelClose, debug)
	go dbHandler(server, debug)
	return server
}

// Used to shutdown a server
func Close(rserver *rServer) {
	rserver.ChannelClose <- true
}

// Used to restart the server
func Restart(id int, config string, rserver *rServer, debug bool) *rServer {
	rserver.ChannelClose <- true
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	var timeout int = 0
	for timeout < minTimeout {
		timeout = r.Intn(maxTimeout)
	}
	server := &rServer{Pid: id, RSLeader: false, ChannelClose: make(chan bool, chanlimit), Timeout: timeout, Outer: make(chan interface{}, chanlimit), Inner: make(chan *LogEntry, chanlimit), Writer: make(chan *serverLogEntry, chanlimit), WriteSuccess: make(chan bool, chanlimit), ReaderRequest: make(chan *ReadRequest, chanlimit), ReaderData: make(chan *serverLogEntry, chanlimit), HandlerClose: make(chan bool, chanlimit), StateMachine: make(map[string]interface{})}
	go serverRoutine(id, server, config, server.ChannelClose, debug)
	go dbHandler(server, debug)
	return server
}

// Used to check for existence of server
func Exists(pid int, debug bool) bool {
	if cluster.Exists(pid, debug) {
		return true
	} else {
		return false
	}
}
