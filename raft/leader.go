package raft

import (
	"github.com/nikolodien/goDB/cluster"
	"log"
	"time"
)

func leaderFunc(pid int, rserver *rServer, channelClose chan bool, inbox chan *cluster.Envelope, outbox chan *cluster.Envelope, peers []int, debug bool) {
	// var nextIndex = make([]int, len(peers))
	// var matchIndex = make([]int, len(peers))
innerl:
	for {
		select {
		case <-time.After(time.Millisecond * heartbeatInterval):
			// Add the PrevLogTerm in the appendEntries message
			var PrevLogTerm int = DEFAULT_TERM
			// Read Previous logEntry to get the PrevLogTerm
			rserver.ReaderRequest <- &ReadRequest{Index: rserver.CommitIndex - 1}
			// Wait for the logEntry to be read
			select {
			case serverLogEntry := <-rserver.ReaderData:
				if debug {
					log.Printf("Leader %d read logEntry %v", rserver.Pid, serverLogEntry)
				}
				if serverLogEntry != nil {
					PrevLogTerm = serverLogEntry.ServerLogData.Term
				}
			}
			appendEntries := AppendEntries{Term: rserver.CurrentTerm, LeaderId: pid, PrevLogIndex: rserver.CommitIndex - 1, PrevLogTerm: PrevLogTerm, Entries: nil, LeaderCommit: rserver.CommitIndex}
			for peer := range peers {
				peerId := peers[peer]
				if debug {
					log.Printf("Leader %d with term %d Sending alive message to server %d", pid, rserver.CurrentTerm, peerId)
				}
				outbox <- &cluster.Envelope{Pid: peerId, MsgId: int64(rserver.CurrentTerm), Msg: appendEntries}
			}
		case message := <-inbox:
			switch msg := message.Msg.(type) {
			// If a server with higher term comes up then step down as a follwer
			case AppendEntries:
				if msg.Term > rserver.CurrentTerm {
					if debug {
						log.Printf("Server %d with term %d is changing state from leader to follower", pid, rserver.CurrentTerm)
					}
					rserver.RSLeader = false
					rserver.State = FOLLOWER
					inbox <- message
					break innerl
				}
			case AppendEntriesStatus:
				if debug {
					log.Printf("leader server %d with term %d Received status message from %d with term %d", pid, rserver.CurrentTerm, message.Pid, msg.Term)
				}
				if msg.Term >= rserver.CurrentTerm {
					if debug {
						log.Printf("Server %d with term %d is changing state from leader to follower", pid, rserver.CurrentTerm)
					}
					rserver.RSLeader = false
					rserver.State = FOLLOWER
					break innerl
				}
			case RequestVote:
				if msg.Term >= rserver.CurrentTerm && msg.Term != rserver.VotedFor {
					if debug {
						log.Printf("Leader %d with term %d is changing state from leader to follower", pid, rserver.CurrentTerm)
					}
					if debug {
						log.Printf("Leader %d with term %d is voting for server %d with term %d", pid, rserver.CurrentTerm, message.Pid, message.MsgId)
					}
					outbox <- &cluster.Envelope{Pid: message.Pid, MsgId: int64(rserver.CurrentTerm), Msg: ElectionStatusMessage{Term: rserver.CurrentTerm, Granted: true}}
					rserver.RSLeader = false
					rserver.State = FOLLOWER
					rserver.VotedFor = msg.Term
					break innerl
				} else {
					outbox <- &cluster.Envelope{Pid: message.Pid, MsgId: int64(rserver.CurrentTerm), Msg: ElectionStatusMessage{Term: rserver.CurrentTerm, Granted: false}}
				}
			}
		// Received command from client
		case message := <-rserver.Outer:
			// Create a new Log Entry
			if debug {
				log.Printf("Received message %v", message)
			}
			logEntry := serverLogEntry{Index: rserver.CommitIndex + 1, ServerLogData: serverLogData{Term: rserver.CurrentTerm, Command: message}}
			if debug {
				log.Printf("CommitIndex of leader is %d", rserver.CommitIndex)
				log.Printf("Created logEntry (%v) for message", logEntry)
			}
			rserver.Writer <- &logEntry

			// Used to determine the success of follower commits
			var count = 0

			// Wait for the write to be successful
			select {
			case status := <-rserver.WriteSuccess:
				if status {
					count = count + 1
					if debug {
						log.Printf("Write succesful")
					}
				}
			}
			// Add the PrevLogTerm in the appendEntries message
			var PrevLogTerm int
			// Read Previous logEntry to ensure that it is in sync with the leader
			rserver.ReaderRequest <- &ReadRequest{Index: rserver.CommitIndex}
			// Wait for the logEntry to be read
			select {
			case serverLogEntry := <-rserver.ReaderData:
				if debug {
					log.Printf("Leader %d read logEntry %v", rserver.Pid, serverLogEntry)
				}
				if serverLogEntry != nil {
					PrevLogTerm = serverLogEntry.ServerLogData.Term
				}
			}
			// Now replicate this logEntry on all the followers
			for peer := range peers {
				peerId := peers[peer]
				entries := []serverLogEntry{logEntry}
				if debug {
					log.Printf("Leader %d with term %d is sending entries to follower %d", pid, rserver.CurrentTerm, peerId)
				}
				appendEntries := AppendEntries{Term: rserver.CurrentTerm, LeaderId: pid, PrevLogIndex: rserver.CommitIndex, PrevLogTerm: PrevLogTerm, Entries: entries, LeaderCommit: rserver.CommitIndex}
				outbox <- &cluster.Envelope{Pid: peerId, MsgId: int64(rserver.CurrentTerm), Msg: appendEntries}
			}

			// Wait for write success on majority
		checkCommit:
			for {
				select {
				case message := <-inbox:
					switch msg := message.Msg.(type) {
					case AppendEntriesStatus:
						if msg.Term > rserver.CurrentTerm {
							if debug {
								log.Printf("Server %d with term %d is changing state from leader to follower", pid, rserver.CurrentTerm)
							}
						} else {
							if msg.Success {
								if count < cluster.Count() {
									count = count + 1
									// Majority of followers were able to log
									if count >= cluster.Count()/2 {
										// The commit was successful on all the followers
										// Tell the client that the command has been written to the log with the index given in logEntry
										if debug {
											log.Printf("Sending success of write to client")
										}
										// Create a Log Entry for the Client
										clogEntry := LogEntry{Index: logEntry.Index, Command: logEntry.ServerLogData.Command}
										rserver.Inner <- &clogEntry
										break checkCommit
									}
								}
							}
						}
					case RequestVote:
						if msg.Term >= rserver.CurrentTerm && msg.Term != rserver.VotedFor {
							if debug {
								log.Printf("Leader %d with term %d is changing state from leader to follower", pid, rserver.CurrentTerm)
							}
							if debug {
								log.Printf("Leader %d with term %d is voting for server %d with term %d", pid, rserver.CurrentTerm, message.Pid, message.MsgId)
							}
							outbox <- &cluster.Envelope{Pid: message.Pid, MsgId: int64(rserver.CurrentTerm), Msg: ElectionStatusMessage{Term: rserver.CurrentTerm, Granted: true}}
							rserver.RSLeader = false
							rserver.State = FOLLOWER
							rserver.VotedFor = msg.Term
							break innerl
						} else {
							outbox <- &cluster.Envelope{Pid: message.Pid, MsgId: int64(rserver.CurrentTerm), Msg: ElectionStatusMessage{Term: rserver.CurrentTerm, Granted: false}}
						}
					case AppendEntries:
						if msg.Term > rserver.CurrentTerm {
							if debug {
								log.Printf("Leader %d with term %d is changing state from leader to follower", pid, rserver.CurrentTerm)
							}
							rserver.RSLeader = false
							rserver.State = FOLLOWER
							inbox <- message
							break innerl
						}
					}
					// Retry after waiting for sometime
					// case time.After(time.Second):
					// break checkCommit
				}
			}
			// Increment the commitIndex
			if debug {
				log.Printf("Incrementing commitIndex to %d", rserver.CommitIndex+1)
			}
			rserver.CommitIndex = rserver.CommitIndex + 1
			appendEntries := AppendEntries{Term: rserver.CurrentTerm, LeaderId: pid, PrevLogIndex: rserver.CommitIndex, PrevLogTerm: PrevLogTerm, Entries: nil, LeaderCommit: rserver.CommitIndex}
			for peer := range peers {
				peerId := peers[peer]
				if debug {
					log.Printf("Leader %d with term %d Sending alive message to server %d", pid, rserver.CurrentTerm, peerId)
				}
				outbox <- &cluster.Envelope{Pid: peerId, MsgId: int64(rserver.CurrentTerm), Msg: appendEntries}
			}
			kvCommand := logEntry.ServerLogData.Command
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
			rserver.LastApplied = rserver.CommitIndex
		case <-channelClose:
			rserver.State = EXIT
			break innerl
		}
	}
}
