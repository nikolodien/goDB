package raft

import (
	"github.com/nikolodien/goDB/cluster"
	"log"
	"time"
)

func candidateFunc(pid int, rserver *rServer, channelClose chan bool, inbox chan *cluster.Envelope, outbox chan *cluster.Envelope, peers []int, debug bool) {
	var LastLogTerm int
	// Read Previous logEntry to ensure that it is in sync with the leader
	rserver.ReaderRequest <- &ReadRequest{Index: rserver.CommitIndex}
	// Wait for the logEntry to be read
	select {
	case serverLogEntry := <-rserver.ReaderData:
		if debug {
			log.Printf("Candidate %d read logEntry %v", rserver.Pid, serverLogEntry)
		}
		if serverLogEntry != nil {
			LastLogTerm = serverLogEntry.ServerLogData.Term
		} else {
			LastLogTerm = DEFAULT_TERM
		}
	}
	requestVote := RequestVote{Term: rserver.CurrentTerm, CandidateId: pid, LastLogIndex: rserver.CommitIndex, LastLogTerm: LastLogTerm}
	// Send request to become a leader to all the peers
	for peer := range peers {
		peerId := peers[peer]
		if debug {
			log.Printf("Server %d with term %d is sending request message to server %d", pid, rserver.CurrentTerm, peerId)
		}
		outbox <- &cluster.Envelope{Pid: peerId, MsgId: int64(rserver.CurrentTerm), Msg: requestVote}
	}
	// It will now wait for response from all the peers
	var count int = 1 // Assume server votes for itself
innerc:
	for {
		select {
		case message := <-inbox:
			switch msg := message.Msg.(type) {
			case ElectionStatusMessage:
				if msg.Granted {
					if debug {
						log.Printf("server %d with term %d got vote from server %d with term %d", pid, rserver.CurrentTerm, message.Pid, message.MsgId)
					}
					count++
					if count > cluster.Count()/2 {
						// The candidate has won the election
						// It should now act a leader and send alive messages to everyone
						if debug {
							log.Printf("server %d with term %d has now become a leader", pid, rserver.CurrentTerm)
						}
						rserver.CurrentTerm++
						rserver.State = LEADER
						rserver.RSLeader = true
						break innerc
					}
				}
			case AppendEntries:
				// A new leader was elected
				if msg.Term >= rserver.CurrentTerm {
					// Change state to follower
					if debug {
						log.Printf("Server %d with term %d is changing state from candidate to follower", pid, rserver.CurrentTerm)
					}
					rserver.State = FOLLOWER
					inbox <- message
					break innerc
				}
			case RequestVote:
				if msg.Term >= rserver.CurrentTerm && msg.Term > rserver.VotedFor {
					if debug {
						log.Printf("Server %d with term %d is changing state from candidate to follower", pid, rserver.CurrentTerm)
					}
					if debug {
						log.Printf("server %d with term %d is voting for server %d with term %d", pid, rserver.CurrentTerm, message.Pid, message.MsgId)
					}
					outbox <- &cluster.Envelope{Pid: message.Pid, MsgId: int64(rserver.CurrentTerm), Msg: ElectionStatusMessage{Term: rserver.CurrentTerm, Granted: true}}
					rserver.State = FOLLOWER
					rserver.VotedFor = msg.Term
					break innerc
				} else {
					outbox <- &cluster.Envelope{Pid: message.Pid, MsgId: int64(rserver.CurrentTerm), Msg: ElectionStatusMessage{Term: rserver.CurrentTerm, Granted: false}}
				}
			case LogEntry:
				// This message is ignored by candidates at the moment
			}
		case <-time.After(time.Duration(int(time.Millisecond) * rserver.Timeout)):
			rserver.State = CANDIDATE
			rserver.CurrentTerm++
		case <-channelClose:
			rserver.State = EXIT
			break innerc
		}
	}
}
