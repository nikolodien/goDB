package raft

import (
	"github.com/nikolodien/goDB/cluster"
	"log"
	"time"
)

func followerFunc(pid int, rserver *rServer, channelClose chan bool, inbox chan *cluster.Envelope, outbox chan *cluster.Envelope, debug bool) {
innerf:
	for {
		select {
		// Internal messages
		case message := <-inbox:
			switch msg := message.Msg.(type) {
			case RequestVote:
				if debug {
					log.Printf("server %d with term %d received requestVote message from server %d with term %d", pid, rserver.CurrentTerm, message.Pid, msg.Term)
				}
				if msg.Term >= rserver.CurrentTerm && msg.Term > rserver.VotedFor {
					// Vote for this candidate
					if debug {
						log.Printf("server %d with term %d is voting for server %d with term %d", pid, rserver.CurrentTerm, message.Pid, msg.Term)
					}
					outbox <- &cluster.Envelope{Pid: message.Pid, MsgId: int64(rserver.CurrentTerm), Msg: ElectionStatusMessage{Term: rserver.CurrentTerm, Granted: true}}
					rserver.VotedFor = msg.Term
				} else {
					outbox <- &cluster.Envelope{Pid: message.Pid, MsgId: int64(rserver.CurrentTerm), Msg: ElectionStatusMessage{Term: rserver.CurrentTerm, Granted: false}}
				}
			case AppendEntries:

				// Now this follower knows that a leader is up
				// Check if the term of this leader is less than this server's term
				if !(msg.Term >= rserver.CurrentTerm) {
					// The term of the current leader is less than this server's term
					// This server should become a candidate
					rserver.CurrentTerm++
					// Changet to candidate state
					rserver.State = CANDIDATE
					if debug {
						log.Printf("Server %d with term %d is changing state from follower to candidate", pid, rserver.CurrentTerm)
					}
					break innerf
				} else {
					rserver.CurrentTerm = msg.Term
					// Set this to msg.PrevLogTerm to consider the case of the server just starting up
					var PrevLogTerm int = msg.PrevLogTerm
					// Update to leader's commitIndex
					// (Makes use of piggy backing)
					if rserver.CommitIndex < msg.LeaderCommit {
						// Now apply logEntries from rserver.CommitIndex to msg.LeaderCommit
						// to the state machine
						for i := rserver.CommitIndex + 1; i <= msg.LeaderCommit; i = i + 1 {

							// Read Previous logEntry to ensure that it is in sync with the leader
							rserver.ReaderRequest <- &ReadRequest{Index: i}
							// Wait for the logEntry to be read
							select {
							case serverLogEntry := <-rserver.ReaderData:
								if debug {
									log.Printf("Follower %d read logEntry %v", rserver.Pid, serverLogEntry)
								}
								if serverLogEntry != nil {
									if debug {
										log.Printf("Updating the commitIndex for follower %d to %d", rserver.Pid, msg.LeaderCommit)
									}
									rserver.CommitIndex = i
									// Store the term of the serverLogEntry
									PrevLogTerm = serverLogEntry.ServerLogData.Term
									// Apply the logEntry to the state machine now
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
									rserver.LastApplied = i
								}
							}
						}
					}

					// Append LogEntries
					if len(msg.Entries) > 0 {
						if PrevLogTerm == msg.PrevLogTerm {
							if debug {
								log.Printf("Server %d received following entries to be appended to the log from leader %d ", pid, message.Pid)
								for index := range msg.Entries {
									log.Printf("%v\n", msg.Entries[index])
								}
							}
							// Only one log Entry is committed at the moment
							rserver.Writer <- &msg.Entries[0]
							// Wait for the write to be successful
							select {
							case status := <-rserver.WriteSuccess:
								if status {
									if debug {
										log.Printf("Write succesful on follower %d", pid)
									}
									// Send status message to the leader
									outbox <- &cluster.Envelope{Pid: message.Pid, MsgId: int64(rserver.CurrentTerm), Msg: AppendEntriesStatus{Term: rserver.CurrentTerm, Success: true}}
								} else {
									if debug {
										log.Printf("Write unsuccesful on follower %d", pid)
									}
									// Send status message to the leader
									outbox <- &cluster.Envelope{Pid: message.Pid, MsgId: int64(rserver.CurrentTerm), Msg: AppendEntriesStatus{Term: rserver.CurrentTerm, Success: false}}
								}
							}
						} else {
							// Do not write new log entries as the follower is now in an inconsistent state
						}
					}
				}
			}
		// This message is ignored by followers at the moment
		case message := <-rserver.Outer:
			// Ignore it
			if debug {
				log.Printf("Follower %d received message %v from client", rserver.Pid, message)
			}
		// If it doesn't receive any message for a certain Timeout interval,
		// it becomes a candidate and stands for election
		case <-time.After(time.Duration(int(time.Millisecond) * rserver.Timeout)):
			// Increment term
			rserver.CurrentTerm++
			// Change to candidate state
			rserver.State = CANDIDATE
			if debug {
				log.Printf("Server %d with term %d is changing state from follower to candidate", pid, rserver.CurrentTerm)
			}
			break innerf
		case <-channelClose:
			rserver.State = EXIT
			break innerf
		}
	}
}
