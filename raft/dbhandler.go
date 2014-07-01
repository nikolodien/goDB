package raft

import (
	"bytes"
	leveldb "code.google.com/p/go-leveldb"
	"encoding/gob"
	"log"
	"strconv"
)

func dbHandler(rserver *rServer, debug bool) {
	// Setup the writer
	dbname := strconv.Itoa(rserver.Pid) + "_db"

	env := leveldb.NewDefaultEnv()
	cache := leveldb.NewLRUCache(1 << 20)

	// Options for the database
	options := leveldb.NewOptions()
	options.SetErrorIfExists(false)
	options.SetCache(cache)
	options.SetEnv(env)
	options.SetInfoLog(nil)
	options.SetWriteBufferSize(1 << 20)
	options.SetParanoidChecks(false)
	options.SetMaxOpenFiles(10)
	options.SetBlockSize(1024)
	options.SetBlockRestartInterval(8)
	options.SetCompression(leveldb.NoCompression)

	roptions := leveldb.NewReadOptions()
	roptions.SetVerifyChecksums(true)
	roptions.SetFillCache(false)

	woptions := leveldb.NewWriteOptions()
	woptions.SetSync(true)
	options.SetCreateIfMissing(true)

	db, err := leveldb.Open(dbname, options)
	if err != nil {
		log.Printf("Open failed: %v", err)
		return
	}

	if db != nil {
		defer db.Close()
	}
	defer options.Close()
	defer roptions.Close()
	defer woptions.Close()
	defer cache.Close()
	defer env.Close()

outer:
	for {
		select {
		// Handle write requests
		case logEntry := <-rserver.Writer:
			if debug {
				log.Printf("Received write entry for server %d", rserver.Pid)
				log.Printf("Putting in the database logEntry with index %d for server %d", logEntry.Index, rserver.Pid)
			}

			// Encode the Index in []bytes
			// Create a new bytes.Buffer for gob encoder
			buffer := new(bytes.Buffer)
			// Create new gob encoder
			enc := gob.NewEncoder(buffer)
			// encode the struct
			err := enc.Encode(logEntry.Index)
			if err != nil {
				log.Fatal("encode:", err)
			}
			// Create a bytes array
			putKey := make([]byte, buff_size)
			// Now read the contents of buffer into a byte array
			_, err = buffer.Read(putKey)
			// if debug {
			// 	log.Printf("Wrote %d bytes", n)
			// }
			if err != nil {
				log.Fatal("read", err)
			}

			// Encode the serverLogData in []bytes
			// Create a new bytes.Buffer for gob encoder
			buffer = new(bytes.Buffer)
			// Create new gob encoder
			enc = gob.NewEncoder(buffer)
			// encode the struct
			err = enc.Encode(logEntry.ServerLogData)
			if err != nil {
				log.Fatal("encode:", err)
			}
			// Create a bytes array
			putValue := make([]byte, buff_size)
			// Now read the contents of buffer into a byte array
			_, err = buffer.Read(putValue)
			// if debug {
			// 	log.Printf("Wrote %d bytes", n)
			// }
			if err != nil {
				log.Fatal("read", err)
			}

			// Put the entry in the db
			err = db.Put(woptions, putKey, putValue)
			if debug {
				if err != nil {
					log.Printf("Put failed: %v", err)
				}
			}

			// check whether the write was succesful
			if CheckGet("after Put", db, roptions, putKey, putValue, debug) {
				rserver.WriteSuccess <- true
			} else {
				rserver.WriteSuccess <- false
			}
			
		// Handle read requests
		case readRequest := <-rserver.ReaderRequest:
			if debug {
				log.Printf("Received read request for server %d", rserver.Pid)
				log.Printf("Reading from database logEntry with index %d for server %d", readRequest.Index, rserver.Pid)
			}

			var logEntry serverLogEntry
			logEntry.Index = readRequest.Index

			// Encode the Index in []bytes
			// Create a new bytes.Buffer for gob encoder
			buffer := new(bytes.Buffer)
			// Create new gob encoder
			enc := gob.NewEncoder(buffer)
			// encode the struct
			err := enc.Encode(readRequest.Index)
			if err != nil {
				log.Fatal("encode:", err)
			}
			// Create a bytes array
			getKey := make([]byte, buff_size)
			// Now read the contents of buffer into a byte array
			_, err = buffer.Read(getKey)
			// if debug {
			// 	log.Printf("Wrote %d bytes", n)
			// }
			if err != nil {
				log.Fatal("read", err)
			}

			getValue, err := db.Get(roptions, getKey)
			if err != nil {
				if debug {
					log.Printf("%s, Get failed: %v", rserver.Pid, err)
				}
				return
			}

			// Decode the command
			buffer = new(bytes.Buffer)
			buffer.Write(getValue)
			decoder := gob.NewDecoder(buffer)
			err = decoder.Decode(&logEntry.ServerLogData)
			if err != nil {
				if debug {
					log.Printf("decode error (%s) while reading from logEntry with index %d for server %d", err, readRequest.Index, rserver.Pid)
				}
				rserver.ReaderData <- nil
			} else {
				rserver.ReaderData <- &logEntry
			}
		// Close dbHandler
		case signal := <-rserver.HandlerClose:
			if signal {
				break outer
			}
		}
	}
	if debug {
		log.Printf("Closing connection to db %d", rserver.Pid)
	}
}

func CheckGet(where string, db *leveldb.DB, roptions *leveldb.ReadOptions, key, expected []byte, debug bool) bool {
	getValue, err := db.Get(roptions, key)

	if err != nil {
		if debug {
			log.Printf("%s, Get failed: %v", where, err)
		}
		return false
	}
	if !bytes.Equal(getValue, expected) {
		if debug {
			log.Printf("%s, expected Get value %v, got %v", where, expected, getValue)
		}
		return false
	}

	return true
}
