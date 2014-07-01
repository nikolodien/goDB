package kvstore

import (
	"fmt"
	"log"
	"net/rpc"
	"testing"
	"time"
)

const (
	MAX = 100
)

func TestKVStore(t *testing.T) {

	var debug bool = false

	New()

	time.Sleep(time.Second * 2)

	client, err := rpc.DialHTTP("tcp", "localhost:"+PORT)
	if err != nil {
		log.Fatal("dialing:", err)
	}

	var start int64 = time.Now().Unix()
	var end int64 = start + MAX

	var committedPuts [MAX]bool
	// Commands to put data in the kv store
	for i, j := start, 0; i < end; i, j = i+1, j+1 {
		time.Sleep(time.Millisecond)
		key := fmt.Sprintf("%v", i)
		keyValue := KeyValue{key, i}
		var reply int
		err = client.Call("KVStore.Put", keyValue, &reply)
		if err != nil {
			log.Printf("server error:%v", err)
			committedPuts[j] = false
		} else {
			committedPuts[j] = true
		}
	}

	time.Sleep(time.Second * 10)

	// Get values now
	for i, j := start, 0; i < end; i, j = i+1, j+1 {
		key := fmt.Sprintf("%v", i)
		if committedPuts[j] {
			var val Val
			var key = key
			err = client.Call("KVStore.Get", key, &val)
			if err != nil {
				log.Printf("server error:%v", err)
			}
			if debug {
				log.Printf("value for %s is %v", key, val.Value)
			}
			if val.Value != nil {
				switch value := val.Value.(type) {
				case int64:
					if value != i {
						t.Error("Value mismatch: expected:%v, got:%v", i, value)
					}
				}
			}
		}
	}

	var committedDels [MAX]bool
	// Commands to put data in the kv store
	for i, j := start, 0; i < end; i, j = i+1, j+1 {
		time.Sleep(time.Millisecond)
		key := fmt.Sprintf("%v", i)
		var reply int
		err = client.Call("KVStore.Del", key, &reply)
		if err != nil {
			log.Printf("server error:%v", err)
			committedDels[j] = false
		} else {
			committedDels[j] = true
		}
	}

	time.Sleep(time.Second * 10)

	// Get values now
	for i, j := start, 0; i < end; i, j = i+1, j+1 {
		key := fmt.Sprintf("%v", i)
		if committedDels[j] {
			var val Val
			var key = key
			err = client.Call("KVStore.Get", key, &val)
			if err != nil {
				log.Printf("server error:%v", err)
			}
			if debug {
				log.Printf("value for %s is %v", key, val.Value)
			}
			if val.Value != nil {
				t.Error("Value mismatch: expected:nil, got:", val.Value)
			}
		}
	}

	Close()
}
