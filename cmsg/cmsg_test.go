package cmsg

import (
	zmq "github.com/pebbe/zmq4"

	"os"
	"testing"
)

func TestCmsg(t *testing.T) {

	//  Prepare our context and sockets
	output, err := zmq.NewSocket(zmq.DEALER)
	if err != nil {
		t.Error(err)
	}

	err = output.Bind("ipc://cmsg_selftest.ipc")
	if err != nil {
		t.Error(err)
	}

	input, err := zmq.NewSocket(zmq.DEALER)
	if err != nil {
		t.Error(err)
	}

	err = input.Connect("ipc://cmsg_selftest.ipc")
	if err != nil {
		t.Error(err)
	}

	kvmap := make(map[string]*Cmsg)

	//  Test send and receive of simple message
	cmsg := NewCmsg(1)
	cmsg.SetKey("key")
	cmsg.SetBody("body")
	cmsg.Dump()
	err = cmsg.Send(output)

	cmsg.Store(kvmap)
	if err != nil {
		t.Error(err)
	}

	cmsg, err = RecvCmsg(input)
	if err != nil {
		t.Error(err)
	}
	cmsg.Dump()
	key, err := cmsg.GetKey()
	if err != nil {
		t.Error(err)
	}
	if key != "key" {
		t.Error("Expected \"key\", got \"" + key + "\"")
	}
	cmsg.Store(kvmap)

	input.Close()
	output.Close()
	os.Remove("cmsg_selftest.ipc")
}
