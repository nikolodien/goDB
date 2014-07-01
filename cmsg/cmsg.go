//  cmsg - simple key-value message class for example applications.

package cmsg

import (
	zmq "github.com/pebbe/zmq4"

	"errors"
	"fmt"
	"os"
)

const (
	frame_KEY   = 0
	frame_SEQ   = 1
	frame_BODY  = 2
	cmsg_FRAMES = 3
)

//  The Cmsg type holds a single key-value message consisting of a
//  list of 0 or more frames.
type Cmsg struct {
	//  Presence indicators for each frame
	present []bool
	//  Corresponding 0MQ message frames, if any
	frame []string
}

//  Constructor, takes a sequence number for the new Cmsg instance.
func NewCmsg(sequence int64) (cmsg *Cmsg) {
	cmsg = &Cmsg{
		present: make([]bool, cmsg_FRAMES),
		frame:   make([]string, cmsg_FRAMES),
	}
	cmsg.SetSequence(sequence)
	return
}

//  The RecvCmsg function reads a key-value message from socket, and returns a new
//  Cmsg instance.
func RecvCmsg(socket *zmq.Socket) (cmsg *Cmsg, err error) {
	cmsg = &Cmsg{
		present: make([]bool, cmsg_FRAMES),
		frame:   make([]string, cmsg_FRAMES),
	}
	msg, err := socket.RecvMessage(0)
	if err != nil {
		return
	}
	//fmt.Printf("Recv from %s: %q\n", socket, msg)
	for i := 0; i < cmsg_FRAMES && i < len(msg); i++ {
		cmsg.frame[i] = msg[i]
		cmsg.present[i] = true
	}
	return
}

//  The send method sends a multi-frame key-value message to a socket.
func (cmsg *Cmsg) Send(socket *zmq.Socket) (err error) {
	//fmt.Printf("Send to %s: %q\n", socket, cmsg.frame)
	_, err = socket.SendMessage(cmsg.frame)
	return
}

func (cmsg *Cmsg) GetKey() (key string, err error) {
	if !cmsg.present[frame_KEY] {
		err = errors.New("Key not set")
		return
	}
	key = cmsg.frame[frame_KEY]
	return
}

func (cmsg *Cmsg) SetKey(key string) {
	cmsg.frame[frame_KEY] = key
	cmsg.present[frame_KEY] = true
}

func (cmsg *Cmsg) GetSequence() (sequence int64, err error) {
	if !cmsg.present[frame_SEQ] {
		err = errors.New("Sequence not set")
		return
	}
	source := cmsg.frame[frame_SEQ]
	sequence = int64(source[0])<<56 +
		int64(source[1])<<48 +
		int64(source[2])<<40 +
		int64(source[3])<<32 +
		int64(source[4])<<24 +
		int64(source[5])<<16 +
		int64(source[6])<<8 +
		int64(source[7])
	return
}

func (cmsg *Cmsg) SetSequence(sequence int64) {

	source := make([]byte, 8)
	source[0] = byte((sequence >> 56) & 255)
	source[1] = byte((sequence >> 48) & 255)
	source[2] = byte((sequence >> 40) & 255)
	source[3] = byte((sequence >> 32) & 255)
	source[4] = byte((sequence >> 24) & 255)
	source[5] = byte((sequence >> 16) & 255)
	source[6] = byte((sequence >> 8) & 255)
	source[7] = byte((sequence) & 255)

	cmsg.frame[frame_SEQ] = string(source)
	cmsg.present[frame_SEQ] = true
}

func (cmsg *Cmsg) GetBody() (body string, err error) {
	if !cmsg.present[frame_BODY] {
		err = errors.New("Body not set")
		return
	}
	body = cmsg.frame[frame_BODY]
	return
}

func (cmsg *Cmsg) SetBody(body string) {
	cmsg.frame[frame_BODY] = body
	cmsg.present[frame_BODY] = true
}

//  The size method returns the body size of the last-read message, if any.
func (cmsg *Cmsg) Size() int {
	if cmsg.present[frame_BODY] {
		return len(cmsg.frame[frame_BODY])
	}
	return 0
}

//  The store method stores the key-value message into a hash map, unless
//  the key is nil.
func (cmsg *Cmsg) Store(cmap map[string]*Cmsg) {
	if cmsg.present[frame_KEY] {
		cmap[cmsg.frame[frame_KEY]] = cmsg
	}
}

//  The dump method prints the key-value message to stderr,
//  for debugging and tracing.
func (cmsg *Cmsg) Dump() {
	size := cmsg.Size()
	body, _ := cmsg.GetBody()
	seq, _ := cmsg.GetSequence()
	key, _ := cmsg.GetKey()
	fmt.Fprintf(os.Stderr, "[seq:%v][key:%v][size:%v]", seq, key, size)
	for char_nbr := 0; char_nbr < size; char_nbr++ {
		fmt.Fprintf(os.Stderr, "%02X", body[char_nbr])
	}
	fmt.Fprintln(os.Stderr)
}

