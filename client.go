package GeeRPC

import (
	"GeeRPC/codec"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
)

type Call struct {
	Method string
	Seq    uint64
	Args   interface{}
	Reply  interface{}
	Error  error
	Done   chan *Call // Strobes when call is complete.
}

func (call *Call) done() {
	call.Done <- call
}

type Client struct {
	cc  codec.Codec
	opt *Option

	sending sync.Mutex   //类似服务端，保证请求的有序发送
	header  codec.Header // 只有发送请求时才需要，而发送时互斥的，因此可以复用， Header {Service.Method, Seq, Error}

	mu       sync.Mutex
	seq      uint64
	pending  map[uint64]*Call // 存储为完成的请求
	closing  bool             // normally Close
	shutdown bool             // Close due to error
}

var _ io.Closer = (*Client)(nil)

var ErrShutdown = errors.New("connection is shut down")

// ===close===
func (client *Client) Close() error {
	client.mu.Lock()
	defer client.mu.Unlock()

	if client.closing {
		return ErrShutdown
	}

	client.closing = true
	return client.cc.Close()
}

func (client *Client) IsAvaliable() bool {
	client.mu.Lock()
	defer client.mu.Unlock()
	return !client.shutdown && !client.closing
}

// =======

func (client *Client) registerCall(call *Call) (uint64, error) {
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.shutdown || client.closing {
		return 0, ErrShutdown
	}

	call.Seq = client.seq
	client.pending[call.Seq] = call
	client.seq++
	return call.Seq, nil
}

func (client *Client) removeCall(seq uint64) *Call {
	client.mu.Lock()
	defer client.mu.Unlock()
	call := client.pending[seq]
	delete(client.pending, seq)
	return call
}

// When client error happens, call this function to terminate all calls and shutdown the client
func (client *Client) terminateCalls(err error) {
	client.mu.Lock()
	defer client.mu.Unlock()
	client.sending.Lock()
	defer client.sending.Unlock()

	client.shutdown = true
	for _, call := range client.pending {
		call.Error = err
		call.done()
	}
}

//=== Receive Calls ===
/*
call 不存在，可能是请求没有发送完整，或者因为其他原因被取消，但是服务端仍旧处理了。
call 存在，但服务端处理出错，即 h.Error 不为空。
call 存在，服务端处理正常，那么需要从 body 中读取 Reply 的值。
*/
func (client *Client) receive() {
	var err error
	for err == nil {
		var h codec.Header
		// cc.ReadHeader 会一直阻塞，直到有消息过来或者发生错误
		if err = client.cc.ReadHeader(&h); err != nil {
			break
		}
		call := client.removeCall(h.Seq)
		switch {
		case call == nil:
			// it usually means that Write partially failed
			// and call was already removed.
			err = client.cc.ReadBody(nil)
		case h.Error != "":
			call.Error = fmt.Errorf(h.Error)
			err = client.cc.ReadBody(nil)
			call.done()
		default:
			err = client.cc.ReadBody(call.Reply)
			if err != nil {
				call.Error = errors.New("reading body err:" + err.Error())
			}
			call.done()
		}
	}
	// Client error happens, terminate all calls
	client.terminateCalls(err)
}

// === 初始化客户端，发送请求 ===
func newClientCodec(cc codec.Codec, opt *Option) *Client {
	client := &Client{
		cc:      cc,
		opt:     opt,
		pending: make(map[uint64]*Call),
		seq:     uint64(1),
	}
	go client.receive()
	return client
}

func NewClient(conn net.Conn, opt *Option) (*Client, error) {
	f := codec.NewCodeFuncMap[opt.CodecType]
	if f == nil {
		err := fmt.Errorf("invalid codec type %s", opt.CodecType)
		log.Println("rpc client: codec error:", err)
		return nil, err
	}

	//Send option
	if err := json.NewEncoder(conn).Encode(opt); err != nil {
		log.Println("rpc client: option error: ", err)
		_ = conn.Close()
		return nil, err
	}

	// Init a client
	return newClientCodec(f(conn), opt), nil
}

// 仅为简化用户调用
func parseOptions(opts ...*Option) (*Option, error) {
	if len(opts) == 0 || opts[0] == nil {
		return DefaultOption, nil
	}
	if len(opts) != 1 {
		return nil, errors.New("too many options, only 1 option is acceptable")
	}
	opt := opts[0]
	opt.MagicNumber = DefaultOption.MagicNumber
	if opt.CodecType == "" {
		opt.CodecType = DefaultOption.CodecType
	}
	return opt, nil
}

// 提供给用户的调用接口，初始化一个客户端
func Dial(network string, address string, opts ...*Option) (client *Client, err error) {
	// parse options
	opt, err := parseOptions(opts...)
	if err != nil {
		return nil, err
	}

	// set up connection
	conn, err := net.Dial(network, address)
	if err != nil {
		return nil, err
	}

	defer func() {
		if client == nil {
			_ = conn.Close()
		}
	}()

	return NewClient(conn, opt)

}

// === Done : 连接与接收

// === 发送请求 ===
func (client *Client) send(call *Call) {
	client.sending.Lock()
	defer client.sending.Unlock()

	// register call
	seq, err := client.registerCall(call)
	if err != nil {
		call.Error = err
		call.done()
		return
	}

	// prepare header
	client.header.Seq = seq
	client.header.ServiceMethod = call.Method
	client.header.Error = ""

	// encode and send
	if err := client.cc.Write(&client.header, call.Args); err != nil {
		call := client.removeCall(seq)
		// call may be nil, it usually means that Write partially failed,
		// client has received the response and handled
		if call != nil {
			call.Error = err
			call.done()
		}
	}
}

// Go invokes the function asynchronously.
// It returns the Call structure representing the invocation.
func (client *Client) Go(method string, args interface{}, replyv interface{}, done chan *Call) *Call {
	// process error
	if done == nil {
		done = make(chan *Call, 10)
	} else if cap(done) == 0 {
		log.Panic("rpc client: done channel is unbuffered")
	}

	call := &Call{
		Method: method,
		Args:   args,
		Reply:  replyv,
		Done:   done,
	}
	client.send(call)
	return call
}

func (client *Client) Call(method string, args interface{}, replyv interface{}) error {
	call := <-client.Go(method, args, replyv, make(chan *Call, 1)).Done // Done是call中的一个管道，当没有完成时，会阻塞在此等待，实现一个同步的Call方法
	// 当call完成时，会调用 call.done() 将自身这个call写入这个管道！
	return call.Error
}
