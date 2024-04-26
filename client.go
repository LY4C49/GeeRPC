package GeeRPC

import (
	"GeeRPC/codec"
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
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
type clientResult struct {
	client *Client
	err    error
}

type newClientFunc func(conn net.Conn, opt *Option) (client *Client, err error)

// 以下代码是防止客户端创建连接超时，
// 除此之外还有Client.Call超时，服务端超时等
func dialTimeout(f newClientFunc, network, address string, opts ...*Option) (client *Client, err error) {
	opt, err := parseOptions(opts...)
	if err != nil {
		return nil, err
	}
	conn, err := net.DialTimeout(network, address, opt.ConnectTimeout)
	if err != nil {
		return nil, err
	}
	// close the connection if client is nil
	defer func() {
		if err != nil {
			_ = conn.Close()
		}
	}()
	ch := make(chan clientResult)
	go func() {
		client, err := f(conn, opt)
		ch <- clientResult{client: client, err: err}
	}()
	if opt.ConnectTimeout == 0 {
		result := <-ch
		return result.client, result.err
	}

	/*
		使用子协程执行 NewClient，执行完成后则通过信道 ch 发送结果，如果 time.After() 信道先接收到消息，则说明 NewClient 执行超时，返回错误。
	*/
	select {
	case <-time.After(opt.ConnectTimeout):
		return nil, fmt.Errorf("rpc client: connect timeout: expect within %s", opt.ConnectTimeout)
	case result := <-ch:
		return result.client, result.err
	}
}

func Dial(network string, address string, opts ...*Option) (client *Client, err error) {
	return dialTimeout(NewClient, network, address, opts...)

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

// 防止client.Call超时
// 使用 context 包实现，控制权交给用户，控制更为灵活。
/*
用户可以使用 context.WithTimeout 创建具备超时检测能力的 context 对象来控制。例如：
	ctx, _ := context.WithTimeout(context.Background(), time.Second)
	var reply int
	err := client.Call(ctx, "Foo.Sum", &Args{1, 2}, &reply)
*/
func (client *Client) Call(ctx context.Context, method string, args interface{}, replyv interface{}) error {
	call := client.Go(method, args, replyv, make(chan *Call, 1))
	// 当call完成时，会调用 call.done() 将自身这个call写入这个管道！
	select {
	case <-ctx.Done():
		client.removeCall(call.Seq)
		return errors.New("rpc client: call failed" + ctx.Err().Error())
	case call := <-call.Done:
		return call.Error
	}
}

// === 支持 HTTP -- client side ===

func NewHTTPClient(conn net.Conn, opt *Option) (*Client, error) {
	_, _ = io.WriteString(conn, fmt.Sprintf("CONNECT %s HTTP/1.0\n\n", defaultRPCPath))

	// Require successful HTTP response
	// before switching to RPC protocol.
	resp, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: "CONNECT"})
	if err == nil && resp.Status == connected {
		return NewClient(conn, opt)
	}
	if err == nil {
		err = errors.New("unexpected HTTP response: " + resp.Status)
	}
	return nil, err

}

func DialHTTP(network string, address string, opts ...*Option) (*Client, error) {
	return dialTimeout(NewHTTPClient, network, address, opts...)
}

// XDial calls different functions to connect to a RPC server
// according the first parameter rpcAddr.
// rpcAddr is a general format (protocol@addr) to represent a rpc server
// eg, http@10.0.0.1:7001, tcp@10.0.0.1:9999, unix@/tmp/geerpc.sock
func XDial(rpcAddr string, opts ...*Option) (*Client, error) {
	parts := strings.Split(rpcAddr, "@")
	if len(parts) != 2 {
		return nil, fmt.Errorf("rpc client err: wrong format '%s', expect protocol@addr", rpcAddr)
	}
	protocol, addr := parts[0], parts[1]
	switch protocol {
	case "http":
		return DialHTTP("tcp", addr, opts...)
	default:
		// tcp, unix or other transport protocol
		return Dial(protocol, addr, opts...)
	}
}
