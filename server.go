package GeeRPC

import (
	"GeeRPC/codec"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"time"
)

// 通信协议
/*
GeeRPC 客户端固定采用 JSON 编码 Option，
后续的 header 和 body 的编码方式由 Option 中的 CodeType 指定，
服务端首先使用 JSON 解码 Option，然后通过 Option 的 CodeType 解码剩余的内容
*/

/*
| Option{MagicNumber: xxx, CodecType: xxx} | Header{ServiceMethod ...} | Body interface{} |
| <------      固定 JSON 编码      ------>  | <-------   编码方式由 CodeType 决定   ------->|
*/

/*
Option固定最开始，后续Header, Body 可以有多个
*/

const MagicNumber = 0x3bef5c

type Option struct {
	MagicNumber int        // MN marks this is a geerpc request
	CodecType   codec.Type // client may choose different Codec to encode body

	ConnectTimeout time.Duration
	HandleTimeout  time.Duration
}

var DefaultOption = &Option{
	MagicNumber:    MagicNumber,
	CodecType:      codec.GobType,
	ConnectTimeout: time.Second * 10,
}

// Server represents an RPC Server.
type Server struct {
	serviceMap sync.Map
}

func NewServer() *Server {
	return &Server{}
}

func (server *Server) Register(rcvr interface{}) error {
	s := newService(rcvr)
	if _, dup := server.serviceMap.LoadOrStore(s.name, s); dup {
		return errors.New("rpc: service already defined: " + s.name) // 若service已经登记过，不可重复登记
	}
	return nil
}

// Register publishes the receiver's methods in the DefaultServer.
func Register(rcvr interface{}) error { return DefaultServer.Register(rcvr) }

func (server *Server) Accept(lis net.Listener) {
	for {
		conn, err := lis.Accept()
		if err != nil {
			log.Panicln("rpc server: accept error:", err)
			return
		}

		/*
			net.conn is an interface implements Readbyte[]), Write(byte[]), Close methods
			io.ReadWriteCloser is an interface includes Reader (has Read(byte[])), Writer(has Write(byte[]) method), Closer interfaces
		*/

		go server.ServerConn(conn)
	}
}

// 先在 serviceMap 中找到对应的 service 实例，再从 service 实例的 method 中，找到对应的 methodType。
func (server *Server) findService(serviceMethod string) (svc *service, mtype *methodType, err error) {
	dot := strings.LastIndex(serviceMethod, ".")
	if dot < 0 {
		err = errors.New("rpc server: service/method request ill-formed: " + serviceMethod)
		return
	}
	serviceName, methodName := serviceMethod[:dot], serviceMethod[dot+1:]
	svci, ok := server.serviceMap.Load(serviceName)
	if !ok {
		err = errors.New("rpc server: can't find service " + serviceName)
		return
	}
	svc = svci.(*service)
	mtype = svc.method[methodName]
	if mtype == nil {
		err = errors.New("rpc server: can't find method " + methodName)
	}
	return
}

func (server *Server) ServerConn(conn io.ReadWriteCloser) {
	defer func() { _ = conn.Close() }()

	//排除错误：非法的magic number等
	var opt Option
	if err := json.NewDecoder(conn).Decode(&opt); err != nil {
		log.Panicln("rpc server: options error: ", err)
		return
	}

	if opt.MagicNumber != MagicNumber {
		log.Printf("rpc server: invalid magic number %x", opt.MagicNumber)
		return
	}
	f := codec.NewCodeFuncMap[opt.CodecType]
	if f == nil {
		log.Printf("rpc server: invalid codec type %s", opt.CodecType)
		return
	}
	// f(conn) 返回对应编码类型的Codec实例化对象，例如 gob 返回 GobCodec （它实现了 readBody, readHeader, Write, Close方法）
	// 将来实现json的JSONCodec即可处理json
	server.serverCodec(f(conn), &opt) // CodecFuncMap keeps the constructor and return an instantiation corresponding Codec object
}

// invalidRequest is a placeholder for response argv when error occurs
var invalidRequest = struct{}{}

// DefaultServer is the default instance of *Server.
var DefaultServer = NewServer()

//== 核心：处理请求 ===
/*
serveCodec 的过程非常简单。主要包含三个阶段

1. 读取请求 readRequest
2. 处理请求 handleRequest
3. 回复请求 sendResponse
之前提到过，在一次连接中，允许接收多个请求，即多个 request header 和 request body，
因此这里使用了 for 无限制地等待请求的到来，直到发生错误（例如 **连接被关闭**，接收到的报文有问题等），这里需要注意的点有三个：

handleRequest 使用了协程并发执行请求。
处理请求是并发的，但是回复请求的报文必须是逐个发送的，并发容易导致多个回复报文交织在一起，客户端无法解析。在这里使用锁(sending)保证。
尽力而为，只有在 header 解析失败时，才终止循环。
*/

func (server *Server) serverCodec(cc codec.Codec, opt *Option) {
	sending := new(sync.Mutex)
	wg := new(sync.WaitGroup)
	for {
		req, err := server.readRequest(cc)
		if err != nil {
			if req == nil {
				break // impossible to recover
			}
			req.h.Error = err.Error()
			server.sendResponse(cc, req.h, invalidRequest, sending)
			continue
		}
		wg.Add(1)
		go server.handleRequest(cc, req, sending, wg, opt.HandleTimeout)
	}
	wg.Wait()
	_ = cc.Close()
}

type request struct {
	h            *codec.Header
	argv, replyv reflect.Value

	mtype *methodType
	svc   *service
}

func (server *Server) readRequestHeader(cc codec.Codec) (*codec.Header, error) {
	var h codec.Header
	if err := cc.ReadHeader(&h); err != nil {
		if err != io.EOF && err != io.ErrUnexpectedEOF {
			log.Println("rpc server: read header error:", err)
		}
		return nil, err
	}
	return &h, nil
}

func (server *Server) readRequest(cc codec.Codec) (*request, error) {
	h, err := server.readRequestHeader(cc)
	if err != nil {
		return nil, err
	}
	req := &request{h: h}

	// TODO: now we don't know the type of request argv
	// day 1, just suppose it's string
	//req.argv = reflect.New(reflect.TypeOf(""))
	req.svc, req.mtype, err = server.findService(h.ServiceMethod)
	if err != nil {
		return req, err
	}
	req.argv = req.mtype.newArgv()
	req.replyv = req.mtype.newReplyv()

	// make sure that argvi is a pointer, ReadBody need a pointer as parameter
	argvi := req.argv.Interface()
	if req.argv.Type().Kind() != reflect.Ptr {
		argvi = req.argv.Addr().Interface()
	}

	// Interface returns v's current value as an interface{}.
	if err = cc.ReadBody(argvi); err != nil {
		log.Println("rpc server: read argv err:", err)
		return req, err
	}
	return req, nil
}

func (server *Server) sendResponse(cc codec.Codec, h *codec.Header, body interface{}, sending *sync.Mutex) {
	sending.Lock()
	defer sending.Unlock()
	if err := cc.Write(h, body); err != nil {
		log.Println("rpc server: write response error: ", err)
	}
}

func (server *Server) handleRequest(cc codec.Codec, req *request, sending *sync.Mutex, wg *sync.WaitGroup, timeout time.Duration) {
	defer wg.Done()
	called := make(chan struct{})
	sent := make(chan struct{})

	go func() {
		err := req.svc.call(req.mtype, req.argv, req.replyv)
		called <- struct{}{}
		if err != nil {
			req.h.Error = err.Error()
			server.sendResponse(cc, req.h, invalidRequest, sending)
			sent <- struct{}{}
			return
		}
		server.sendResponse(cc, req.h, req.replyv.Interface(), sending)
		sent <- struct{}{}
	}()

	if timeout == 0 {
		<-called
		<-sent
		return
	}

	//called 信道接收到消息，代表处理没有超时，继续执行 sendResponse。
	//time.After() 先于 called 接收到消息，说明处理已经超时，called 和 sent 都将被阻塞。在 case <-time.After(timeout) 处调用 sendResponse。
	select {
	case <-time.After(timeout):
		req.h.Error = fmt.Sprintf("rpc server: request handle timeout: expect within %s", timeout)
		server.sendResponse(cc, req.h, invalidRequest, sending)
	case <-called:
		<-sent // 阻塞至发送完成
	}
}

// Accept accepts connections on the listener and serves requests
// for each incoming connection.
func Accept(lis net.Listener) { DefaultServer.Accept(lis) }

// === Add Support to HTTP - server side ===

/*
通信过程应该是这样的：

1. 客户端向 RPC 服务器发送 CONNECT 请求
CONNECT 10.0.0.1:9999/_geerpc_ HTTP/1.0

2.RPC 服务器返回 HTTP 200 状态码表示连接建立。
HTTP/1.0 200 Connected to Gee RPC

3.客户端使用创建好的连接发送 RPC 报文，先发送 Option，再发送 N 个请求报文，服务端处理 RPC 请求并响应。
*/

const (
	connected        = "200 Connected to Gee RPC"
	defaultRPCPath   = "/_geerpc_"
	defaultDebugPath = "/debug/geerpc"
)

func (server *Server) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method != "CONNECT" {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusMethodNotAllowed)
		_, _ = io.WriteString(w, "405 must CONNECT\n")
		return
	}

	//Hijack lets the caller take over the connection. After a call to Hijack the HTTP server library will not do anything else with the connection.
	//It becomes the caller's responsibility to manage and close the connection.
	conn, _, err := w.(http.Hijacker).Hijack()
	if err != nil {
		log.Print("rpc hijacking ", req.RemoteAddr, ": ", err.Error())
		return
	}
	_, _ = io.WriteString(conn, "HTTP/1.0 "+connected+"\n\n") // 服务器回应
	server.ServerConn(conn)
}

func (server *Server) HandleHTTP() {
	http.Handle(defaultRPCPath, server)
	http.Handle(defaultDebugPath, debugHTTP{server})
	log.Println("rpc server debug path:", defaultDebugPath)
}

// HandleHTTP is a convenient approach for default server to register HTTP handlers
func HandleHTTP() {
	DefaultServer.HandleHTTP()
}
