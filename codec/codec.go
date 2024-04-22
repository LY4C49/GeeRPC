package codec

import "io"

// 请求和响应中的参数和返回值抽象为 body，剩余的信息放在 header 中，那么就可以抽象出数据结构 Header
type Header struct {
	ServiceMethod string //请求的方法名 format: Service.Method
	Seq           uint64 //sequence number chosen by client
	Error         string //客户端设置为空，服务端如果发生错误放在Error中
}

// 对消息体进行编解码的接口 Codec
type Codec interface {
	io.Closer
	ReadHeader(*Header) error
	ReadBody(interface{}) error
	Write(*Header, interface{}) error
}

/*
抽象出 Codec 的构造函数，客户端和服务端可以通过 Codec 的 Type 得到构造函数，从而创建 Codec 实例。
*/
type NewCodecFunc func(closer io.ReadWriteCloser) Codec
type Type string

const (
	GobType  Type = "application/gob"
	JsonType Type = "application/json" //Not implemented
)

var NewCodeFuncMap map[Type]NewCodecFunc

// 这里的init会默认使用 NewGobCodec， 其返回一个 GobCodec对象
// 实际上，如果实现了json等其他编码方法，还需要判断
func init() {
	NewCodeFuncMap = make(map[Type]NewCodecFunc)
	NewCodeFuncMap[GobType] = NewGobCodec
}
