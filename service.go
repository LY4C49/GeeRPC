package GeeRPC

import (
	"go/ast"
	"log"
	"reflect"
	"sync/atomic"
)

/*
对 net/rpc 而言，一个函数需要能够被远程调用，需要满足如下五个条件：

the method’s type is exported. – 方法所属类型是导出的。
the method is exported. – 方式是导出的。 (方法名首字母必须大写)
the method has two arguments, both exported (or builtin) types. – 两个入参，均为导出或内置类型。
the method’s second argument is a pointer. – 第二个入参必须是一个指针。
the method has return type error. – 返回值为 error 类型。

更直观一些：
func (t *T) MethodName(argType T1, replyType *T2) error //可以被远程调用的函数必须满足这几个条件

客户端发送的请求类似于：
{
    "ServiceMethod"： "T.MethodName"
    "Argv"："0101110101..." // 序列化之后的字节流
}
*/

//通过反射，我们能够非常容易地获取某个结构体的所有方法，并且能够通过方法，获取到该方法所有的参数类型与返回值。

/*
//reflect.Type 就是用来表示类型信息的，它包含了类型的各种属性和方法，可以用来查询类型的各种信息
*/

type methodType struct {
	method reflect.Method //方法本身
	//reflect.Type 是一个 interface 具有很多方法例如：Elem, Kind等
	// .Elem和.Kind: 对于一个切片来说 .Elem 返回切片元素的种类，.Kind返回切片本身的种类
	//.Elem() 方法用于获取指针、切片、映射、通道等类型的元素类型。
	ArgType   reflect.Type //第一个参数类型（开头注释中的T1）
	ReplyType reflect.Type //第二个参数类型（开头注释中的T2）
	numCalls  uint64       //后续统计调用次数用到
}

func (m *methodType) NumCalls() uint64 {
	return atomic.LoadUint64(&m.numCalls)
}

func (m *methodType) newArgv() reflect.Value {
	var argv reflect.Value
	// arg may be a pointer type, or a value type
	if m.ArgType.Kind() == reflect.Ptr {
		argv = reflect.New(m.ArgType.Elem())
	} else {
		argv = reflect.New(m.ArgType).Elem()
	}
	return argv
}

func (m *methodType) newReplyv() reflect.Value {
	// reply must be a pointer type
	replyv := reflect.New(m.ReplyType.Elem())
	switch m.ReplyType.Elem().Kind() {
	case reflect.Map:
		replyv.Elem().Set(reflect.MakeMap(m.ReplyType.Elem()))
	case reflect.Slice:
		replyv.Elem().Set(reflect.MakeSlice(m.ReplyType.Elem(), 0, 0))
	}
	return replyv
}

type service struct {
	name   string
	typ    reflect.Type
	rcvr   reflect.Value
	method map[string]*methodType // 将来调用类似于 ServiceA.add   ServiceA is the name of service, add will map to a method through this map
}

func (s *service) registerMethods() {
	s.method = make(map[string]*methodType)
	for i := 0; i < s.typ.NumMethod(); i++ {
		method := s.typ.Method(i)
		mType := method.Type
		if mType.NumIn() != 3 || mType.NumOut() != 1 {
			continue
		}
		if mType.Out(0) != reflect.TypeOf((*error)(nil)).Elem() {
			continue
		}
		argType, replyType := mType.In(1), mType.In(2)
		if !isExportedOrBuiltinType(argType) || !isExportedOrBuiltinType(replyType) {
			continue
		}
		s.method[method.Name] = &methodType{
			method:    method,
			ArgType:   argType,
			ReplyType: replyType,
		}
	}
}

func isExportedOrBuiltinType(t reflect.Type) bool {
	return ast.IsExported(t.Name()) || t.PkgPath() == ""
}

func newService(rcvr interface{}) *service {
	s := new(service)
	s.rcvr = reflect.ValueOf(rcvr)
	s.name = reflect.Indirect(s.rcvr).Type().Name()
	s.typ = reflect.TypeOf(rcvr)
	if !ast.IsExported(s.name) {
		log.Fatalf("rpc server: %s is not a valid service name", s.name)
	}
	s.registerMethods()
	return s
}

func (s *service) call(m *methodType, argv reflect.Value, replyv reflect.Value) error {
	atomic.AddUint64(&m.numCalls, 1)
	f := m.method.Func
	//Call calls the function v with the input arguments in.
	//For example, if len(in) == 3, v.Call(in) represents the Go call v(in[0], in[1], in[2]).
	returnValues := f.Call([]reflect.Value{s.rcvr, argv, replyv})
	if errInter := returnValues[0].Interface(); errInter != nil {
		return errInter.(error)
	}
	return nil
}
