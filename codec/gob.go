package codec

import (
	"bufio"
	"encoding/gob"
	"io"
	"log"
)

type GobCodec struct {
	conn io.ReadWriteCloser
	buf  *bufio.Writer
	dec  *gob.Decoder
	enc  *gob.Encoder
}

/*
这是确保接口被实现的一种常用方法
利用强制转换，确保 GobCodec实现了Codec接口
如果未实现，在编译器就会被提示！(事实上IDEA直接画红线了)
*/
var _ Codec = (*GobCodec)(nil)

// 实现Codec接口
func (c *GobCodec) ReadHeader(h *Header) error {
	return c.dec.Decode(h)
}

func (c *GobCodec) ReadBody(body interface{}) error {
	return c.dec.Decode(body)
}

func (c *GobCodec) Write(h *Header, body interface{}) (err error) {
	defer func() {
		_ = c.buf.Flush()
		if err != nil {
			_ = c.Close()
		}
	}()
	if err := c.enc.Encode(h); err != nil {
		log.Println("rpc codec: gob error encoding header:", err)
		return err
	}
	if err := c.enc.Encode(body); err != nil {
		log.Println("rpc codec: gob error encoding body:", err)
		return err
	}
	return nil
}

func (c *GobCodec) Close() error {
	return c.conn.Close()
}

/*
func NewDecoder(r io.Reader) *Decoder: NewDecoder returns a new decoder that reads from the io.Reader
func NewEncoder(w io.Writer) *Encoder: NewEncoder returns a new encoder that will transmit on the io.Writer.

conn: ReadWriteCloser is the interface that groups the basic Read, Write and Close methods.

buf 使用conn.Write, 所以Enconder会将结果写入buf, buf关联到conn.Writer进行发送 在Write中调用了 c.buf.Flush
Decoder将读取的内容进行解码, dec.Decode将写入响应的对象中--> c.dec.Decode(h) & c.dec.Decode(body)
*/

func NewGobCodec(conn io.ReadWriteCloser) Codec {
	buf := bufio.NewWriter(conn)
	return &GobCodec{
		conn: conn,
		buf:  buf,
		dec:  gob.NewDecoder(conn),
		enc:  gob.NewEncoder(buf),
	}
}
