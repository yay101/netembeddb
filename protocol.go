package netembeddb

import (
	"bytes"
	"fmt"
	"io"

	"github.com/yay101/embeddb"
)

const (
	OpRegister     = 0x01
	OpInsert       = 0x02
	OpGet          = 0x03
	OpUpdate       = 0x04
	OpDelete       = 0x05
	OpQuery        = 0x06
	OpQueryGT      = 0x07
	OpQueryLT      = 0x08
	OpQueryBetween = 0x09
	OpFilter       = 0x0A
	OpScan         = 0x0B
	OpCount        = 0x0C
	OpListTables   = 0x0D
	OpCreateTable  = 0x0E
	OpClose        = 0x0F
	OpVacuum       = 0x10
)

type Response struct {
	Success bool
	Data    []byte
	Error   string
}

type Request struct {
	Op     byte
	Table  string
	Field  string
	Key    []byte
	Value  []byte
	Schema []byte
}

func WriteError(w *Writer, err error) error {
	return WriteResp(w, Response{Success: false, Error: err.Error()})
}

func WriteResp(w *Writer, resp Response) error {
	if resp.Success {
		w.WriteByte(1)
		w.WriteBytes(resp.Data)
	} else {
		w.WriteByte(0)
		w.WriteString(resp.Error)
	}
	return nil
}

func ReadResp(r *Reader) (Response, error) {
	success, err := r.ReadBool()
	if err != nil {
		return Response{}, err
	}

	if success {
		data, err := r.ReadBytes()
		return Response{Success: true, Data: data}, err
	}

	errMsg, err := r.ReadString()
	return Response{Success: false, Error: errMsg}, err
}

type Writer struct {
	buf bytes.Buffer
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{}
}

func (w *Writer) WriteTo(wr io.Writer) (int64, error) {
	return w.buf.WriteTo(wr)
}

func (w *Writer) Bytes() []byte {
	return w.buf.Bytes()
}

func (w *Writer) WriteByte(b byte) error {
	_, err := w.buf.Write([]byte{b})
	return err
}

func (w *Writer) WriteUint64(v uint64) error {
	return w.writeUvarint(v)
}

func (w *Writer) WriteInt64(v int64) error {
	return w.writeVarint(v)
}

func (w *Writer) WriteString(s string) error {
	w.buf.Write(embeddb.EncodeString(nil, s))
	return nil
}

func (w *Writer) WriteBytes(data []byte) error {
	w.buf.Write(embeddb.EncodeUvarint(nil, uint64(len(data))))
	w.buf.Write(data)
	return nil
}

func (w *Writer) WriteFloat64(v float64) error {
	w.buf.Write(embeddb.EncodeFloat64(nil, v))
	return nil
}

func (w *Writer) WriteFloat32(v float32) error {
	w.buf.Write(embeddb.EncodeFloat64(nil, float64(v)))
	return nil
}

func (w *Writer) WriteBool(v bool) error {
	w.buf.Write(embeddb.EncodeBool(nil, v))
	return nil
}

func (w *Writer) writeUvarint(v uint64) error {
	w.buf.Write(embeddb.EncodeUvarint(nil, v))
	return nil
}

func (w *Writer) writeVarint(v int64) error {
	w.buf.Write(embeddb.EncodeVarint(nil, v))
	return nil
}

type Reader struct {
	r   io.Reader
	buf []byte
	pos int
}

func NewReader(r io.Reader) *Reader {
	return &Reader{r: r}
}

func (r *Reader) ReadByte() (byte, error) {
	if r.pos >= len(r.buf) {
		r.buf = make([]byte, 4096)
		n, err := r.r.Read(r.buf)
		if err != nil {
			return 0, err
		}
		r.buf = r.buf[:n]
		r.pos = 0
	}
	if r.pos >= len(r.buf) {
		return 0, fmt.Errorf("EOF")
	}
	b := r.buf[r.pos]
	r.pos++
	return b, nil
}

func (r *Reader) ReadUint64() (uint64, error) {
	var total uint64
	var shift uint
	for {
		b, err := r.ReadByte()
		if err != nil {
			return 0, err
		}
		total |= uint64(b&0x7F) << shift
		if b&0x80 == 0 {
			break
		}
		shift += 7
	}
	return total, nil
}

func (r *Reader) ReadInt64() (int64, error) {
	uv, err := r.ReadUint64()
	if err != nil {
		return 0, err
	}
	return int64(uv>>1) ^ -(int64(uv & 1)), nil
}

func (r *Reader) ReadString() (string, error) {
	n, err := r.ReadUint64()
	if err != nil {
		return "", err
	}
	if n == 0 {
		return "", nil
	}

	buf := make([]byte, n)
	for i := uint64(0); i < n; i++ {
		b, err := r.ReadByte()
		if err != nil {
			return "", err
		}
		buf[i] = b
	}
	return string(buf), nil
}

func (r *Reader) ReadBytes() ([]byte, error) {
	n, err := r.ReadUint64()
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return []byte{}, nil
	}

	buf := make([]byte, n)
	for i := uint64(0); i < n; i++ {
		b, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		buf[i] = b
	}
	return buf, nil
}

func (r *Reader) ReadFloat64() (float64, error) {
	bits, err := r.ReadUint64()
	if err != nil {
		return 0, err
	}
	return float64(bits), nil
}

func (r *Reader) ReadFloat32() (float32, error) {
	bits, err := r.ReadUint64()
	if err != nil {
		return 0, err
	}
	return float32(bits), nil
}

func (r *Reader) ReadBool() (bool, error) {
	b, err := r.ReadByte()
	if err != nil {
		return false, err
	}
	return b != 0, nil
}
