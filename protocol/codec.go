package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
)

type Writer struct {
	buf []byte
}

func NewWriter() *Writer {
	return &Writer{
		buf: make([]byte, 0, 256),
	}
}

func (w *Writer) Reset() {
	w.buf = w.buf[:0]
}

func (w *Writer) Bytes() []byte {
	return w.buf
}

func (w *Writer) WriteTo(wr io.Writer) (int64, error) {
	n, err := wr.Write(encodeUvarint(uint64(len(w.buf))))
	if err != nil {
		return 0, err
	}
	m, err := wr.Write(w.buf)
	return int64(n + m), err
}

func (w *Writer) WriteRaw(data []byte) (int64, error) {
	w.buf = append(w.buf, data...)
	return int64(len(data)), nil
}

func (w *Writer) WriteByte(b byte) error {
	w.buf = append(w.buf, b)
	return nil
}

func (w *Writer) WriteUvarint(v uint64) error {
	for v >= 0x80 {
		w.buf = append(w.buf, byte(v)|0x80)
		v >>= 7
	}
	w.buf = append(w.buf, byte(v))
	return nil
}

func (w *Writer) WriteVarint(v int64) error {
	uv := uint64((v << 1) ^ (v >> 63))
	return w.WriteUvarint(uv)
}

func (w *Writer) WriteString(s string) error {
	if err := w.WriteUvarint(uint64(len(s))); err != nil {
		return err
	}
	w.buf = append(w.buf, s...)
	return nil
}

func (w *Writer) WriteBytes(data []byte) error {
	if err := w.WriteUvarint(uint64(len(data))); err != nil {
		return err
	}
	w.buf = append(w.buf, data...)
	return nil
}

func (w *Writer) WriteFloat64(v float64) error {
	return w.WriteUvarint(math.Float64bits(v))
}

func (w *Writer) WriteFloat32(v float32) error {
	return w.WriteUvarint(uint64(math.Float32bits(v)))
}

func (w *Writer) WriteBool(v bool) error {
	if v {
		w.buf = append(w.buf, 1)
	} else {
		w.buf = append(w.buf, 0)
	}
	return nil
}

func (w *Writer) WriteUint32(v uint32) (int64, error) {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], v)
	return w.WriteRaw(buf[:])
}

func (w *Writer) WriteUint64(v uint64) (int64, error) {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], v)
	return w.WriteRaw(buf[:])
}

func encodeUvarint(v uint64) []byte {
	var buf []byte
	for v >= 0x80 {
		buf = append(buf, byte(v)|0x80)
		v >>= 7
	}
	buf = append(buf, byte(v))
	return buf
}

type Reader struct {
	r   io.Reader
	buf []byte
	pos int
}

func NewReader(r io.Reader) *Reader {
	return &Reader{
		r:   r,
		buf: make([]byte, 0, 4096),
	}
}

func (r *Reader) fill() error {
	if r.pos >= len(r.buf) {
		r.buf = make([]byte, 0, 4096)
		r.pos = 0
		n, err := r.r.Read(r.buf[:cap(r.buf)])
		if err != nil {
			return err
		}
		r.buf = r.buf[:n]
	}
	return nil
}

func (r *Reader) ReadByte() (byte, error) {
	if err := r.fill(); err != nil {
		return 0, err
	}
	b := r.buf[r.pos]
	r.pos++
	return b, nil
}

func (r *Reader) ReadUvarint() (uint64, error) {
	var v uint64
	var shift uint
	for {
		b, err := r.ReadByte()
		if err != nil {
			return 0, err
		}
		if b < 0x80 {
			return v | uint64(b)<<shift, nil
		}
		v |= uint64(b&0x7F) << shift
		shift += 7
		if shift > 63 {
			return 0, fmt.Errorf("varint overflow")
		}
	}
}

func (r *Reader) ReadVarint() (int64, error) {
	uv, err := r.ReadUvarint()
	if err != nil {
		return 0, err
	}
	return int64(uv>>1) ^ -(int64(uv & 1)), nil
}

func (r *Reader) ReadString() (string, error) {
	n, err := r.ReadUvarint()
	if err != nil {
		return "", err
	}
	if n == 0 {
		return "", nil
	}

	for r.pos+int(n) > len(r.buf) {
		r.buf = append(r.buf, make([]byte, 256)...)
		n2, err := r.r.Read(r.buf[len(r.buf)-256:])
		if err != nil {
			return "", err
		}
		r.buf = r.buf[:len(r.buf)-256+n2]
	}

	s := string(r.buf[r.pos : r.pos+int(n)])
	r.pos += int(n)
	return s, nil
}

func (r *Reader) ReadBytes() ([]byte, error) {
	n, err := r.ReadUvarint()
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return []byte{}, nil
	}

	for r.pos+int(n) > len(r.buf) {
		r.buf = append(r.buf, make([]byte, 256)...)
		n2, err := r.r.Read(r.buf[len(r.buf)-256:])
		if err != nil {
			return nil, err
		}
		r.buf = r.buf[:len(r.buf)-256+n2]
	}

	b := make([]byte, n)
	copy(b, r.buf[r.pos:r.pos+int(n)])
	r.pos += int(n)
	return b, nil
}

func (r *Reader) ReadFloat64() (float64, error) {
	bits, err := r.ReadUvarint()
	if err != nil {
		return 0, err
	}
	return math.Float64frombits(bits), nil
}

func (r *Reader) ReadFloat32() (float32, error) {
	bits, err := r.ReadUvarint()
	if err != nil {
		return 0, err
	}
	return math.Float32frombits(uint32(bits)), nil
}

func (r *Reader) ReadBool() (bool, error) {
	b, err := r.ReadByte()
	if err != nil {
		return false, err
	}
	return b != 0, nil
}

func (r *Reader) ReadUint32() (uint32, error) {
	if err := r.fill(); err != nil {
		return 0, err
	}
	if len(r.buf)-r.pos < 4 {
		return 0, fmt.Errorf("need 4 bytes")
	}
	v := binary.BigEndian.Uint32(r.buf[r.pos:])
	r.pos += 4
	return v, nil
}

func (r *Reader) ReadUint64() (uint64, error) {
	if err := r.fill(); err != nil {
		return 0, err
	}
	if len(r.buf)-r.pos < 8 {
		return 0, fmt.Errorf("need 8 bytes")
	}
	v := binary.BigEndian.Uint64(r.buf[r.pos:])
	r.pos += 8
	return v, nil
}
