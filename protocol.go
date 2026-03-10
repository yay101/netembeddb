package netembeddb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"reflect"
	"time"
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
)

const (
	TypeMarkerInt    = 'i'
	TypeMarkerUint   = 'u'
	TypeMarkerFloat  = 'f'
	TypeMarkerString = 's'
	TypeMarkerBool   = 'b'
	TypeMarkerTime   = 't'
	TypeMarkerSlice  = 'l'
	TypeMarkerBytes  = 'B'
)

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
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], v)
	_, err := w.buf.Write(buf[:n])
	return err
}

func (w *Writer) WriteInt64(v int64) error {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutVarint(buf[:], v)
	_, err := w.buf.Write(buf[:n])
	return err
}

func (w *Writer) WriteString(s string) error {
	if err := w.WriteUint64(uint64(len(s))); err != nil {
		return err
	}
	_, err := w.buf.WriteString(s)
	return err
}

func (w *Writer) WriteBytes(data []byte) error {
	if err := w.WriteUint64(uint64(len(data))); err != nil {
		return err
	}
	_, err := w.buf.Write(data)
	return err
}

func (w *Writer) WriteFloat64(v float64) error {
	return w.WriteUint64(math.Float64bits(v))
}

func (w *Writer) WriteFloat32(v float32) error {
	return w.WriteUint64(uint64(math.Float32bits(v)))
}

func (w *Writer) WriteBool(v bool) error {
	if v {
		return w.WriteByte(1)
	}
	return w.WriteByte(0)
}

func (w *Writer) WriteTime(t time.Time) error {
	if err := w.WriteByte(TypeMarkerTime); err != nil {
		return err
	}
	return w.WriteInt64(t.UnixNano())
}

func (w *Writer) WriteSlice(slice interface{}) error {
	v := reflect.ValueOf(slice)
	if v.Kind() != reflect.Slice {
		return fmt.Errorf("not a slice: %T", slice)
	}

	if err := w.WriteByte(TypeMarkerSlice); err != nil {
		return err
	}

	length := v.Len()
	if err := w.WriteUint64(uint64(length)); err != nil {
		return err
	}

	elemType := v.Type().Elem()
	elemMarker := getTypeMarker(elemType)
	if err := w.WriteByte(elemMarker); err != nil {
		return err
	}

	for i := 0; i < length; i++ {
		elem := v.Index(i).Interface()
		if err := writeValueByType(w, elem, elemType); err != nil {
			return err
		}
	}
	return nil
}

func getTypeMarker(t reflect.Type) byte {
	switch t.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return TypeMarkerInt
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return TypeMarkerUint
	case reflect.Float32, reflect.Float64:
		return TypeMarkerFloat
	case reflect.String:
		return TypeMarkerString
	case reflect.Bool:
		return TypeMarkerBool
	default:
		return TypeMarkerBytes
	}
}

func writeValueByType(w *Writer, value interface{}, t reflect.Type) error {
	switch t.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return w.WriteInt64(reflect.ValueOf(value).Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return w.WriteUint64(reflect.ValueOf(value).Uint())
	case reflect.Float32:
		return w.WriteFloat32(float32(reflect.ValueOf(value).Float()))
	case reflect.Float64:
		return w.WriteFloat64(reflect.ValueOf(value).Float())
	case reflect.String:
		return w.WriteString(value.(string))
	case reflect.Bool:
		return w.WriteBool(value.(bool))
	default:
		return fmt.Errorf("unsupported slice element type: %v", t)
	}
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
	b := r.buf[r.pos]
	r.pos++
	return b, nil
}

func (r *Reader) ReadUint64() (uint64, error) {
	// Simple varint decode
	var val uint64
	var shift uint
	for {
		b, err := r.ReadByte()
		if err != nil {
			return 0, err
		}
		val |= uint64(b&0x7F) << shift
		if b&0x80 == 0 {
			break
		}
		shift += 7
	}
	return val, nil
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

	// Read n bytes
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
	return math.Float64frombits(bits), nil
}

func (r *Reader) ReadFloat32() (float32, error) {
	bits, err := r.ReadUint64()
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

func (r *Reader) ReadTime() (time.Time, error) {
	marker, err := r.ReadByte()
	if err != nil {
		return time.Time{}, err
	}
	if marker != TypeMarkerTime {
		return time.Time{}, fmt.Errorf("expected time marker, got %c", marker)
	}
	nano, err := r.ReadInt64()
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(0, nano), nil
}

func (r *Reader) ReadSlice(elemType reflect.Type) (interface{}, error) {
	marker, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	if marker != TypeMarkerSlice {
		return nil, fmt.Errorf("expected slice marker, got %c", marker)
	}

	length, err := r.ReadUint64()
	if err != nil {
		return nil, err
	}

	elemMarker, err := r.ReadByte()
	if err != nil {
		return nil, err
	}

	slice := reflect.MakeSlice(reflect.SliceOf(elemType), int(length), int(length))
	for i := 0; i < int(length); i++ {
		elem, err := r.readValueByMarker(elemMarker, elemType)
		if err != nil {
			return nil, err
		}
		slice.Index(i).Set(reflect.ValueOf(elem))
	}
	return slice.Interface(), nil
}

func (r *Reader) readValueByMarker(marker byte, t reflect.Type) (interface{}, error) {
	switch marker {
	case TypeMarkerInt:
		return r.ReadInt64()
	case TypeMarkerUint:
		return r.ReadUint64()
	case TypeMarkerFloat:
		if t.Kind() == reflect.Float32 {
			return r.ReadFloat32()
		}
		return r.ReadFloat64()
	case TypeMarkerString:
		return r.ReadString()
	case TypeMarkerBool:
		return r.ReadBool()
	default:
		return nil, fmt.Errorf("unsupported type marker: %c", marker)
	}
}

type Response struct {
	Success bool
	Data    []byte
	Error   string
}

func WriteResponse(w *Writer, resp Response) error {
	if resp.Success {
		if err := w.WriteByte(1); err != nil {
			return err
		}
		return w.WriteBytes(resp.Data)
	}
	if err := w.WriteByte(0); err != nil {
		return err
	}
	return w.WriteString(resp.Error)
}

func ReadResponse(r *Reader) (Response, error) {
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

func WriteError(w *Writer, err error) error {
	return WriteResponse(w, Response{Success: false, Error: err.Error()})
}

type Request struct {
	Op     byte
	Table  string
	Field  string
	Key    []byte
	Value  []byte
	Schema []byte
}

func WriteRequest(w *Writer, req Request) error {
	if err := w.WriteByte(req.Op); err != nil {
		return err
	}
	if err := w.WriteString(req.Table); err != nil {
		return err
	}
	if err := w.WriteString(req.Field); err != nil {
		return err
	}
	if err := w.WriteBytes(req.Key); err != nil {
		return err
	}
	if err := w.WriteBytes(req.Value); err != nil {
		return err
	}
	return w.WriteBytes(req.Schema)
}

func ReadRequest(r *Reader) (Request, error) {
	op, err := r.ReadByte()
	if err != nil {
		return Request{}, err
	}

	table, err := r.ReadString()
	if err != nil {
		return Request{}, err
	}

	field, err := r.ReadString()
	if err != nil {
		return Request{}, err
	}

	key, err := r.ReadBytes()
	if err != nil {
		return Request{}, err
	}

	value, err := r.ReadBytes()
	if err != nil {
		return Request{}, err
	}

	schema, err := r.ReadBytes()
	if err != nil {
		return Request{}, err
	}

	return Request{
		Op:     op,
		Table:  table,
		Field:  field,
		Key:    key,
		Value:  value,
		Schema: schema,
	}, nil
}

// EncodeValue encodes a value to bytes based on its type
func EncodeValue(value interface{}) ([]byte, error) {
	w := NewWriter(nil)
	if value == nil {
		return w.Bytes(), nil
	}

	switch v := value.(type) {
	case int:
		w.WriteByte(TypeMarkerInt)
		w.WriteInt64(int64(v))
	case int8:
		w.WriteByte(TypeMarkerInt)
		w.WriteInt64(int64(v))
	case int16:
		w.WriteByte(TypeMarkerInt)
		w.WriteInt64(int64(v))
	case int32:
		w.WriteByte(TypeMarkerInt)
		w.WriteInt64(int64(v))
	case int64:
		w.WriteByte(TypeMarkerInt)
		w.WriteInt64(v)
	case uint:
		w.WriteByte(TypeMarkerUint)
		w.WriteUint64(uint64(v))
	case uint8:
		w.WriteByte(TypeMarkerUint)
		w.WriteUint64(uint64(v))
	case uint16:
		w.WriteByte(TypeMarkerUint)
		w.WriteUint64(uint64(v))
	case uint32:
		w.WriteByte(TypeMarkerUint)
		w.WriteUint64(uint64(v))
	case uint64:
		w.WriteByte(TypeMarkerUint)
		w.WriteUint64(v)
	case float32:
		w.WriteByte(TypeMarkerFloat)
		w.WriteFloat32(v)
	case float64:
		w.WriteByte(TypeMarkerFloat)
		w.WriteFloat64(v)
	case string:
		w.WriteByte(TypeMarkerString)
		w.WriteString(v)
	case bool:
		w.WriteByte(TypeMarkerBool)
		w.WriteBool(v)
	case time.Time:
		w.WriteTime(v)
	default:
		rv := reflect.ValueOf(v)
		if rv.Kind() == reflect.Slice {
			w.WriteSlice(v)
		} else {
			return nil, fmt.Errorf("unsupported type: %T", value)
		}
	}

	return w.Bytes(), nil
}

// DecodeValue decodes a value from bytes based on expected type
func DecodeValue(data []byte, expectedType string) (interface{}, error) {
	if len(data) == 0 {
		return nil, nil
	}

	r := NewReader(bytes.NewReader(data))
	marker, err := r.ReadByte()
	if err != nil {
		return nil, err
	}

	switch marker {
	case TypeMarkerInt:
		val, err := r.ReadInt64()
		return val, err
	case TypeMarkerUint:
		val, err := r.ReadUint64()
		return val, err
	case TypeMarkerFloat:
		val, err := r.ReadFloat64()
		return val, err
	case TypeMarkerString:
		val, err := r.ReadString()
		return val, err
	case TypeMarkerBool:
		val, err := r.ReadBool()
		return val, err
	case TypeMarkerTime:
		// Marker already read, just read the int64
		nano, err := r.ReadInt64()
		if err != nil {
			return time.Time{}, err
		}
		return time.Unix(0, nano), nil
	case TypeMarkerSlice:
		return decodeSliceByType(r, expectedType)
	default:
		return nil, fmt.Errorf("unknown type marker: %c", marker)
	}
}

func decodeSliceByType(r *Reader, expectedType string) (interface{}, error) {
	length, err := r.ReadUint64()
	if err != nil {
		return nil, err
	}

	_, err = r.ReadByte() // element type marker (not currently used)
	if err != nil {
		return nil, err
	}

	switch expectedType {
	case "[]string":
		result := make([]string, length)
		for i := uint64(0); i < length; i++ {
			val, err := r.ReadString()
			if err != nil {
				return nil, err
			}
			result[i] = val
		}
		return result, nil
	case "[]int":
		result := make([]int, length)
		for i := uint64(0); i < length; i++ {
			val, err := r.ReadInt64()
			if err != nil {
				return nil, err
			}
			result[i] = int(val)
		}
		return result, nil
	case "[]int64":
		result := make([]int64, length)
		for i := uint64(0); i < length; i++ {
			val, err := r.ReadInt64()
			if err != nil {
				return nil, err
			}
			result[i] = val
		}
		return result, nil
	default:
		return nil, fmt.Errorf("unsupported slice type: %s", expectedType)
	}
}
