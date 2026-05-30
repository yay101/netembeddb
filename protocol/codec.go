package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"reflect"
	"strconv"
	"time"
)

type OpCode byte

const (
	OpInsert         OpCode = 0x01
	OpGet            OpCode = 0x02
	OpUpdate         OpCode = 0x03
	OpDelete         OpCode = 0x04
	OpCount          OpCode = 0x05
	OpRegisterSchema OpCode = 0x06
	OpVacuum         OpCode = 0x07
	OpSync           OpCode = 0x08
	OpQuery          OpCode = 0x0A
	OpQueryRange     OpCode = 0x0B
	OpQueryLike      OpCode = 0x0C
	OpUpsert         OpCode = 0x0D
	OpInsertMany     OpCode = 0x0E
	OpInsertManyBulk OpCode = 0x0F
	OpDeleteMany     OpCode = 0x10
	OpUpdateMany     OpCode = 0x11
	OpScan           OpCode = 0x12
	OpAll            OpCode = 0x13
	OpDrop           OpCode = 0x14
	OpCreateIndex    OpCode = 0x15
	OpDropIndex      OpCode = 0x16
	OpGetIndexed     OpCode = 0x17
	OpBackup         OpCode = 0x1A
	OpStats          OpCode = 0x1B
	OpBegin          OpCode = 0x1C
	OpCommit         OpCode = 0x1D
	OpRollback       OpCode = 0x1E
	OpClose          OpCode = 0xFF
)

const (
	RangeFlagGT   byte = 1 << 0
	RangeFlagIncl byte = 1 << 1
	RangeFlagBtwn byte = 1 << 2
)

type Response struct {
	Success bool
	Error   string
	Data    []byte
}

type Encoder struct {
	w io.Writer
}

func NewEncoder(w io.Writer) *Encoder {
	return &Encoder{w: w}
}

func (e *Encoder) EncodeResponse(resp *Response) error {
	hdr := make([]byte, 9)
	if resp.Success {
		hdr[0] = 1
		binary.BigEndian.PutUint64(hdr[1:9], uint64(len(resp.Data)))
	} else {
		hdr[0] = 0
		binary.BigEndian.PutUint64(hdr[1:9], uint64(len(resp.Error)))
	}

	if _, err := e.w.Write(hdr); err != nil {
		return err
	}

	if resp.Success {
		if len(resp.Data) > 0 {
			if _, err := e.w.Write(resp.Data); err != nil {
				return err
			}
		}
	} else {
		if len(resp.Error) > 0 {
			if _, err := e.w.Write([]byte(resp.Error)); err != nil {
				return err
			}
		}
	}

	return nil
}

type Decoder struct {
	r io.Reader
}

func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{r: r}
}

func (d *Decoder) DecodeResponse() (*Response, error) {
	hdr := make([]byte, 9)
	if _, err := io.ReadFull(d.r, hdr); err != nil {
		return nil, err
	}

	resp := &Response{
		Success: hdr[0] == 1,
	}

	dataLen := binary.BigEndian.Uint64(hdr[1:9])

	if !resp.Success {
		if dataLen > 0 {
			errBuf := make([]byte, dataLen)
			if _, err := io.ReadFull(d.r, errBuf); err != nil {
				return nil, err
			}
			resp.Error = string(errBuf)
		}
	} else {
		if dataLen > 0 {
			resp.Data = make([]byte, dataLen)
			if _, err := io.ReadFull(d.r, resp.Data); err != nil {
				return nil, err
			}
		}
	}

	return resp, nil
}

func EncodeString(s string) []byte {
	buf := make([]byte, 4+len(s))
	binary.BigEndian.PutUint32(buf, uint32(len(s)))
	copy(buf[4:], s)
	return buf
}

func DecodeString(data []byte) (string, int, error) {
	if len(data) < 4 {
		return "", 0, io.ErrUnexpectedEOF
	}
	n := binary.BigEndian.Uint32(data)
	if int(n) > len(data)-4 {
		return "", 0, io.ErrUnexpectedEOF
	}
	return string(data[4 : 4+n]), int(n) + 4, nil
}

func EncodeUint64(v uint64) []byte {
	var enc [10]byte
	n := binary.PutUvarint(enc[:], v)
	return enc[:n]
}

func DecodeUint64(data []byte) (uint64, int, error) {
	v, n := binary.Uvarint(data)
	if n <= 0 {
		return 0, 0, io.ErrUnexpectedEOF
	}
	return v, n, nil
}

func EncodeInt64(v int64) []byte {
	ux := uint64(v) << 1
	if v < 0 {
		ux = ^ux
	}
	return EncodeUint64(ux)
}

func DecodeInt64(data []byte) (int64, int, error) {
	ux, n, err := DecodeUint64(data)
	if err != nil {
		return 0, 0, err
	}
	x := int64(ux >> 1)
	if ux&1 != 0 {
		x = ^x
	}
	return x, n, nil
}

func EncodeFloat64(v float64) []byte {
	return EncodeUint64(math.Float64bits(v))
}

func DecodeFloat64(data []byte) (float64, int, error) {
	v, n, err := DecodeUint64(data)
	if err != nil {
		return 0, 0, err
	}
	return math.Float64frombits(v), n, nil
}

func EncodeBytes(v []byte) []byte {
	buf := make([]byte, 4+len(v))
	binary.BigEndian.PutUint32(buf, uint32(len(v)))
	copy(buf[4:], v)
	return buf
}

func DecodeBytes(data []byte) ([]byte, int, error) {
	if len(data) < 4 {
		return nil, 0, io.ErrUnexpectedEOF
	}
	n := binary.BigEndian.Uint32(data)
	if int(n) > len(data)-4 {
		return nil, 0, io.ErrUnexpectedEOF
	}
	out := make([]byte, n)
	copy(out, data[4:4+n])
	return out, int(n) + 4, nil
}

func EncodeBool(v bool) []byte {
	if v {
		return []byte{1}
	}
	return []byte{0}
}

func DecodeBool(data []byte) (bool, int, error) {
	if len(data) < 1 {
		return false, 0, io.ErrUnexpectedEOF
	}
	return data[0] != 0, 1, nil
}

func EncodeUint32(v uint32) []byte {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, v)
	return buf
}

func DecodeUint32(data []byte) (uint32, int, error) {
	if len(data) < 4 {
		return 0, 0, io.ErrUnexpectedEOF
	}
	return binary.BigEndian.Uint32(data), 4, nil
}

func EncodeUint32Varint(v uint32) []byte {
	return EncodeUint64(uint64(v))
}

func DecodeUint32Varint(data []byte) (uint32, int, error) {
	v, n, err := DecodeUint64(data)
	return uint32(v), n, err
}

func EncodeTime(v time.Time) []byte {
	return EncodeInt64(v.UnixNano())
}

func DecodeTime(data []byte) (time.Time, int, error) {
	nano, n, err := DecodeInt64(data)
	if err != nil {
		return time.Time{}, 0, err
	}
	return time.Unix(0, nano), n, nil
}

const (
	FieldTypeInt    byte = 0x01
	FieldTypeInt8   byte = 0x02
	FieldTypeInt16  byte = 0x03
	FieldTypeInt32  byte = 0x04
	FieldTypeInt64  byte = 0x05
	FieldTypeUint   byte = 0x06
	FieldTypeUint8  byte = 0x07
	FieldTypeUint16 byte = 0x08
	FieldTypeUint32 byte = 0x09
	FieldTypeUint64 byte = 0x0A
	FieldTypeFloat32  byte = 0x0B
	FieldTypeFloat64  byte = 0x0C
	FieldTypeBool     byte = 0x0D
	FieldTypeString   byte = 0x0E
	FieldTypeTime     byte = 0x0F
	FieldTypeBytes    byte = 0x10
)

func fieldTypeTag(kind reflect.Kind) byte {
	switch kind {
	case reflect.Int:
		return FieldTypeInt
	case reflect.Int8:
		return FieldTypeInt8
	case reflect.Int16:
		return FieldTypeInt16
	case reflect.Int32:
		return FieldTypeInt32
	case reflect.Int64:
		return FieldTypeInt64
	case reflect.Uint:
		return FieldTypeUint
	case reflect.Uint8:
		return FieldTypeUint8
	case reflect.Uint16:
		return FieldTypeUint16
	case reflect.Uint32:
		return FieldTypeUint32
	case reflect.Uint64:
		return FieldTypeUint64
	case reflect.Float32:
		return FieldTypeFloat32
	case reflect.Float64:
		return FieldTypeFloat64
	case reflect.Bool:
		return FieldTypeBool
	case reflect.String:
		return FieldTypeString
	default:
		return 0
	}
}

func EncodeFieldValue(v any, kind reflect.Kind) ([]byte, error) {
	switch kind {
	case reflect.Int:
		tag := []byte{FieldTypeInt}
		return append(tag, EncodeInt64(int64(v.(int)))...), nil
	case reflect.Int8:
		tag := []byte{FieldTypeInt8}
		return append(tag, EncodeInt64(int64(v.(int8)))...), nil
	case reflect.Int16:
		tag := []byte{FieldTypeInt16}
		return append(tag, EncodeInt64(int64(v.(int16)))...), nil
	case reflect.Int32:
		tag := []byte{FieldTypeInt32}
		return append(tag, EncodeInt64(int64(v.(int32)))...), nil
	case reflect.Int64:
		tag := []byte{FieldTypeInt64}
		return append(tag, EncodeInt64(v.(int64))...), nil
	case reflect.Uint:
		tag := []byte{FieldTypeUint}
		return append(tag, EncodeUint64(uint64(v.(uint)))...), nil
	case reflect.Uint8:
		tag := []byte{FieldTypeUint8}
		return append(tag, EncodeUint64(uint64(v.(uint8)))...), nil
	case reflect.Uint16:
		tag := []byte{FieldTypeUint16}
		return append(tag, EncodeUint64(uint64(v.(uint16)))...), nil
	case reflect.Uint32:
		tag := []byte{FieldTypeUint32}
		return append(tag, EncodeUint64(uint64(v.(uint32)))...), nil
	case reflect.Uint64:
		tag := []byte{FieldTypeUint64}
		return append(tag, EncodeUint64(v.(uint64))...), nil
	case reflect.Float32:
		tag := []byte{FieldTypeFloat32}
		return append(tag, EncodeFloat64(float64(v.(float32)))...), nil
	case reflect.Float64:
		tag := []byte{FieldTypeFloat64}
		return append(tag, EncodeFloat64(v.(float64))...), nil
	case reflect.Bool:
		tag := []byte{FieldTypeBool}
		return append(tag, EncodeBool(v.(bool))...), nil
	case reflect.String:
		tag := []byte{FieldTypeString}
		return append(tag, EncodeString(v.(string))...), nil
	case reflect.Struct:
		if t, ok := v.(time.Time); ok {
			tag := []byte{FieldTypeTime}
			return append(tag, EncodeTime(t)...), nil
		}
		return nil, fmt.Errorf("unsupported struct field type: %v", kind)
	default:
		return nil, fmt.Errorf("unsupported field kind: %v", kind)
	}
}

func DecodeFieldValue(data []byte) (any, error) {
	if len(data) < 1 {
		return nil, io.ErrUnexpectedEOF
	}
	tag := data[0]
	payload := data[1:]

	switch tag {
	case FieldTypeInt:
		v, _, err := DecodeInt64(payload)
		return int(v), err
	case FieldTypeInt8:
		v, _, err := DecodeInt64(payload)
		return int8(v), err
	case FieldTypeInt16:
		v, _, err := DecodeInt64(payload)
		return int16(v), err
	case FieldTypeInt32:
		v, _, err := DecodeInt64(payload)
		return int32(v), err
	case FieldTypeInt64:
		v, _, err := DecodeInt64(payload)
		return v, err
	case FieldTypeUint:
		v, _, err := DecodeUint64(payload)
		return uint(v), err
	case FieldTypeUint8:
		v, _, err := DecodeUint64(payload)
		return uint8(v), err
	case FieldTypeUint16:
		v, _, err := DecodeUint64(payload)
		return uint16(v), err
	case FieldTypeUint32:
		v, _, err := DecodeUint64(payload)
		return uint32(v), err
	case FieldTypeUint64:
		v, _, err := DecodeUint64(payload)
		return v, err
	case FieldTypeFloat32:
		v, _, err := DecodeFloat64(payload)
		return float32(v), err
	case FieldTypeFloat64:
		v, _, err := DecodeFloat64(payload)
		return v, err
	case FieldTypeBool:
		v, _, err := DecodeBool(payload)
		return v, err
	case FieldTypeString:
		v, _, err := DecodeString(payload)
		return v, err
	case FieldTypeTime:
		v, _, err := DecodeTime(payload)
		return v, err
	default:
		return nil, fmt.Errorf("unknown field type tag: 0x%x", tag)
	}
}

func DecodeFieldValueAsString(data []byte, kind reflect.Kind) string {
	if len(data) < 1 {
		return ""
	}
	payload := data[1:]

	switch kind {
	case reflect.Int:
		v, _, _ := DecodeInt64(payload)
		return strconv.FormatInt(v, 10)
	case reflect.Int8:
		v, _, _ := DecodeInt64(payload)
		return strconv.FormatInt(v, 10)
	case reflect.Int16:
		v, _, _ := DecodeInt64(payload)
		return strconv.FormatInt(v, 10)
	case reflect.Int32:
		v, _, _ := DecodeInt64(payload)
		return strconv.FormatInt(v, 10)
	case reflect.Int64:
		v, _, _ := DecodeInt64(payload)
		return strconv.FormatInt(v, 10)
	case reflect.Uint:
		v, _, _ := DecodeUint64(payload)
		return strconv.FormatUint(v, 10)
	case reflect.Uint8:
		v, _, _ := DecodeUint64(payload)
		return strconv.FormatUint(v, 10)
	case reflect.Uint16:
		v, _, _ := DecodeUint64(payload)
		return strconv.FormatUint(v, 10)
	case reflect.Uint32:
		v, _, _ := DecodeUint64(payload)
		return strconv.FormatUint(v, 10)
	case reflect.Uint64:
		v, _, _ := DecodeUint64(payload)
		return strconv.FormatUint(v, 10)
	case reflect.Float32:
		v, _, _ := DecodeFloat64(payload)
		return strconv.FormatFloat(v, 'f', -1, 64)
	case reflect.Float64:
		v, _, _ := DecodeFloat64(payload)
		return strconv.FormatFloat(v, 'f', -1, 64)
	case reflect.Bool:
		v, _, _ := DecodeBool(payload)
		return strconv.FormatBool(v)
	case reflect.String:
		v, _, _ := DecodeString(payload)
		return v
	case reflect.Struct:
		v, _, _ := DecodeTime(payload)
		return strconv.FormatInt(v.UnixNano(), 10)
	default:
		return ""
	}
}

func DecodeFieldValueByKind(data []byte, kind reflect.Kind) (any, error) {
	if len(data) < 1 {
		return nil, io.ErrUnexpectedEOF
	}
	payload := data[1:]

	switch kind {
	case reflect.Int:
		v, _, err := DecodeInt64(payload)
		return int(v), err
	case reflect.Int8:
		v, _, err := DecodeInt64(payload)
		return int8(v), err
	case reflect.Int16:
		v, _, err := DecodeInt64(payload)
		return int16(v), err
	case reflect.Int32:
		v, _, err := DecodeInt64(payload)
		return int32(v), err
	case reflect.Int64:
		v, _, err := DecodeInt64(payload)
		return v, err
	case reflect.Uint:
		v, _, err := DecodeUint64(payload)
		return uint(v), err
	case reflect.Uint8:
		v, _, err := DecodeUint64(payload)
		return uint8(v), err
	case reflect.Uint16:
		v, _, err := DecodeUint64(payload)
		return uint16(v), err
	case reflect.Uint32:
		v, _, err := DecodeUint64(payload)
		return uint32(v), err
	case reflect.Uint64:
		v, _, err := DecodeUint64(payload)
		return v, err
	case reflect.Float32:
		v, _, err := DecodeFloat64(payload)
		return float32(v), err
	case reflect.Float64:
		v, _, err := DecodeFloat64(payload)
		return v, err
	case reflect.Bool:
		v, _, err := DecodeBool(payload)
		return v, err
	case reflect.String:
		v, _, err := DecodeString(payload)
		return v, err
	case reflect.Struct:
		v, _, err := DecodeTime(payload)
		return v, err
	default:
		return nil, fmt.Errorf("unsupported field kind: %v", kind)
	}
}

type TableStatsInfo struct {
	Name        string
	RecordCount int
	TableID     uint8
}

type StatsInfo struct {
	Tables     []TableStatsInfo
	FileSize   int64
	WALSize    int64
	IndexKeys  int
	BTreeDepth int
}

func EncodeStats(stats *StatsInfo) []byte {
	buf := make([]byte, 0, 256)
	buf = append(buf, EncodeUint64(uint64(len(stats.Tables)))...)
	for _, t := range stats.Tables {
		buf = append(buf, EncodeString(t.Name)...)
		buf = append(buf, EncodeUint64(uint64(t.RecordCount))...)
		buf = append(buf, t.TableID)
	}
	buf = append(buf, EncodeInt64(stats.FileSize)...)
	buf = append(buf, EncodeInt64(stats.WALSize)...)
	buf = append(buf, EncodeUint64(uint64(stats.IndexKeys))...)
	buf = append(buf, EncodeUint64(uint64(stats.BTreeDepth))...)
	return buf
}

func DecodeStats(data []byte) (*StatsInfo, error) {
	pos := 0
	tableCount, n, err := DecodeUint64(data[pos:])
	if err != nil {
		return nil, err
	}
	pos += n

	tables := make([]TableStatsInfo, 0, tableCount)
	for i := uint64(0); i < tableCount; i++ {
		name, n, err := DecodeString(data[pos:])
		if err != nil {
			break
		}
		pos += n

		count, n, err := DecodeUint64(data[pos:])
		if err != nil {
			break
		}
		pos += n

		if pos >= len(data) {
			break
		}
		tid := data[pos]
		pos++

		tables = append(tables, TableStatsInfo{
			Name:        name,
			RecordCount: int(count),
			TableID:     tid,
		})
	}

	fileSize, n, err := DecodeInt64(data[pos:])
	if err != nil {
		return nil, err
	}
	pos += n

	walSize, n, err := DecodeInt64(data[pos:])
	if err != nil {
		return nil, err
	}
	pos += n

	indexKeys, n, err := DecodeUint64(data[pos:])
	if err != nil {
		return nil, err
	}
	pos += n

	btreeDepth, _, err := DecodeUint64(data[pos:])
	if err != nil {
		return nil, err
	}

	return &StatsInfo{
		Tables:     tables,
		FileSize:   fileSize,
		WALSize:    walSize,
		IndexKeys:  int(indexKeys),
		BTreeDepth: int(btreeDepth),
	}, nil
}
