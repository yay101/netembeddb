package netembeddb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"reflect"
	"time"

	"github.com/yay101/embeddb"
)

type Client struct {
	conn net.Conn
}

func Connect(addr string, authKey string) (*Client, error) {
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		if addr[0] == '/' {
			conn, err = net.DialTimeout("unix", addr, 5*time.Second)
			if err != nil {
				return nil, fmt.Errorf("failed to connect: %w", err)
			}
		} else {
			return nil, fmt.Errorf("failed to connect: %w", err)
		}
	}

	if authKey != "" {
		conn.SetWriteDeadline(time.Now().Add(5 * time.Second))

		var buf [10]byte
		n := binary.PutUvarint(buf[:], uint64(len(authKey)))
		conn.Write(buf[:n])
		conn.Write([]byte(authKey))

		conn.SetWriteDeadline(time.Time{})
	}

	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	resp := make([]byte, 1)
	_, err = conn.Read(resp)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to read auth response: %w", err)
	}
	conn.SetReadDeadline(time.Time{})

	if resp[0] == 0 {
		lenBuf := make([]byte, 4)
		conn.Read(lenBuf)
		errLen := binary.BigEndian.Uint32(lenBuf)
		errMsg := make([]byte, errLen)
		conn.Read(errMsg)
		conn.Close()
		return nil, fmt.Errorf("auth failed: %s", string(errMsg))
	}

	return &Client{conn: conn}, nil
}

func (c *Client) Close() error {
	c.conn.Write([]byte{OpClose})
	return c.conn.Close()
}

func (c *Client) RegisterTable(name string, schema interface{}) error {
	w := NewWriter(nil)
	w.WriteByte(OpRegister)
	w.WriteString(name)

	c.conn.Write(w.Bytes())

	r := NewReader(c.conn)
	resp, err := ReadResp(r)
	if err != nil {
		return err
	}
	if !resp.Success {
		return fmt.Errorf("%s", resp.Error)
	}
	return nil
}

func (c *Client) CreateTable(name string, schema interface{}) error {
	w := NewWriter(nil)
	w.WriteByte(OpCreateTable)
	w.WriteString(name)

	c.conn.Write(w.Bytes())

	r := NewReader(c.conn)
	resp, err := ReadResp(r)
	if err != nil {
		return err
	}
	if !resp.Success {
		return fmt.Errorf("%s", resp.Error)
	}
	return nil
}

func (c *Client) ListTables() ([]string, error) {
	w := NewWriter(nil)
	w.WriteByte(OpListTables)

	c.conn.Write(w.Bytes())

	r := NewReader(c.conn)
	resp, err := ReadResp(r)
	if err != nil {
		return nil, err
	}
	if !resp.Success {
		return nil, fmt.Errorf("%s", resp.Error)
	}

	reader := NewReader(bytes.NewReader(resp.Data))
	count, _ := reader.ReadUint64()
	tables := make([]string, count)
	for i := uint64(0); i < count; i++ {
		tables[i], _ = reader.ReadString()
	}
	return tables, nil
}

func (c *Client) Insert(table string, record interface{}) (uint32, error) {
	var recordData []byte
	switch r := record.(type) {
	case []byte:
		recordData = r
	case string:
		recordData = []byte(r)
	default:
		var err error
		recordData, err = encodeRecord(record)
		if err != nil {
			return 0, err
		}
	}

	w := NewWriter(nil)
	w.WriteByte(OpInsert)
	w.WriteString(table)
	w.WriteBytes(recordData)

	c.conn.Write(w.Bytes())

	r := NewReader(c.conn)
	resp, err := ReadResp(r)
	if err != nil {
		return 0, err
	}
	if !resp.Success {
		return 0, fmt.Errorf("%s", resp.Error)
	}

	if len(resp.Data) >= 4 {
		return binary.BigEndian.Uint32(resp.Data), nil
	}
	return 0, nil
}

func (c *Client) Get(table string, id uint32) ([]byte, error) {
	w := NewWriter(nil)
	w.WriteByte(OpGet)
	w.WriteString(table)
	w.WriteUint64(uint64(id))

	c.conn.Write(w.Bytes())

	r := NewReader(c.conn)
	resp, err := ReadResp(r)
	if err != nil {
		return nil, err
	}
	if !resp.Success {
		return nil, fmt.Errorf("%s", resp.Error)
	}

	return resp.Data, nil
}

func (c *Client) Update(table string, id uint32, record interface{}) error {
	var recordData []byte
	switch r := record.(type) {
	case []byte:
		recordData = r
	case string:
		recordData = []byte(r)
	default:
		var err error
		recordData, err = encodeRecord(record)
		if err != nil {
			return err
		}
	}

	w := NewWriter(nil)
	w.WriteByte(OpUpdate)
	w.WriteString(table)
	w.WriteUint64(uint64(id))
	w.WriteBytes(recordData)

	c.conn.Write(w.Bytes())

	r := NewReader(c.conn)
	resp, err := ReadResp(r)
	if err != nil {
		return err
	}
	if !resp.Success {
		return fmt.Errorf("%s", resp.Error)
	}
	return nil
}

func (c *Client) Delete(table string, id uint32) error {
	w := NewWriter(nil)
	w.WriteByte(OpDelete)
	w.WriteString(table)
	w.WriteUint64(uint64(id))

	c.conn.Write(w.Bytes())

	r := NewReader(c.conn)
	resp, err := ReadResp(r)
	if err != nil {
		return err
	}
	if !resp.Success {
		return fmt.Errorf("%s", resp.Error)
	}
	return nil
}

func (c *Client) Query(table, field string, value interface{}) ([]uint32, error) {
	valueData, err := encodeValue(value)
	if err != nil {
		return nil, err
	}

	w := NewWriter(nil)
	w.WriteByte(OpQuery)
	w.WriteString(table)
	w.WriteString(field)
	w.WriteBytes(valueData)

	c.conn.Write(w.Bytes())

	r := NewReader(c.conn)
	resp, err := ReadResp(r)
	if err != nil {
		return nil, err
	}
	if !resp.Success {
		return nil, fmt.Errorf("%s", resp.Error)
	}

	reader := NewReader(bytes.NewReader(resp.Data))
	count, _ := reader.ReadUint64()
	ids := make([]uint32, count)
	for i := uint64(0); i < count; i++ {
		id, _ := reader.ReadUint64()
		ids[i] = uint32(id)
	}
	return ids, nil
}

func (c *Client) QueryGT(table, field string, value interface{}, inclusive bool) ([]uint32, error) {
	valueData, err := encodeValue(value)
	if err != nil {
		return nil, err
	}

	w := NewWriter(nil)
	w.WriteByte(OpQueryGT)
	w.WriteString(table)
	w.WriteString(field)
	w.WriteBytes(valueData)
	w.WriteBool(inclusive)

	c.conn.Write(w.Bytes())

	r := NewReader(c.conn)
	resp, err := ReadResp(r)
	if err != nil {
		return nil, err
	}
	if !resp.Success {
		return nil, fmt.Errorf("%s", resp.Error)
	}

	reader := NewReader(bytes.NewReader(resp.Data))
	count, _ := reader.ReadUint64()
	ids := make([]uint32, count)
	for i := uint64(0); i < count; i++ {
		id, _ := reader.ReadUint64()
		ids[i] = uint32(id)
	}
	return ids, nil
}

func (c *Client) QueryLT(table, field string, value interface{}, inclusive bool) ([]uint32, error) {
	valueData, err := encodeValue(value)
	if err != nil {
		return nil, err
	}

	w := NewWriter(nil)
	w.WriteByte(OpQueryLT)
	w.WriteString(table)
	w.WriteString(field)
	w.WriteBytes(valueData)
	w.WriteBool(inclusive)

	c.conn.Write(w.Bytes())

	r := NewReader(c.conn)
	resp, err := ReadResp(r)
	if err != nil {
		return nil, err
	}
	if !resp.Success {
		return nil, fmt.Errorf("%s", resp.Error)
	}

	reader := NewReader(bytes.NewReader(resp.Data))
	count, _ := reader.ReadUint64()
	ids := make([]uint32, count)
	for i := uint64(0); i < count; i++ {
		id, _ := reader.ReadUint64()
		ids[i] = uint32(id)
	}
	return ids, nil
}

func (c *Client) QueryBetween(table, field string, min, max interface{}, inclusiveMin, inclusiveMax bool) ([]uint32, error) {
	minData, err := encodeValue(min)
	if err != nil {
		return nil, err
	}
	maxData, err := encodeValue(max)
	if err != nil {
		return nil, err
	}

	w := NewWriter(nil)
	w.WriteByte(OpQueryBetween)
	w.WriteString(table)
	w.WriteString(field)
	w.WriteBytes(minData)
	w.WriteBytes(maxData)
	w.WriteBool(inclusiveMin)
	w.WriteBool(inclusiveMax)

	c.conn.Write(w.Bytes())

	r := NewReader(c.conn)
	resp, err := ReadResp(r)
	if err != nil {
		return nil, err
	}
	if !resp.Success {
		return nil, fmt.Errorf("%s", resp.Error)
	}

	reader := NewReader(bytes.NewReader(resp.Data))
	count, _ := reader.ReadUint64()
	ids := make([]uint32, count)
	for i := uint64(0); i < count; i++ {
		id, _ := reader.ReadUint64()
		ids[i] = uint32(id)
	}
	return ids, nil
}

func (c *Client) Count(table string) (uint32, error) {
	w := NewWriter(nil)
	w.WriteByte(OpCount)
	w.WriteString(table)

	data := w.Bytes()
	_, err := c.conn.Write(data)
	if err != nil {
		return 0, err
	}

	r := NewReader(c.conn)
	resp, err := ReadResp(r)
	if err != nil {
		return 0, err
	}
	if !resp.Success {
		return 0, fmt.Errorf("%s", resp.Error)
	}

	if len(resp.Data) >= 4 {
		return binary.BigEndian.Uint32(resp.Data), nil
	}
	return 0, nil
}

func (c *Client) Vacuum() error {
	w := NewWriter(nil)
	w.WriteByte(OpVacuum)

	c.conn.Write(w.Bytes())

	r := NewReader(c.conn)
	resp, err := ReadResp(r)
	if err != nil {
		return err
	}
	if !resp.Success {
		return fmt.Errorf("%s", resp.Error)
	}
	return nil
}

func encodeRecord(record interface{}) ([]byte, error) {
	var buffer []byte

	layout, err := embeddb.ComputeStructLayout(record)
	if err != nil {
		return nil, err
	}

	keys := make([]byte, 0, len(layout.FieldOffsets))
	for key := range layout.FieldOffsets {
		keys = append(keys, key)
	}

	for _, key := range keys {
		fieldOffset := layout.FieldOffsets[key]
		if fieldOffset.IsStruct && !fieldOffset.IsTime {
			continue
		}
		if fieldOffset.Primary {
			continue
		}

		buffer = append(buffer, key, embeddb.ValueStartMarker)

		val := reflect.ValueOf(record)
		if val.Kind() == reflect.Ptr {
			val = val.Elem()
		}

		fieldVal := val.FieldByName(fieldOffset.Name)
		if !fieldVal.IsValid() {
			continue
		}

		switch fieldVal.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			buffer = embeddb.EncodeVarint(buffer, fieldVal.Int())
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			buffer = embeddb.EncodeUvarint(buffer, fieldVal.Uint())
		case reflect.Float32, reflect.Float64:
			buffer = embeddb.EncodeFloat64(buffer, fieldVal.Float())
		case reflect.String:
			buffer = embeddb.EncodeString(buffer, fieldVal.String())
		case reflect.Bool:
			buffer = embeddb.EncodeBool(buffer, fieldVal.Bool())
		}

		buffer = append(buffer, embeddb.ValueEndMarker)
	}

	return buffer, nil
}

func encodeValue(value interface{}) ([]byte, error) {
	var buffer []byte

	switch v := value.(type) {
	case int:
		buffer = embeddb.EncodeVarint(buffer, int64(v))
	case int8:
		buffer = embeddb.EncodeVarint(buffer, int64(v))
	case int16:
		buffer = embeddb.EncodeVarint(buffer, int64(v))
	case int32:
		buffer = embeddb.EncodeVarint(buffer, int64(v))
	case int64:
		buffer = embeddb.EncodeVarint(buffer, v)
	case uint:
		buffer = embeddb.EncodeUvarint(buffer, uint64(v))
	case uint8:
		buffer = embeddb.EncodeUvarint(buffer, uint64(v))
	case uint16:
		buffer = embeddb.EncodeUvarint(buffer, uint64(v))
	case uint32:
		buffer = embeddb.EncodeUvarint(buffer, uint64(v))
	case uint64:
		buffer = embeddb.EncodeUvarint(buffer, v)
	case float32:
		buffer = embeddb.EncodeFloat64(buffer, float64(v))
	case float64:
		buffer = embeddb.EncodeFloat64(buffer, v)
	case string:
		buffer = embeddb.EncodeString(buffer, v)
	case bool:
		buffer = embeddb.EncodeBool(buffer, v)
	default:
		return nil, fmt.Errorf("unsupported value type: %T", value)
	}

	return buffer, nil
}
