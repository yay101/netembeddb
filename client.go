package netembeddb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"reflect"
	"time"
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

	// Send auth key
	if authKey != "" {
		conn.SetWriteDeadline(time.Now().Add(5 * time.Second))

		// Write key length as varint
		var buf [10]byte
		n := binary.PutUvarint(buf[:], uint64(len(authKey)))
		conn.Write(buf[:n])
		conn.Write([]byte(authKey))

		conn.SetWriteDeadline(time.Time{})
	}

	// Read auth response
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
	w.WriteString(fmt.Sprintf("%T", schema))

	w.WriteTo(c.conn)

	r := NewReader(c.conn)
	resp, err := ReadResponse(r)
	if err != nil {
		return err
	}
	if !resp.Success {
		return fmt.Errorf(resp.Error)
	}
	return nil
}

func (c *Client) CreateTable(name string, schema interface{}) error {
	w := NewWriter(nil)
	w.WriteByte(OpCreateTable)
	w.WriteString(name)
	w.WriteString(fmt.Sprintf("%T", schema))

	w.WriteTo(c.conn)

	r := NewReader(c.conn)
	resp, err := ReadResponse(r)
	if err != nil {
		return err
	}
	if !resp.Success {
		return fmt.Errorf(resp.Error)
	}
	return nil
}

func (c *Client) ListTables() ([]string, error) {
	w := NewWriter(nil)
	w.WriteByte(OpListTables)

	w.WriteTo(c.conn)

	r := NewReader(c.conn)
	resp, err := ReadResponse(r)
	if err != nil {
		return nil, err
	}
	if !resp.Success {
		return nil, fmt.Errorf(resp.Error)
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
	recordData, err := encodeRecord(record)
	if err != nil {
		return 0, err
	}

	w := NewWriter(nil)
	w.WriteByte(OpInsert)
	w.WriteString(table)
	w.WriteBytes(recordData)

	w.WriteTo(c.conn)

	r := NewReader(c.conn)
	resp, err := ReadResponse(r)
	if err != nil {
		return 0, err
	}
	if !resp.Success {
		return 0, fmt.Errorf(resp.Error)
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

	w.WriteTo(c.conn)

	r := NewReader(c.conn)
	resp, err := ReadResponse(r)
	if err != nil {
		return nil, err
	}
	if !resp.Success {
		return nil, fmt.Errorf(resp.Error)
	}

	return resp.Data, nil
}

func (c *Client) Update(table string, id uint32, record interface{}) error {
	recordData, err := encodeRecord(record)
	if err != nil {
		return err
	}

	w := NewWriter(nil)
	w.WriteByte(OpUpdate)
	w.WriteString(table)
	w.WriteUint64(uint64(id))
	w.WriteBytes(recordData)

	w.WriteTo(c.conn)

	r := NewReader(c.conn)
	resp, err := ReadResponse(r)
	if err != nil {
		return err
	}
	if !resp.Success {
		return fmt.Errorf(resp.Error)
	}
	return nil
}

func (c *Client) Delete(table string, id uint32) error {
	w := NewWriter(nil)
	w.WriteByte(OpDelete)
	w.WriteString(table)
	w.WriteUint64(uint64(id))

	w.WriteTo(c.conn)

	r := NewReader(c.conn)
	resp, err := ReadResponse(r)
	if err != nil {
		return err
	}
	if !resp.Success {
		return fmt.Errorf(resp.Error)
	}
	return nil
}

func (c *Client) Query(table, field string, value interface{}) ([]uint32, error) {
	valueData, err := EncodeValue(value)
	if err != nil {
		return nil, err
	}

	w := NewWriter(nil)
	w.WriteByte(OpQuery)
	w.WriteString(table)
	w.WriteString(field)
	w.WriteBytes(valueData)

	w.WriteTo(c.conn)

	r := NewReader(c.conn)
	resp, err := ReadResponse(r)
	if err != nil {
		return nil, err
	}
	if !resp.Success {
		return nil, fmt.Errorf(resp.Error)
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

	w.WriteTo(c.conn)

	r := NewReader(c.conn)
	resp, err := ReadResponse(r)
	if err != nil {
		return 0, err
	}
	if !resp.Success {
		return 0, fmt.Errorf(resp.Error)
	}

	if len(resp.Data) >= 4 {
		return binary.BigEndian.Uint32(resp.Data), nil
	}
	return 0, nil
}

func encodeRecord(record interface{}) ([]byte, error) {
	w := NewWriter(nil)

	val := reflect.ValueOf(record)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	typ := val.Type()

	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		if !field.IsExported() {
			continue
		}

		w.WriteString(field.Name)
		w.WriteByte(0x01) // Start marker

		fieldVal := val.Field(i)
		switch fieldVal.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			w.WriteInt64(fieldVal.Int())
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			w.WriteUint64(fieldVal.Uint())
		case reflect.Float32:
			w.WriteFloat32(float32(fieldVal.Float()))
		case reflect.Float64:
			w.WriteFloat64(fieldVal.Float())
		case reflect.String:
			w.WriteString(fieldVal.String())
		case reflect.Bool:
			w.WriteBool(fieldVal.Bool())
		}

		w.WriteByte(0x04) // End marker
	}

	return w.Bytes(), nil
}
