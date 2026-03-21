package netembeddb

import (
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/yay101/netembeddb/protocol"
)

type Client struct {
	conn net.Conn
}

func Dial(addr string) (*Client, error) {
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		if len(addr) > 0 && addr[0] == '/' {
			conn, err = net.DialTimeout("unix", addr, 5*time.Second)
			if err != nil {
				return nil, fmt.Errorf("failed to connect: %w", err)
			}
		} else {
			return nil, fmt.Errorf("failed to connect: %w", err)
		}
	}

	client := &Client{conn: conn}
	if err := client.readAuthResponse(); err != nil {
		conn.Close()
		return nil, err
	}

	return client, nil
}

func (c *Client) readAuthResponse() error {
	buf := make([]byte, 1)
	c.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, err := c.conn.Read(buf)
	if err != nil {
		return err
	}
	if buf[0] == 0 {
		r := protocol.NewReader(c.conn)
		errMsg, _ := r.ReadString()
		return errors.New(errMsg)
	}
	return nil
}

func (c *Client) Close() error {
	if c.conn == nil {
		return nil
	}
	c.conn.Write([]byte{protocol.OpClose})
	return c.conn.Close()
}

func (c *Client) Insert(table string, data []byte) (uint32, error) {
	w := protocol.NewWriter()
	w.WriteByte(protocol.OpInsert)
	w.WriteString(table)
	w.WriteBytes(data)

	_, err := c.conn.Write(w.Bytes())
	if err != nil {
		return 0, err
	}

	r := protocol.NewReader(c.conn)
	resp, err := readResponse(r)
	if err != nil {
		return 0, err
	}
	if !resp.Success {
		return 0, errors.New(resp.Error)
	}

	reader := bytesToReader(resp.Data)
	id, _ := reader.ReadUint64()
	return uint32(id), nil
}

func (c *Client) Get(table string, id uint32) ([]byte, error) {
	w := protocol.NewWriter()
	w.WriteByte(protocol.OpGet)
	w.WriteString(table)
	w.WriteUint64(uint64(id))

	_, err := c.conn.Write(w.Bytes())
	if err != nil {
		return nil, err
	}

	r := protocol.NewReader(c.conn)
	resp, err := readResponse(r)
	if err != nil {
		return nil, err
	}
	if !resp.Success {
		return nil, errors.New(resp.Error)
	}

	reader := bytesToReader(resp.Data)
	data, _ := reader.ReadBytes()
	return data, nil
}

func (c *Client) Update(table string, id uint32, data []byte) error {
	w := protocol.NewWriter()
	w.WriteByte(protocol.OpUpdate)
	w.WriteString(table)
	w.WriteUint64(uint64(id))
	w.WriteBytes(data)

	_, err := c.conn.Write(w.Bytes())
	if err != nil {
		return err
	}

	r := protocol.NewReader(c.conn)
	resp, err := readResponse(r)
	if err != nil {
		return err
	}
	if !resp.Success {
		return errors.New(resp.Error)
	}

	return nil
}

func (c *Client) Delete(table string, id uint32) error {
	w := protocol.NewWriter()
	w.WriteByte(protocol.OpDelete)
	w.WriteString(table)
	w.WriteUint64(uint64(id))

	_, err := c.conn.Write(w.Bytes())
	if err != nil {
		return err
	}

	r := protocol.NewReader(c.conn)
	resp, err := readResponse(r)
	if err != nil {
		return err
	}
	if !resp.Success {
		return errors.New(resp.Error)
	}

	return nil
}

func (c *Client) Query(table, field string, value interface{}) ([]uint32, error) {
	valueData, err := encodeValue(value)
	if err != nil {
		return nil, err
	}

	w := protocol.NewWriter()
	w.WriteByte(protocol.OpQuery)
	w.WriteString(table)
	w.WriteString(field)
	w.WriteBytes(valueData)

	_, err = c.conn.Write(w.Bytes())
	if err != nil {
		return nil, err
	}

	r := protocol.NewReader(c.conn)
	resp, err := readResponse(r)
	if err != nil {
		return nil, err
	}
	if !resp.Success {
		return nil, errors.New(resp.Error)
	}

	reader := bytesToReader(resp.Data)
	count, _ := reader.ReadUint64()
	ids := make([]uint32, count)
	for i := uint64(0); i < count; i++ {
		id, _ := reader.ReadUint64()
		ids[i] = uint32(id)
	}
	return ids, nil
}

func (c *Client) QueryRangeGreaterThan(table, field string, min interface{}, inclusive bool) ([]uint32, error) {
	minData, err := encodeValue(min)
	if err != nil {
		return nil, err
	}

	flags := byte(0)
	if inclusive {
		flags |= byte(protocol.RangeMinInclusive)
	}

	w := protocol.NewWriter()
	w.WriteByte(protocol.OpQueryRange)
	w.WriteString(table)
	w.WriteString(field)
	w.WriteByte(byte(protocol.QueryRangeGreaterThan))
	w.WriteBytes(minData)
	w.WriteBytes(nil)
	w.WriteByte(flags)

	_, err = c.conn.Write(w.Bytes())
	if err != nil {
		return nil, err
	}

	r := protocol.NewReader(c.conn)
	resp, err := readResponse(r)
	if err != nil {
		return nil, err
	}
	if !resp.Success {
		return nil, errors.New(resp.Error)
	}

	reader := bytesToReader(resp.Data)
	count, _ := reader.ReadUint64()
	ids := make([]uint32, count)
	for i := uint64(0); i < count; i++ {
		id, _ := reader.ReadUint64()
		ids[i] = uint32(id)
	}
	return ids, nil
}

func (c *Client) QueryRangeLessThan(table, field string, max interface{}, inclusive bool) ([]uint32, error) {
	maxData, err := encodeValue(max)
	if err != nil {
		return nil, err
	}

	flags := byte(0)
	if inclusive {
		flags |= byte(protocol.RangeMaxInclusive)
	}

	w := protocol.NewWriter()
	w.WriteByte(protocol.OpQueryRange)
	w.WriteString(table)
	w.WriteString(field)
	w.WriteByte(byte(protocol.QueryRangeLessThan))
	w.WriteBytes(nil)
	w.WriteBytes(maxData)
	w.WriteByte(flags)

	_, err = c.conn.Write(w.Bytes())
	if err != nil {
		return nil, err
	}

	r := protocol.NewReader(c.conn)
	resp, err := readResponse(r)
	if err != nil {
		return nil, err
	}
	if !resp.Success {
		return nil, errors.New(resp.Error)
	}

	reader := bytesToReader(resp.Data)
	count, _ := reader.ReadUint64()
	ids := make([]uint32, count)
	for i := uint64(0); i < count; i++ {
		id, _ := reader.ReadUint64()
		ids[i] = uint32(id)
	}
	return ids, nil
}

func (c *Client) QueryRangeBetween(table, field string, min, max interface{}, inclusiveMin, inclusiveMax bool) ([]uint32, error) {
	minData, err := encodeValue(min)
	if err != nil {
		return nil, err
	}

	maxData, err := encodeValue(max)
	if err != nil {
		return nil, err
	}

	flags := byte(0)
	if inclusiveMin {
		flags |= byte(protocol.RangeMinInclusive)
	}
	if inclusiveMax {
		flags |= byte(protocol.RangeMaxInclusive)
	}

	w := protocol.NewWriter()
	w.WriteByte(protocol.OpQueryRange)
	w.WriteString(table)
	w.WriteString(field)
	w.WriteByte(byte(protocol.QueryRangeBetween))
	w.WriteBytes(minData)
	w.WriteBytes(maxData)
	w.WriteByte(flags)

	_, err = c.conn.Write(w.Bytes())
	if err != nil {
		return nil, err
	}

	r := protocol.NewReader(c.conn)
	resp, err := readResponse(r)
	if err != nil {
		return nil, err
	}
	if !resp.Success {
		return nil, errors.New(resp.Error)
	}

	reader := bytesToReader(resp.Data)
	count, _ := reader.ReadUint64()
	ids := make([]uint32, count)
	for i := uint64(0); i < count; i++ {
		id, _ := reader.ReadUint64()
		ids[i] = uint32(id)
	}
	return ids, nil
}

func (c *Client) Scan(table string) ([][]byte, error) {
	w := protocol.NewWriter()
	w.WriteByte(protocol.OpScan)
	w.WriteString(table)

	_, err := c.conn.Write(w.Bytes())
	if err != nil {
		return nil, err
	}

	r := protocol.NewReader(c.conn)
	resp, err := readResponse(r)
	if err != nil {
		return nil, err
	}
	if !resp.Success {
		return nil, errors.New(resp.Error)
	}

	reader := bytesToReader(resp.Data)
	count, _ := reader.ReadUint64()
	records := make([][]byte, count)
	for i := uint64(0); i < count; i++ {
		data, _ := reader.ReadBytes()
		records[i] = data
	}
	return records, nil
}

func (c *Client) Count(table string) (uint32, error) {
	w := protocol.NewWriter()
	w.WriteByte(protocol.OpCount)
	w.WriteString(table)

	_, err := c.conn.Write(w.Bytes())
	if err != nil {
		return 0, err
	}

	r := protocol.NewReader(c.conn)
	resp, err := readResponse(r)
	if err != nil {
		return 0, err
	}
	if !resp.Success {
		return 0, errors.New(resp.Error)
	}

	reader := bytesToReader(resp.Data)
	count, _ := reader.ReadUint64()
	return uint32(count), nil
}

func (c *Client) Vacuum() error {
	w := protocol.NewWriter()
	w.WriteByte(protocol.OpVacuum)

	_, err := c.conn.Write(w.Bytes())
	if err != nil {
		return err
	}

	r := protocol.NewReader(c.conn)
	resp, err := readResponse(r)
	if err != nil {
		return err
	}
	if !resp.Success {
		return errors.New(resp.Error)
	}

	return nil
}

func (c *Client) CreateTable(table string) error {
	w := protocol.NewWriter()
	w.WriteByte(protocol.OpCreateTable)
	w.WriteString(table)

	_, err := c.conn.Write(w.Bytes())
	if err != nil {
		return err
	}

	r := protocol.NewReader(c.conn)
	resp, err := readResponse(r)
	if err != nil {
		return err
	}
	if !resp.Success {
		return errors.New(resp.Error)
	}

	return nil
}

func (c *Client) CreateIndex(table, field string) error {
	w := protocol.NewWriter()
	w.WriteByte(protocol.OpCreateIndex)
	w.WriteString(table)
	w.WriteString(field)

	_, err := c.conn.Write(w.Bytes())
	if err != nil {
		return err
	}

	r := protocol.NewReader(c.conn)
	resp, err := readResponse(r)
	if err != nil {
		return err
	}
	if !resp.Success {
		return errors.New(resp.Error)
	}

	return nil
}

func (c *Client) Filter(table, field string, op protocol.FilterOperator, value interface{}) ([]uint32, error) {
	valueData, err := encodeValue(value)
	if err != nil {
		return nil, err
	}

	w := protocol.NewWriter()
	w.WriteByte(protocol.OpFilter)
	w.WriteString(table)
	w.WriteString(field)
	w.WriteByte(byte(op))
	w.WriteBytes(valueData)

	_, err = c.conn.Write(w.Bytes())
	if err != nil {
		return nil, err
	}

	r := protocol.NewReader(c.conn)
	resp, err := readResponse(r)
	if err != nil {
		return nil, err
	}
	if !resp.Success {
		return nil, errors.New(resp.Error)
	}

	reader := bytesToReader(resp.Data)
	count, _ := reader.ReadUint64()
	ids := make([]uint32, count)
	for i := uint64(0); i < count; i++ {
		id, _ := reader.ReadUint64()
		ids[i] = uint32(id)
	}
	return ids, nil
}

type FilterPagedResult struct {
	TotalCount uint32
	HasMore    bool
	Records    [][]byte
}

func (c *Client) FilterPaged(table, field string, op protocol.FilterOperator, value interface{}, offset, limit int) (*FilterPagedResult, error) {
	valueData, err := encodeValue(value)
	if err != nil {
		return nil, err
	}

	w := protocol.NewWriter()
	w.WriteByte(protocol.OpFilterPaged)
	w.WriteString(table)
	w.WriteString(field)
	w.WriteByte(byte(op))
	w.WriteBytes(valueData)
	w.WriteUint64(uint64(offset))
	w.WriteUint64(uint64(limit))

	_, err = c.conn.Write(w.Bytes())
	if err != nil {
		return nil, err
	}

	r := protocol.NewReader(c.conn)
	resp, err := readResponse(r)
	if err != nil {
		return nil, err
	}
	if !resp.Success {
		return nil, errors.New(resp.Error)
	}

	reader := bytesToReader(resp.Data)
	totalCount, _ := reader.ReadUint64()
	hasMore, _ := reader.ReadBool()
	recordCount, _ := reader.ReadUint64()

	records := make([][]byte, recordCount)
	for i := uint64(0); i < recordCount; i++ {
		data, _ := reader.ReadBytes()
		records[i] = data
	}

	return &FilterPagedResult{
		TotalCount: uint32(totalCount),
		HasMore:    hasMore,
		Records:    records,
	}, nil
}

func readResponse(r *protocol.Reader) (*Response, error) {
	msgType, err := r.ReadByte()
	if err != nil {
		return nil, err
	}

	resp := &Response{}

	switch msgType {
	case protocol.OpResponse:
		resp.Success = true
		resp.Data, _ = r.ReadBytes()
	case protocol.OpError:
		resp.Success = false
		resp.Error, _ = r.ReadString()
	default:
		return nil, fmt.Errorf("unexpected message type: %d", msgType)
	}

	return resp, nil
}

func bytesToReader(data []byte) *protocol.Reader {
	return protocol.NewReader(&byteReader{data: data})
}

type byteReader struct {
	data []byte
	pos  int
}

func (b *byteReader) Read(p []byte) (int, error) {
	if b.pos >= len(b.data) {
		return 0, fmt.Errorf("EOF")
	}
	n := copy(p, b.data[b.pos:])
	b.pos += n
	return n, nil
}

type Response struct {
	Success bool
	Data    []byte
	Error   string
}

func encodeValue(value interface{}) ([]byte, error) {
	w := protocol.NewWriter()

	switch v := value.(type) {
	case int:
		w.WriteVarint(int64(v))
	case int8:
		w.WriteVarint(int64(v))
	case int16:
		w.WriteVarint(int64(v))
	case int32:
		w.WriteVarint(int64(v))
	case int64:
		w.WriteVarint(v)
	case uint:
		w.WriteUvarint(uint64(v))
	case uint8:
		w.WriteUvarint(uint64(v))
	case uint16:
		w.WriteUvarint(uint64(v))
	case uint32:
		w.WriteUvarint(uint64(v))
	case uint64:
		w.WriteUvarint(v)
	case float32:
		w.WriteFloat32(v)
	case float64:
		w.WriteFloat64(v)
	case string:
		w.WriteString(v)
	case bool:
		w.WriteBool(v)
	default:
		return nil, fmt.Errorf("unsupported type: %T", value)
	}

	return w.Bytes(), nil
}
