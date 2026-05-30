package netembeddb

import (
	"encoding/binary"
	"fmt"
	"net"
	"reflect"
	"time"

	embedcore "github.com/yay101/embeddbcore"
	"github.com/yay101/netembeddb/protocol"
)

type Client struct {
	conn    net.Conn
	schemas map[string]*embedcore.StructLayout
}

type ClientOptions struct {
	AuthKey string
}

func Dial(addr string, opts ...ClientOptions) (*Client, error) {
	var authKey string
	if len(opts) > 0 {
		authKey = opts[0].AuthKey
	}

	var conn net.Conn
	var err error

	if len(addr) > 0 && addr[0] == '/' {
		conn, err = net.DialTimeout("unix", addr, 5*time.Second)
	} else {
		conn, err = net.DialTimeout("tcp", addr, 5*time.Second)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}

	if authKey != "" {
		conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
		_, err := conn.Write([]byte(authKey))
		if err != nil {
			conn.Close()
			return nil, err
		}

		dec := protocol.NewDecoder(conn)
		resp, err := dec.DecodeResponse()
		if err != nil {
			conn.Close()
			return nil, err
		}
		if !resp.Success {
			conn.Close()
			return nil, fmt.Errorf("authentication failed: %s", resp.Error)
		}
	}

	return &Client{
		conn:    conn,
		schemas: make(map[string]*embedcore.StructLayout),
	}, nil
}

func (c *Client) Close() error {
	if c.conn == nil {
		return nil
	}
	hdr := make([]byte, 5)
	hdr[0] = byte(protocol.OpClose)
	binary.BigEndian.PutUint32(hdr[1:5], 0)
	c.conn.Write(hdr)
	return c.conn.Close()
}

func (c *Client) connReader() *protocol.Decoder {
	return protocol.NewDecoder(c.conn)
}

func (c *Client) sendOp(op protocol.OpCode, payload []byte) error {
	hdr := make([]byte, 5)
	hdr[0] = byte(op)
	binary.BigEndian.PutUint32(hdr[1:5], uint32(len(payload)))

	if _, err := c.conn.Write(hdr); err != nil {
		return err
	}
	if len(payload) > 0 {
		if _, err := c.conn.Write(payload); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) RegisterSchema(tableName string, layout *embedcore.StructLayout) error {
	c.schemas[tableName] = layout

	buf := make([]byte, 0, 256)
	buf = append(buf, protocol.EncodeString(tableName)...)
	buf = append(buf, protocol.EncodeString(layout.PrimaryKey)...)
	buf = append(buf, byte(layout.PKType))

	var indexableCount int
	for _, field := range layout.Fields {
		if !field.Primary && field.Offset > 0 && !field.IsSlice && !field.IsStruct {
			indexableCount++
		}
	}
	buf = append(buf, byte(indexableCount))

	for _, field := range layout.Fields {
		if !field.Primary && field.Offset > 0 && !field.IsSlice && !field.IsStruct {
			buf = append(buf, protocol.EncodeString(field.Name)...)
			buf = append(buf, protocol.EncodeString(field.Name)...)
			buf = append(buf, protocol.EncodeUint64(uint64(field.Type))...)
		}
	}

	if err := c.sendOp(protocol.OpRegisterSchema, buf); err != nil {
		return err
	}

	dec := c.connReader()
	resp, err := dec.DecodeResponse()
	if err != nil {
		return err
	}
	if !resp.Success {
		return fmt.Errorf("register schema failed: %s", resp.Error)
	}

	return nil
}

func encodeTLVRecord(record any, layout *embedcore.StructLayout) ([]byte, error) {
	buf := make([]byte, 0, 256)
	for _, field := range layout.Fields {
		if field.IsStruct && !field.IsTime && !field.IsSlice && !field.IsMap {
			continue
		}
		if field.IsSlice || field.IsMap {
			continue
		}

		val, err := embedcore.GetFieldValue(record, field)
		if err != nil {
			continue
		}
		if val == nil {
			continue
		}

		fieldType := field.Type
		if field.IsTime {
			fieldType = reflect.Struct
		}
		encodedVal, err := protocol.EncodeFieldValue(val, fieldType)
		if err != nil {
			return nil, fmt.Errorf("encode field %s: %w", field.Name, err)
		}

		buf = embedcore.EncodeTLVField(buf, field.Name, encodedVal)
	}
	return buf, nil
}

func decodeTLVRecord(data []byte, layout *embedcore.StructLayout, result any) error {
	remaining := data
	for len(remaining) > 0 {
		name, value, rem, err := embedcore.DecodeTLVField(remaining)
		if err != nil {
			break
		}
		remaining = rem

		for _, field := range layout.Fields {
			if field.Name == name {
				decodedVal, err := protocol.DecodeFieldValueByKind(value, field.Type)
				if err != nil {
					continue
				}
				embedcore.SetFieldValue(result, field, decodedVal)
				break
			}
		}
	}
	return nil
}

func (c *Client) Insert(tableName string, data []byte) (uint32, error) {
	buf := make([]byte, 0, 256)
	buf = append(buf, protocol.EncodeString(tableName)...)
	buf = append(buf, data...)

	if err := c.sendOp(protocol.OpInsert, buf); err != nil {
		return 0, err
	}

	resp, err := c.connReader().DecodeResponse()
	if err != nil {
		return 0, err
	}
	if !resp.Success {
		return 0, fmt.Errorf("insert failed: %s", resp.Error)
	}

	if len(resp.Data) < 4 {
		return 0, fmt.Errorf("invalid response data")
	}
	return binary.BigEndian.Uint32(resp.Data), nil
}

func (c *Client) Get(tableName string, id uint32) ([]byte, error) {
	buf := make([]byte, 0, 256)
	buf = append(buf, protocol.EncodeString(tableName)...)
	buf = append(buf, protocol.EncodeUint32(id)...)

	if err := c.sendOp(protocol.OpGet, buf); err != nil {
		return nil, err
	}

	resp, err := c.connReader().DecodeResponse()
	if err != nil {
		return nil, err
	}
	if !resp.Success {
		return nil, fmt.Errorf("get failed: %s", resp.Error)
	}

	return resp.Data, nil
}

func (c *Client) Update(tableName string, id uint32, data []byte) error {
	buf := make([]byte, 0, 256)
	buf = append(buf, protocol.EncodeString(tableName)...)
	buf = append(buf, protocol.EncodeUint32(id)...)
	buf = append(buf, data...)

	if err := c.sendOp(protocol.OpUpdate, buf); err != nil {
		return err
	}

	resp, err := c.connReader().DecodeResponse()
	if err != nil {
		return err
	}
	if !resp.Success {
		return fmt.Errorf("update failed: %s", resp.Error)
	}

	return nil
}

func (c *Client) Delete(tableName string, id uint32) error {
	buf := make([]byte, 0, 256)
	buf = append(buf, protocol.EncodeString(tableName)...)
	buf = append(buf, protocol.EncodeUint32(id)...)

	if err := c.sendOp(protocol.OpDelete, buf); err != nil {
		return err
	}

	resp, err := c.connReader().DecodeResponse()
	if err != nil {
		return err
	}
	if !resp.Success {
		return fmt.Errorf("delete failed: %s", resp.Error)
	}

	return nil
}

func (c *Client) Count(tableName string) (uint64, error) {
	buf := protocol.EncodeString(tableName)

	if err := c.sendOp(protocol.OpCount, buf); err != nil {
		return 0, err
	}

	resp, err := c.connReader().DecodeResponse()
	if err != nil {
		return 0, err
	}
	if !resp.Success {
		return 0, fmt.Errorf("count failed: %s", resp.Error)
	}

	if len(resp.Data) < 8 {
		return 0, fmt.Errorf("invalid response data")
	}
	return binary.BigEndian.Uint64(resp.Data), nil
}

func (c *Client) Upsert(tableName string, id uint32, data []byte) (uint32, bool, error) {
	buf := make([]byte, 0, 256)
	buf = append(buf, protocol.EncodeString(tableName)...)
	buf = append(buf, protocol.EncodeUint32(id)...)
	buf = append(buf, data...)

	if err := c.sendOp(protocol.OpUpsert, buf); err != nil {
		return 0, false, err
	}

	resp, err := c.connReader().DecodeResponse()
	if err != nil {
		return 0, false, err
	}
	if !resp.Success {
		return 0, false, fmt.Errorf("upsert failed: %s", resp.Error)
	}

	if len(resp.Data) < 5 {
		return 0, false, fmt.Errorf("invalid response data")
	}
	inserted := resp.Data[0] != 0
	recordID := binary.BigEndian.Uint32(resp.Data[1:5])
	return recordID, inserted, nil
}

func (c *Client) InsertMany(tableName string, records [][]byte) ([]uint32, error) {
	buf := make([]byte, 0, 1024)
	buf = append(buf, protocol.EncodeString(tableName)...)
	buf = append(buf, protocol.EncodeUint64(uint64(len(records)))...)
	for _, rec := range records {
		buf = append(buf, protocol.EncodeBytes(rec)...)
	}

	if err := c.sendOp(protocol.OpInsertMany, buf); err != nil {
		return nil, err
	}

	resp, err := c.connReader().DecodeResponse()
	if err != nil {
		return nil, err
	}
	if !resp.Success {
		return nil, fmt.Errorf("insertMany failed: %s", resp.Error)
	}

	return decodeUint32Slice(resp.Data)
}

func (c *Client) InsertManyBulk(tableName string, records [][]byte) ([]uint32, error) {
	buf := make([]byte, 0, 1024)
	buf = append(buf, protocol.EncodeString(tableName)...)
	buf = append(buf, protocol.EncodeUint64(uint64(len(records)))...)
	for _, rec := range records {
		buf = append(buf, protocol.EncodeBytes(rec)...)
	}

	if err := c.sendOp(protocol.OpInsertManyBulk, buf); err != nil {
		return nil, err
	}

	resp, err := c.connReader().DecodeResponse()
	if err != nil {
		return nil, err
	}
	if !resp.Success {
		return nil, fmt.Errorf("insertManyBulk failed: %s", resp.Error)
	}

	return decodeUint32Slice(resp.Data)
}

func (c *Client) DeleteMany(tableName string, ids []uint32) (int, error) {
	buf := make([]byte, 0, 256)
	buf = append(buf, protocol.EncodeString(tableName)...)
	buf = append(buf, protocol.EncodeUint64(uint64(len(ids)))...)
	for _, id := range ids {
		buf = append(buf, protocol.EncodeUint32(id)...)
	}

	if err := c.sendOp(protocol.OpDeleteMany, buf); err != nil {
		return 0, err
	}

	resp, err := c.connReader().DecodeResponse()
	if err != nil {
		return 0, err
	}
	if !resp.Success {
		return 0, fmt.Errorf("deleteMany failed: %s", resp.Error)
	}

	if len(resp.Data) < 4 {
		return 0, fmt.Errorf("invalid response data")
	}
	return int(binary.BigEndian.Uint32(resp.Data)), nil
}

func (c *Client) Query(tableName, fieldName string, value any) ([][]byte, error) {
	buf := make([]byte, 0, 256)
	buf = append(buf, protocol.EncodeString(tableName)...)
	buf = append(buf, protocol.EncodeString(fieldName)...)
	buf = append(buf, protocol.EncodeString(fmt.Sprintf("%v", value))...)

	if err := c.sendOp(protocol.OpQuery, buf); err != nil {
		return nil, err
	}

	resp, err := c.connReader().DecodeResponse()
	if err != nil {
		return nil, err
	}
	if !resp.Success {
		return nil, fmt.Errorf("query failed: %s", resp.Error)
	}

	return decodeBytesSlice(resp.Data)
}

func (c *Client) QueryRange(tableName, fieldName string, op string, value any, inclusive bool, maxValue any) ([][]byte, error) {
	buf := make([]byte, 0, 256)
	buf = append(buf, protocol.EncodeString(tableName)...)
	buf = append(buf, protocol.EncodeString(fieldName)...)

	var flags byte
	switch op {
	case "gt":
		flags |= protocol.RangeFlagGT
	case "lt":
	default:
	}
	if inclusive {
		flags |= protocol.RangeFlagIncl
	}
	if maxValue != nil {
		flags |= protocol.RangeFlagBtwn
	}
	buf = append(buf, flags)
	buf = append(buf, protocol.EncodeString(fmt.Sprintf("%v", value))...)
	if maxValue != nil {
		buf = append(buf, protocol.EncodeString(fmt.Sprintf("%v", maxValue))...)
	}

	if err := c.sendOp(protocol.OpQueryRange, buf); err != nil {
		return nil, err
	}

	resp, err := c.connReader().DecodeResponse()
	if err != nil {
		return nil, err
	}
	if !resp.Success {
		return nil, fmt.Errorf("queryRange failed: %s", resp.Error)
	}

	return decodeBytesSlice(resp.Data)
}

func (c *Client) QueryLike(tableName, fieldName, pattern string) ([][]byte, error) {
	buf := make([]byte, 0, 256)
	buf = append(buf, protocol.EncodeString(tableName)...)
	buf = append(buf, protocol.EncodeString(fieldName)...)
	buf = append(buf, protocol.EncodeString(pattern)...)

	if err := c.sendOp(protocol.OpQueryLike, buf); err != nil {
		return nil, err
	}

	resp, err := c.connReader().DecodeResponse()
	if err != nil {
		return nil, err
	}
	if !resp.Success {
		return nil, fmt.Errorf("queryLike failed: %s", resp.Error)
	}

	return decodeBytesSlice(resp.Data)
}

func (c *Client) Scan(tableName string) ([][]byte, error) {
	buf := protocol.EncodeString(tableName)

	if err := c.sendOp(protocol.OpScan, buf); err != nil {
		return nil, err
	}

	resp, err := c.connReader().DecodeResponse()
	if err != nil {
		return nil, err
	}
	if !resp.Success {
		return nil, fmt.Errorf("scan failed: %s", resp.Error)
	}

	return decodeBytesSlice(resp.Data)
}

func (c *Client) All(tableName string) ([][]byte, error) {
	return c.Scan(tableName)
}

func (c *Client) Drop(tableName string) error {
	buf := protocol.EncodeString(tableName)

	if err := c.sendOp(protocol.OpDrop, buf); err != nil {
		return err
	}

	resp, err := c.connReader().DecodeResponse()
	if err != nil {
		return err
	}
	if !resp.Success {
		return fmt.Errorf("drop failed: %s", resp.Error)
	}

	return nil
}

func (c *Client) CreateIndex(tableName, fieldName string) error {
	buf := make([]byte, 0, 256)
	buf = append(buf, protocol.EncodeString(tableName)...)
	buf = append(buf, protocol.EncodeString(fieldName)...)

	if err := c.sendOp(protocol.OpCreateIndex, buf); err != nil {
		return err
	}

	resp, err := c.connReader().DecodeResponse()
	if err != nil {
		return err
	}
	if !resp.Success {
		return fmt.Errorf("createIndex failed: %s", resp.Error)
	}

	return nil
}

func (c *Client) DropIndex(tableName, fieldName string) error {
	buf := make([]byte, 0, 256)
	buf = append(buf, protocol.EncodeString(tableName)...)
	buf = append(buf, protocol.EncodeString(fieldName)...)

	if err := c.sendOp(protocol.OpDropIndex, buf); err != nil {
		return err
	}

	resp, err := c.connReader().DecodeResponse()
	if err != nil {
		return err
	}
	if !resp.Success {
		return fmt.Errorf("dropIndex failed: %s", resp.Error)
	}

	return nil
}

func (c *Client) GetIndexedFields(tableName string) ([]string, error) {
	buf := protocol.EncodeString(tableName)

	if err := c.sendOp(protocol.OpGetIndexed, buf); err != nil {
		return nil, err
	}

	resp, err := c.connReader().DecodeResponse()
	if err != nil {
		return nil, err
	}
	if !resp.Success {
		return nil, fmt.Errorf("getIndexedFields failed: %s", resp.Error)
	}

	return decodeStringSlice(resp.Data)
}

func (c *Client) Vacuum() error {
	if err := c.sendOp(protocol.OpVacuum, nil); err != nil {
		return err
	}

	resp, err := c.connReader().DecodeResponse()
	if err != nil {
		return err
	}
	if !resp.Success {
		return fmt.Errorf("vacuum failed: %s", resp.Error)
	}

	return nil
}

func (c *Client) Sync() error {
	if err := c.sendOp(protocol.OpSync, nil); err != nil {
		return err
	}

	resp, err := c.connReader().DecodeResponse()
	if err != nil {
		return err
	}
	if !resp.Success {
		return fmt.Errorf("sync failed: %s", resp.Error)
	}

	return nil
}

func (c *Client) Begin() error {
	if err := c.sendOp(protocol.OpBegin, nil); err != nil {
		return err
	}

	resp, err := c.connReader().DecodeResponse()
	if err != nil {
		return err
	}
	if !resp.Success {
		return fmt.Errorf("begin failed: %s", resp.Error)
	}

	return nil
}

func (c *Client) Commit() error {
	if err := c.sendOp(protocol.OpCommit, nil); err != nil {
		return err
	}

	resp, err := c.connReader().DecodeResponse()
	if err != nil {
		return err
	}
	if !resp.Success {
		return fmt.Errorf("commit failed: %s", resp.Error)
	}

	return nil
}

func (c *Client) Rollback() error {
	if err := c.sendOp(protocol.OpRollback, nil); err != nil {
		return err
	}

	resp, err := c.connReader().DecodeResponse()
	if err != nil {
		return err
	}
	if !resp.Success {
		return fmt.Errorf("rollback failed: %s", resp.Error)
	}

	return nil
}

func (c *Client) Backup(destPath string) error {
	buf := protocol.EncodeString(destPath)

	if err := c.sendOp(protocol.OpBackup, buf); err != nil {
		return err
	}

	resp, err := c.connReader().DecodeResponse()
	if err != nil {
		return err
	}
	if !resp.Success {
		return fmt.Errorf("backup failed: %s", resp.Error)
	}

	return nil
}

func (c *Client) Stats() (*protocol.StatsInfo, error) {
	if err := c.sendOp(protocol.OpStats, nil); err != nil {
		return nil, err
	}

	resp, err := c.connReader().DecodeResponse()
	if err != nil {
		return nil, err
	}
	if !resp.Success {
		return nil, fmt.Errorf("stats failed: %s", resp.Error)
	}

	return protocol.DecodeStats(resp.Data)
}

func decodeUint32Slice(data []byte) ([]uint32, error) {
	pos := 0
	if pos+4 > len(data) {
		return nil, fmt.Errorf("invalid data")
	}
	count := binary.BigEndian.Uint32(data[pos:])
	pos += 4

	results := make([]uint32, 0, count)
	for i := uint32(0); i < count; i++ {
		if pos+4 > len(data) {
			break
		}
		results = append(results, binary.BigEndian.Uint32(data[pos:]))
		pos += 4
	}
	return results, nil
}

func decodeBytesSlice(data []byte) ([][]byte, error) {
	pos := 0
	if pos+4 > len(data) {
		return nil, fmt.Errorf("invalid data")
	}
	count := binary.BigEndian.Uint32(data[pos:])
	pos += 4

	results := make([][]byte, 0, count)
	for i := uint32(0); i < count; i++ {
		if pos+4 > len(data) {
			break
		}
		recLen := binary.BigEndian.Uint32(data[pos:])
		pos += 4
		if recLen == 0 {
			results = append(results, nil)
			continue
		}
		if pos+int(recLen) > len(data) {
			break
		}
		rec := make([]byte, recLen)
		copy(rec, data[pos:pos+int(recLen)])
		results = append(results, rec)
		pos += int(recLen)
	}
	return results, nil
}

func decodeStringSlice(data []byte) ([]string, error) {
	pos := 0
	if pos+4 > len(data) {
		return nil, fmt.Errorf("invalid data")
	}
	count := binary.BigEndian.Uint32(data[pos:])
	pos += 4

	results := make([]string, 0, count)
	for i := uint32(0); i < count; i++ {
		s, n, err := protocol.DecodeString(data[pos:])
		if err != nil {
			break
		}
		results = append(results, s)
		pos += n
	}
	return results, nil
}

func encodeBytesSlice(records [][]byte) []byte {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(len(records)))
	for _, rec := range records {
		lenBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(lenBytes, uint32(len(rec)))
		buf = append(buf, lenBytes...)
		buf = append(buf, rec...)
	}
	return buf
}

func SchemaFromStruct[T any](name string) (*embedcore.StructLayout, error) {
	var instance T
	layout, err := embedcore.ComputeStructLayout(instance)
	if err != nil {
		return nil, err
	}
	_ = name
	return layout, nil
}

func EncodeRecord(record any, layout *embedcore.StructLayout) ([]byte, error) {
	return encodeTLVRecord(record, layout)
}

func DecodeRecord(data []byte, layout *embedcore.StructLayout, result any) error {
	return decodeTLVRecord(data, layout, result)
}
