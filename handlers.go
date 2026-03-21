package netembeddb

import (
	"fmt"
	"net"

	"github.com/yay101/embeddb"
	embedcore "github.com/yay101/embeddbcore"
	"github.com/yay101/netembeddb/protocol"
)

type Record struct {
	ID   uint32
	Data []byte
}

func (s *Server) handleInsert(r *protocol.Reader, conn net.Conn) {
	tableName, err := r.ReadString()
	if err != nil {
		s.writeError(conn, err)
		return
	}

	recordData, err := r.ReadBytes()
	if err != nil {
		s.writeError(conn, err)
		return
	}

	table, err := s.getTable(tableName)
	if err != nil {
		s.writeError(conn, err)
		return
	}

	record := Record{Data: recordData}
	id, err := table.Insert(&record)
	if err != nil {
		s.writeError(conn, err)
		return
	}

	resp := protocol.NewWriter()
	resp.WriteUint64(uint64(id))
	s.writeSuccess(conn, resp.Bytes())
}

func (s *Server) handleGet(r *protocol.Reader, conn net.Conn) {
	tableName, err := r.ReadString()
	if err != nil {
		s.writeError(conn, err)
		return
	}

	id, err := r.ReadUint64()
	if err != nil {
		s.writeError(conn, err)
		return
	}

	table, err := s.getTable(tableName)
	if err != nil {
		s.writeError(conn, err)
		return
	}

	record, err := table.Get(uint32(id))
	if err != nil {
		s.writeError(conn, err)
		return
	}

	resp := protocol.NewWriter()
	if record != nil {
		resp.WriteBytes(record.Data)
	} else {
		resp.WriteBytes(nil)
	}
	s.writeSuccess(conn, resp.Bytes())
}

func (s *Server) handleUpdate(r *protocol.Reader, conn net.Conn) {
	tableName, err := r.ReadString()
	if err != nil {
		s.writeError(conn, err)
		return
	}

	id, err := r.ReadUint64()
	if err != nil {
		s.writeError(conn, err)
		return
	}

	recordData, err := r.ReadBytes()
	if err != nil {
		s.writeError(conn, err)
		return
	}

	table, err := s.getTable(tableName)
	if err != nil {
		s.writeError(conn, err)
		return
	}

	record := Record{Data: recordData}
	err = table.Update(uint32(id), &record)
	if err != nil {
		s.writeError(conn, err)
		return
	}

	s.writeSuccess(conn, nil)
}

func (s *Server) handleDelete(r *protocol.Reader, conn net.Conn) {
	tableName, err := r.ReadString()
	if err != nil {
		s.writeError(conn, err)
		return
	}

	id, err := r.ReadUint64()
	if err != nil {
		s.writeError(conn, err)
		return
	}

	table, err := s.getTable(tableName)
	if err != nil {
		s.writeError(conn, err)
		return
	}

	err = table.Delete(uint32(id))
	if err != nil {
		s.writeError(conn, err)
		return
	}

	s.writeSuccess(conn, nil)
}

func (s *Server) handleQuery(r *protocol.Reader, conn net.Conn) {
	tableName, err := r.ReadString()
	if err != nil {
		s.writeError(conn, err)
		return
	}

	fieldName, err := r.ReadString()
	if err != nil {
		s.writeError(conn, err)
		return
	}

	valueData, err := r.ReadBytes()
	if err != nil {
		s.writeError(conn, err)
		return
	}

	table, err := s.getTable(tableName)
	if err != nil {
		s.writeError(conn, err)
		return
	}

	value := decodeValue(valueData)
	results, err := table.Query(fieldName, value)
	if err != nil {
		s.writeError(conn, err)
		return
	}

	resp := protocol.NewWriter()
	resp.WriteUint64(uint64(len(results)))
	for _, rec := range results {
		resp.WriteUint64(uint64(rec.ID))
	}
	s.writeSuccess(conn, resp.Bytes())
}

func (s *Server) handleQueryRange(r *protocol.Reader, conn net.Conn) {
	tableName, err := r.ReadString()
	if err != nil {
		s.writeError(conn, err)
		return
	}

	fieldName, err := r.ReadString()
	if err != nil {
		s.writeError(conn, err)
		return
	}

	queryType, err := r.ReadByte()
	if err != nil {
		s.writeError(conn, err)
		return
	}

	minData, err := r.ReadBytes()
	if err != nil {
		s.writeError(conn, err)
		return
	}

	maxData, err := r.ReadBytes()
	if err != nil {
		s.writeError(conn, err)
		return
	}

	flags, err := r.ReadByte()
	if err != nil {
		s.writeError(conn, err)
		return
	}

	table, err := s.getTable(tableName)
	if err != nil {
		s.writeError(conn, err)
		return
	}

	minVal := decodeValue(minData)
	maxVal := decodeValue(maxData)
	minInclusive := flags&byte(protocol.RangeMinInclusive) != 0
	maxInclusive := flags&byte(protocol.RangeMaxInclusive) != 0

	var ids []uint32

	switch queryType {
	case byte(protocol.QueryRangeGreaterThan):
		results, err := table.QueryRangeGreaterThan(fieldName, minVal, minInclusive)
		if err != nil {
			s.writeError(conn, err)
			return
		}
		for _, rec := range results {
			ids = append(ids, rec.ID)
		}
	case byte(protocol.QueryRangeLessThan):
		results, err := table.QueryRangeLessThan(fieldName, maxVal, maxInclusive)
		if err != nil {
			s.writeError(conn, err)
			return
		}
		for _, rec := range results {
			ids = append(ids, rec.ID)
		}
	case byte(protocol.QueryRangeBetween):
		results, err := table.QueryRangeBetween(fieldName, minVal, maxVal, minInclusive, maxInclusive)
		if err != nil {
			s.writeError(conn, err)
			return
		}
		for _, rec := range results {
			ids = append(ids, rec.ID)
		}
	}

	resp := protocol.NewWriter()
	resp.WriteUint64(uint64(len(ids)))
	for _, id := range ids {
		resp.WriteUint64(uint64(id))
	}
	s.writeSuccess(conn, resp.Bytes())
}

func (s *Server) handleQueryPaged(r *protocol.Reader, conn net.Conn) {
	tableName, err := r.ReadString()
	if err != nil {
		s.writeError(conn, err)
		return
	}

	fieldName, err := r.ReadString()
	if err != nil {
		s.writeError(conn, err)
		return
	}

	valueData, err := r.ReadBytes()
	if err != nil {
		s.writeError(conn, err)
		return
	}

	offset, err := r.ReadUint64()
	if err != nil {
		s.writeError(conn, err)
		return
	}

	limit, err := r.ReadUint64()
	if err != nil {
		s.writeError(conn, err)
		return
	}

	table, err := s.getTable(tableName)
	if err != nil {
		s.writeError(conn, err)
		return
	}

	result, err := table.QueryPaged(fieldName, decodeValue(valueData), int(offset), int(limit))
	if err != nil {
		s.writeError(conn, err)
		return
	}

	resp := protocol.NewWriter()
	resp.WriteUint64(uint64(result.TotalCount))
	resp.WriteBool(result.HasMore)
	resp.WriteUint64(uint64(len(result.Records)))
	for _, rec := range result.Records {
		resp.WriteBytes(rec.Data)
	}
	s.writeSuccess(conn, resp.Bytes())
}

const (
	FilterOpEqual       protocol.FilterOperator = 0x01
	FilterOpNotEqual    protocol.FilterOperator = 0x02
	FilterOpGreaterThan protocol.FilterOperator = 0x03
	FilterOpLessThan    protocol.FilterOperator = 0x04
	FilterOpGreaterOrEq protocol.FilterOperator = 0x05
	FilterOpLessOrEq    protocol.FilterOperator = 0x06
	FilterOpLike        protocol.FilterOperator = 0x07
)

func (s *Server) handleFilter(r *protocol.Reader, conn net.Conn) {
	tableName, err := r.ReadString()
	if err != nil {
		s.writeError(conn, err)
		return
	}

	fieldName, err := r.ReadString()
	if err != nil {
		s.writeError(conn, err)
		return
	}

	op, err := r.ReadByte()
	if err != nil {
		s.writeError(conn, err)
		return
	}

	valueData, err := r.ReadBytes()
	if err != nil {
		s.writeError(conn, err)
		return
	}

	table, err := s.getTable(tableName)
	if err != nil {
		s.writeError(conn, err)
		return
	}

	value := decodeValue(valueData)
	filterFn := buildFilterFn(fieldName, op, value)

	var ids []uint32
	table.Scan(func(rec Record) bool {
		if filterFn(rec.Data) {
			ids = append(ids, rec.ID)
		}
		return true
	})

	resp := protocol.NewWriter()
	resp.WriteUint64(uint64(len(ids)))
	for _, id := range ids {
		resp.WriteUint64(uint64(id))
	}
	s.writeSuccess(conn, resp.Bytes())
}

func (s *Server) handleFilterPaged(r *protocol.Reader, conn net.Conn) {
	tableName, err := r.ReadString()
	if err != nil {
		s.writeError(conn, err)
		return
	}

	fieldName, err := r.ReadString()
	if err != nil {
		s.writeError(conn, err)
		return
	}

	op, err := r.ReadByte()
	if err != nil {
		s.writeError(conn, err)
		return
	}

	valueData, err := r.ReadBytes()
	if err != nil {
		s.writeError(conn, err)
		return
	}

	offset, err := r.ReadUint64()
	if err != nil {
		s.writeError(conn, err)
		return
	}

	limit, err := r.ReadUint64()
	if err != nil {
		s.writeError(conn, err)
		return
	}

	table, err := s.getTable(tableName)
	if err != nil {
		s.writeError(conn, err)
		return
	}

	value := decodeValue(valueData)
	filterFn := buildFilterFn(fieldName, op, value)

	var matched int
	var ids []uint32
	var records [][]byte

	table.Scan(func(rec Record) bool {
		if filterFn(rec.Data) {
			if matched >= int(offset) && len(ids) < int(limit) {
				ids = append(ids, rec.ID)
				records = append(records, rec.Data)
			}
			matched++
		}
		return true
	})

	resp := protocol.NewWriter()
	resp.WriteUint64(uint64(matched))
	resp.WriteBool(matched > int(offset)+len(ids))
	resp.WriteUint64(uint64(len(records)))
	for _, data := range records {
		resp.WriteBytes(data)
	}
	s.writeSuccess(conn, resp.Bytes())
}

func buildFilterFn(fieldName string, op byte, value interface{}) func([]byte) bool {
	filterOp := protocol.FilterOperator(op)
	return func(data []byte) bool {
		fieldValue := extractFieldValue(data, fieldName)
		if fieldValue == nil {
			return false
		}
		return compareValues(fieldValue, value, filterOp)
	}
}

func extractFieldValue(data []byte, fieldName string) interface{} {
	return decodeValue(data)
}

func compareValues(a, b interface{}, op protocol.FilterOperator) bool {
	switch op {
	case protocol.FilterOpEqual:
		return compareEqual(a, b)
	case protocol.FilterOpNotEqual:
		return !compareEqual(a, b)
	case protocol.FilterOpGreaterThan:
		return compareGreaterThan(a, b)
	case protocol.FilterOpLessThan:
		return compareLessThan(a, b)
	case protocol.FilterOpGreaterOrEq:
		return compareGreaterThan(a, b) || compareEqual(a, b)
	case protocol.FilterOpLessOrEq:
		return compareLessThan(a, b) || compareEqual(a, b)
	case protocol.FilterOpLike:
		return compareLike(a, b)
	default:
		return compareEqual(a, b)
	}
}

func compareEqual(a, b interface{}) bool {
	switch av := a.(type) {
	case int:
		if bv, ok := toInt(b); ok {
			return av == bv
		}
	case int64:
		if bv, ok := toInt64(b); ok {
			return av == bv
		}
	case float64:
		if bv, ok := toFloat64(b); ok {
			return av == bv
		}
	case string:
		if bv, ok := b.(string); ok {
			return av == bv
		}
	case bool:
		if bv, ok := b.(bool); ok {
			return av == bv
		}
	}
	return false
}

func compareGreaterThan(a, b interface{}) bool {
	switch av := a.(type) {
	case int:
		if bv, ok := toInt(b); ok {
			return av > bv
		}
	case int64:
		if bv, ok := toInt64(b); ok {
			return av > bv
		}
	case float64:
		if bv, ok := toFloat64(b); ok {
			return av > bv
		}
	}
	return false
}

func compareLessThan(a, b interface{}) bool {
	switch av := a.(type) {
	case int:
		if bv, ok := toInt(b); ok {
			return av < bv
		}
	case int64:
		if bv, ok := toInt64(b); ok {
			return av < bv
		}
	case float64:
		if bv, ok := toFloat64(b); ok {
			return av < bv
		}
	}
	return false
}

func compareLike(a, b interface{}) bool {
	as, ok := a.(string)
	if !ok {
		return false
	}
	bs, ok := b.(string)
	if !ok {
		return false
	}
	return len(bs) > 0 && len(as) > 0 && containsString(as, bs)
}

func containsString(s, substr string) bool {
	return len(s) >= len(substr) && findSubstring(s, substr)
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func toInt(v interface{}) (int, bool) {
	switch iv := v.(type) {
	case int:
		return iv, true
	case int64:
		return int(iv), true
	case float64:
		return int(iv), true
	}
	return 0, false
}

func toInt64(v interface{}) (int64, bool) {
	switch iv := v.(type) {
	case int:
		return int64(iv), true
	case int64:
		return iv, true
	case float64:
		return int64(iv), true
	}
	return 0, false
}

func toFloat64(v interface{}) (float64, bool) {
	switch iv := v.(type) {
	case int:
		return float64(iv), true
	case int64:
		return float64(iv), true
	case float64:
		return iv, true
	}
	return 0, false
}

func (s *Server) handleScan(r *protocol.Reader, conn net.Conn) {
	tableName, err := r.ReadString()
	if err != nil {
		s.writeError(conn, err)
		return
	}

	table, err := s.getTable(tableName)
	if err != nil {
		s.writeError(conn, err)
		return
	}

	var records []Record
	table.Scan(func(rec Record) bool {
		records = append(records, rec)
		return true
	})

	resp := protocol.NewWriter()
	resp.WriteUint64(uint64(len(records)))
	for _, rec := range records {
		resp.WriteBytes(rec.Data)
	}
	s.writeSuccess(conn, resp.Bytes())
}

func (s *Server) handleCount(r *protocol.Reader, conn net.Conn) {
	tableName, err := r.ReadString()
	if err != nil {
		s.writeError(conn, err)
		return
	}

	table, err := s.getTable(tableName)
	if err != nil {
		s.writeError(conn, err)
		return
	}

	count := table.Count()

	resp := protocol.NewWriter()
	resp.WriteUint64(uint64(count))
	s.writeSuccess(conn, resp.Bytes())
}

func (s *Server) handleVacuum(r *protocol.Reader, conn net.Conn) {
	if err := s.db.Vacuum(); err != nil {
		s.writeError(conn, err)
		return
	}
	s.writeSuccess(conn, nil)
}

func (s *Server) handleCreateTable(r *protocol.Reader, conn net.Conn) {
	tableName, err := r.ReadString()
	if err != nil {
		s.writeError(conn, err)
		return
	}

	_, err = s.getTable(tableName)
	if err != nil {
		s.writeError(conn, err)
		return
	}

	s.writeSuccess(conn, nil)
}

func (s *Server) handleListTables(r *protocol.Reader, conn net.Conn) {
	resp := protocol.NewWriter()
	resp.WriteUint64(0)
	s.writeSuccess(conn, resp.Bytes())
}

func (s *Server) handleCreateIndex(r *protocol.Reader, conn net.Conn) {
	tableName, err := r.ReadString()
	if err != nil {
		s.writeError(conn, err)
		return
	}

	fieldName, err := r.ReadString()
	if err != nil {
		s.writeError(conn, err)
		return
	}

	table, err := s.getTable(tableName)
	if err != nil {
		s.writeError(conn, err)
		return
	}

	if err := table.CreateIndex(fieldName); err != nil {
		s.writeError(conn, err)
		return
	}

	s.writeSuccess(conn, nil)
}

func (s *Server) handleDropIndex(r *protocol.Reader, conn net.Conn) {
	tableName, err := r.ReadString()
	if err != nil {
		s.writeError(conn, err)
		return
	}

	fieldName, err := r.ReadString()
	if err != nil {
		s.writeError(conn, err)
		return
	}

	table, err := s.getTable(tableName)
	if err != nil {
		s.writeError(conn, err)
		return
	}

	if err := table.DropIndex(fieldName); err != nil {
		s.writeError(conn, err)
		return
	}

	s.writeSuccess(conn, nil)
}

func (s *Server) getTable(name string) (*embeddb.Table[Record], error) {
	return embeddb.Use[Record](s.db, name)
}

func decodeValue(data []byte) interface{} {
	if len(data) == 0 {
		return nil
	}

	val, _, err := embedcore.DecodeVarint(data)
	if err == nil {
		return val
	}

	val2, _, err := embedcore.DecodeUvarint(data)
	if err == nil {
		return val2
	}

	str, _, err := embedcore.DecodeString(data)
	if err == nil {
		return str
	}

	b, _, err := embedcore.DecodeBool(data)
	if err == nil {
		return b
	}

	return nil
}

func decodeVarint(data []byte) (int64, error) {
	uv, _, err := decodeUvarint(data)
	if err != nil {
		return 0, err
	}
	return int64(uv>>1) ^ -(int64(uv & 1)), nil
}

func decodeUvarint(data []byte) (uint64, int, error) {
	var v uint64
	var shift uint
	var n int
	for {
		if n >= len(data) {
			return 0, 0, fmt.Errorf("unexpected EOF")
		}
		b := data[n]
		n++
		if b < 0x80 {
			return v | uint64(b)<<shift, n, nil
		}
		v |= uint64(b&0x7F) << shift
		shift += 7
		if shift > 63 {
			return 0, 0, fmt.Errorf("varint overflow")
		}
	}
}
