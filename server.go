package netembeddb

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"sync"

	"github.com/yay101/embeddb"
)

type Server struct {
	mu      sync.RWMutex
	db      *embeddb.DB
	dataDir string
	authKey string
	ln      net.Listener
	wg      sync.WaitGroup
	tables  map[string]*embeddb.Table[RawRecord]
}

type RawRecord struct {
	Data []byte
}

func NewServer(dataDir string, authKey string) *Server {
	return &Server{
		dataDir: dataDir,
		authKey: authKey,
		tables:  make(map[string]*embeddb.Table[RawRecord]),
	}
}

func (s *Server) Listen(addr string) error {
	if err := os.MkdirAll(s.dataDir, 0755); err != nil {
		return fmt.Errorf("failed to create data directory: %w", err)
	}

	dbPath := filepath.Join(s.dataDir, "netembed.db")
	db, err := embeddb.Open(dbPath, embeddb.OpenOptions{Migrate: true, AutoIndex: true})
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	s.db = db

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		if addr[0] == '/' {
			os.Remove(addr)
			ln, err = net.Listen("unix", addr)
			if err != nil {
				return fmt.Errorf("failed to listen on %s: %w", addr, err)
			}
			os.Chmod(addr, 0777)
		} else {
			return fmt.Errorf("failed to listen on %s: %w", addr, err)
		}
	}

	s.ln = ln

	for {
		conn, err := ln.Accept()
		if err != nil {
			if s.ln == nil {
				return nil
			}
			continue
		}

		s.wg.Add(1)
		go s.handleConn(conn)
	}
}

func (s *Server) Close() error {
	if s.ln != nil {
		s.ln.Close()
	}
	s.ln = nil

	s.wg.Wait()

	if s.db != nil {
		if err := s.db.Sync(); err != nil {
			return fmt.Errorf("failed to sync database: %w", err)
		}
		if err := s.db.Close(); err != nil {
			return fmt.Errorf("failed to close database: %w", err)
		}
	}
	return nil
}

func (s *Server) getTable(table string) (*embeddb.Table[RawRecord], error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if tbl, ok := s.tables[table]; ok {
		return tbl, nil
	}

	tbl, err := embeddb.Use[RawRecord](s.db, table)
	if err != nil {
		return nil, err
	}

	s.tables[table] = tbl
	return tbl, nil
}

func (s *Server) handleConn(conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()

	if err := authenticateClient(conn, s.authKey); err != nil {
		writeAuthResponse(conn, fmt.Errorf("auth failed: %w", err))
		return
	}
	writeAuthResponse(conn, nil)

	r := NewReader(conn)

	for {
		op, err := r.ReadByte()
		if err != nil {
			return
		}

		switch op {
		case OpClose:
			return

		case OpRegister:
			s.handleRegister(r, conn)

		case OpCreateTable:
			s.handleCreateTable(r, conn)

		case OpListTables:
			s.handleListTables(r, conn)

		case OpInsert:
			s.handleInsert(r, conn)

		case OpGet:
			s.handleGet(r, conn)

		case OpUpdate:
			s.handleUpdate(r, conn)

		case OpDelete:
			s.handleDelete(r, conn)

		case OpQuery:
			s.handleQuery(r, conn)

		case OpQueryGT:
			s.handleQueryGT(r, conn)

		case OpQueryLT:
			s.handleQueryLT(r, conn)

		case OpQueryBetween:
			s.handleQueryBetween(r, conn)

		case OpFilter:
			s.handleFilter(r, conn)

		case OpScan:
			s.handleScan(r, conn)

		case OpCount:
			s.handleCount(r, conn)

		case OpVacuum:
			s.handleVacuum(r, conn)

		default:
			w := NewWriter(nil)
			WriteError(w, fmt.Errorf("unknown operation: %d", op))
			conn.Write(w.Bytes())
		}
	}
}

func (s *Server) handleRegister(r *Reader, conn net.Conn) {
	table, _ := r.ReadString()

	w := NewWriter(nil)

	_, err := s.getTable(table)
	if err != nil {
		WriteError(w, fmt.Errorf("failed to register table: %w", err))
		conn.Write(w.Bytes())
		return
	}

	WriteResp(w, Response{Success: true, Data: []byte{}})
	conn.Write(w.Bytes())
}

func (s *Server) handleCreateTable(r *Reader, conn net.Conn) {
	table, _ := r.ReadString()

	w := NewWriter(nil)

	_, err := s.getTable(table)
	if err != nil {
		WriteError(w, fmt.Errorf("failed to create table: %w", err))
		conn.Write(w.Bytes())
		return
	}

	WriteResp(w, Response{Success: true, Data: []byte{}})
	conn.Write(w.Bytes())
}

func (s *Server) handleListTables(r *Reader, conn net.Conn) {
	w := NewWriter(nil)
	WriteResp(w, Response{Success: true, Data: []byte{}})
	conn.Write(w.Bytes())
}

func (s *Server) handleInsert(r *Reader, conn net.Conn) {
	table, _ := r.ReadString()
	recordData, _ := r.ReadBytes()

	w := NewWriter(nil)

	tableRef, err := s.getTable(table)
	if err != nil {
		WriteError(w, fmt.Errorf("table not found: %w", err))
		conn.Write(w.Bytes())
		return
	}

	rawRec := &RawRecord{Data: recordData}
	id, err := tableRef.Insert(rawRec)
	if err != nil {
		WriteError(w, fmt.Errorf("failed to insert: %w", err))
		conn.Write(w.Bytes())
		return
	}

	idBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(idBytes, id)

	WriteResp(w, Response{Success: true, Data: idBytes})
	conn.Write(w.Bytes())
}

func (s *Server) handleGet(r *Reader, conn net.Conn) {
	table, _ := r.ReadString()
	id, _ := r.ReadUint64()

	w := NewWriter(nil)

	tableRef, err := s.getTable(table)
	if err != nil {
		WriteError(w, fmt.Errorf("table not found: %w", err))
		conn.Write(w.Bytes())
		return
	}

	record, err := tableRef.Get(uint32(id))
	if err != nil {
		WriteResp(w, Response{Success: true, Data: []byte{}})
	} else {
		WriteResp(w, Response{Success: true, Data: record.Data})
	}
	conn.Write(w.Bytes())
}

func (s *Server) handleUpdate(r *Reader, conn net.Conn) {
	table, _ := r.ReadString()
	id, _ := r.ReadUint64()
	recordData, _ := r.ReadBytes()

	w := NewWriter(nil)

	tableRef, err := s.getTable(table)
	if err != nil {
		WriteError(w, fmt.Errorf("table not found: %w", err))
		conn.Write(w.Bytes())
		return
	}

	rawRec := &RawRecord{Data: recordData}
	err = tableRef.Update(uint32(id), rawRec)
	if err != nil {
		WriteError(w, fmt.Errorf("failed to update: %w", err))
		conn.Write(w.Bytes())
		return
	}

	WriteResp(w, Response{Success: true, Data: []byte{}})
	conn.Write(w.Bytes())
}

func (s *Server) handleDelete(r *Reader, conn net.Conn) {
	table, _ := r.ReadString()
	id, _ := r.ReadUint64()

	w := NewWriter(nil)

	tableRef, err := s.getTable(table)
	if err != nil {
		WriteError(w, fmt.Errorf("table not found: %w", err))
		conn.Write(w.Bytes())
		return
	}

	err = tableRef.Delete(uint32(id))
	if err != nil {
		WriteError(w, fmt.Errorf("failed to delete: %w", err))
		conn.Write(w.Bytes())
		return
	}

	WriteResp(w, Response{Success: true, Data: []byte{}})
	conn.Write(w.Bytes())
}

func (s *Server) handleQuery(r *Reader, conn net.Conn) {
	table, _ := r.ReadString()
	field, _ := r.ReadString()
	valueData, _ := r.ReadBytes()

	w := NewWriter(nil)

	tableRef, err := s.getTable(table)
	if err != nil {
		WriteError(w, fmt.Errorf("table not found: %w", err))
		conn.Write(w.Bytes())
		return
	}

	ids := s.queryRecords(tableRef, field, valueData, func(recVal, queryVal any) bool {
		return reflect.DeepEqual(recVal, queryVal)
	})

	data := new(bytes.Buffer)
	data.Write(embeddb.EncodeUvarint(nil, uint64(len(ids))))
	for _, id := range ids {
		data.Write(embeddb.EncodeUvarint(nil, uint64(id)))
	}
	WriteResp(w, Response{Success: true, Data: data.Bytes()})
	conn.Write(w.Bytes())
}

func (s *Server) handleQueryGT(r *Reader, conn net.Conn) {
	table, _ := r.ReadString()
	field, _ := r.ReadString()
	valueData, _ := r.ReadBytes()
	inclusive, _ := r.ReadBool()

	w := NewWriter(nil)

	tableRef, err := s.getTable(table)
	if err != nil {
		WriteError(w, fmt.Errorf("table not found: %w", err))
		conn.Write(w.Bytes())
		return
	}

	ids := s.queryRecords(tableRef, field, valueData, func(recVal, queryVal any) bool {
		if inclusive {
			return compareGE(recVal, queryVal)
		}
		return compareGT(recVal, queryVal)
	})

	data := new(bytes.Buffer)
	data.Write(embeddb.EncodeUvarint(nil, uint64(len(ids))))
	for _, id := range ids {
		data.Write(embeddb.EncodeUvarint(nil, uint64(id)))
	}
	WriteResp(w, Response{Success: true, Data: data.Bytes()})
	conn.Write(w.Bytes())
}

func (s *Server) handleQueryLT(r *Reader, conn net.Conn) {
	table, _ := r.ReadString()
	field, _ := r.ReadString()
	valueData, _ := r.ReadBytes()
	inclusive, _ := r.ReadBool()

	w := NewWriter(nil)

	tableRef, err := s.getTable(table)
	if err != nil {
		WriteError(w, fmt.Errorf("table not found: %w", err))
		conn.Write(w.Bytes())
		return
	}

	ids := s.queryRecords(tableRef, field, valueData, func(recVal, queryVal any) bool {
		if inclusive {
			return compareLE(recVal, queryVal)
		}
		return compareLT(recVal, queryVal)
	})

	data := new(bytes.Buffer)
	data.Write(embeddb.EncodeUvarint(nil, uint64(len(ids))))
	for _, id := range ids {
		data.Write(embeddb.EncodeUvarint(nil, uint64(id)))
	}
	WriteResp(w, Response{Success: true, Data: data.Bytes()})
	conn.Write(w.Bytes())
}

func (s *Server) handleQueryBetween(r *Reader, conn net.Conn) {
	table, _ := r.ReadString()
	field, _ := r.ReadString()
	minData, _ := r.ReadBytes()
	maxData, _ := r.ReadBytes()
	inclusiveMin, _ := r.ReadBool()
	inclusiveMax, _ := r.ReadBool()

	w := NewWriter(nil)

	tableRef, err := s.getTable(table)
	if err != nil {
		WriteError(w, fmt.Errorf("table not found: %w", err))
		conn.Write(w.Bytes())
		return
	}

	ids := s.queryRecordsBetween(tableRef, field, minData, maxData, inclusiveMin, inclusiveMax)

	data := new(bytes.Buffer)
	data.Write(embeddb.EncodeUvarint(nil, uint64(len(ids))))
	for _, id := range ids {
		data.Write(embeddb.EncodeUvarint(nil, uint64(id)))
	}
	WriteResp(w, Response{Success: true, Data: data.Bytes()})
	conn.Write(w.Bytes())
}

func (s *Server) handleFilter(r *Reader, conn net.Conn) {
	table, _ := r.ReadString()

	w := NewWriter(nil)

	tableRef, err := s.getTable(table)
	if err != nil {
		WriteError(w, fmt.Errorf("table not found: %w", err))
		conn.Write(w.Bytes())
		return
	}

	results, err := tableRef.Filter(func(r RawRecord) bool { return true })
	if err != nil {
		WriteError(w, fmt.Errorf("filter failed: %w", err))
		conn.Write(w.Bytes())
		return
	}

	w.WriteUint64(uint64(len(results)))
	for _, rec := range results {
		w.WriteBytes(rec.Data)
	}

	WriteResp(w, Response{Success: true, Data: w.Bytes()})
	conn.Write(w.Bytes())
}

func (s *Server) handleScan(r *Reader, conn net.Conn) {
	table, _ := r.ReadString()

	w := NewWriter(nil)

	tableRef, err := s.getTable(table)
	if err != nil {
		WriteError(w, fmt.Errorf("table not found: %w", err))
		conn.Write(w.Bytes())
		return
	}

	count := tableRef.Count()
	data := make([]byte, 4)
	binary.BigEndian.PutUint32(data, uint32(count))
	WriteResp(w, Response{Success: true, Data: data})
	conn.Write(w.Bytes())
}

func (s *Server) handleCount(r *Reader, conn net.Conn) {
	table, err := r.ReadString()
	if err != nil {
		w := NewWriter(nil)
		WriteError(w, fmt.Errorf("failed to read table name: %w", err))
		conn.Write(w.Bytes())
		return
	}

	w := NewWriter(nil)

	tableRef, err := s.getTable(table)
	if err != nil {
		WriteError(w, fmt.Errorf("table not found: %w", err))
		conn.Write(w.Bytes())
		return
	}

	count := uint32(tableRef.Count())
	data := make([]byte, 4)
	binary.BigEndian.PutUint32(data, count)
	WriteResp(w, Response{Success: true, Data: data})
	conn.Write(w.Bytes())
}

func (s *Server) handleVacuum(r *Reader, conn net.Conn) {
	w := NewWriter(nil)

	if s.db == nil {
		WriteError(w, fmt.Errorf("database not initialized"))
		conn.Write(w.Bytes())
		return
	}

	if err := s.db.Vacuum(); err != nil {
		WriteError(w, fmt.Errorf("vacuum failed: %w", err))
		conn.Write(w.Bytes())
		return
	}

	WriteResp(w, Response{Success: true, Data: []byte{}})
	conn.Write(w.Bytes())
}

type simpleReader struct {
	data []byte
	pos  int
}

func (r *simpleReader) Read(p []byte) (n int, err error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n = copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}

func (s *Server) TablePath(name string) string {
	return filepath.Join(s.dataDir, name+".db")
}

func (s *Server) CreateTable(name string) error {
	_, err := s.getTable(name)
	return err
}

func (s *Server) GetTable(name string) (any, bool) {
	t, err := s.getTable(name)
	return t, err == nil
}

func (s *Server) ListTables() []string {
	return []string{}
}

func (s *Server) queryRecords(table *embeddb.Table[RawRecord], field string, valueData []byte, match func(any, any) bool) []uint32 {
	var ids []uint32

	count := uint32(table.Count())
	for id := uint32(1); id <= count; id++ {
		rec, err := table.Get(id)
		if err != nil || rec == nil {
			continue
		}

		var data map[string]any
		if err := json.Unmarshal(rec.Data, &data); err != nil {
			continue
		}

		recVal, ok := data[field]
		if !ok {
			continue
		}

		queryVal, err := decodeValue(valueData)
		if err != nil {
			continue
		}

		if match(recVal, queryVal) {
			ids = append(ids, id)
		}
	}

	return ids
}

func (s *Server) queryRecordsBetween(table *embeddb.Table[RawRecord], field string, minData, maxData []byte, inclusiveMin, inclusiveMax bool) []uint32 {
	var ids []uint32

	minVal, err := decodeValue(minData)
	if err != nil {
		return ids
	}
	maxVal, err := decodeValue(maxData)
	if err != nil {
		return ids
	}

	minCompare := compareGT
	if inclusiveMin {
		minCompare = compareGE
	}
	maxCompare := compareLT
	if inclusiveMax {
		maxCompare = compareLE
	}

	count := uint32(table.Count())
	for id := uint32(1); id <= count; id++ {
		rec, err := table.Get(id)
		if err != nil || rec == nil {
			continue
		}

		var data map[string]any
		if err := json.Unmarshal(rec.Data, &data); err != nil {
			continue
		}

		recVal, ok := data[field]
		if !ok {
			continue
		}

		if minCompare(recVal, minVal) && maxCompare(recVal, maxVal) {
			ids = append(ids, id)
		}
	}

	return ids
}

func decodeValue(data []byte) (any, error) {
	if len(data) == 0 {
		return nil, nil
	}

	firstByte := data[0]

	switch {
	case firstByte == 0x00:
		return false, nil
	case firstByte == 0x01:
		return true, nil
	case len(data) >= 2 && firstByte&0x80 != 0:
		val, _, err := embeddb.DecodeVarint(data)
		return val, err
	case len(data) == 1 && firstByte%2 == 0:
		val, _, err := embeddb.DecodeVarint(data)
		return val, err
	default:
		length, n := binary.Uvarint(data)
		if n > 0 && len(data) >= int(n)+int(length) {
			str, _, err := embeddb.DecodeString(data)
			return str, err
		}
		val, _, err := embeddb.DecodeUvarint(data)
		return val, err
	}
}

func compareGT(a, b any) bool {
	aNum, aOk := toFloat64(a)
	bNum, bOk := toFloat64(b)
	if aOk && bOk {
		return aNum > bNum
	}
	aInt, aOk := toInt64(a)
	bInt, bOk := toInt64(b)
	if aOk && bOk {
		return aInt > bInt
	}
	aStr, aOk := a.(string)
	bStr, bOk := b.(string)
	if aOk && bOk {
		return aStr > bStr
	}
	return false
}

func compareGE(a, b any) bool {
	aNum, aOk := toFloat64(a)
	bNum, bOk := toFloat64(b)
	if aOk && bOk {
		return aNum >= bNum
	}
	aInt, aOk := toInt64(a)
	bInt, bOk := toInt64(b)
	if aOk && bOk {
		return aInt >= bInt
	}
	aStr, aOk := a.(string)
	bStr, bOk := b.(string)
	if aOk && bOk {
		return aStr >= bStr
	}
	return false
}

func compareLT(a, b any) bool {
	aNum, aOk := toFloat64(a)
	bNum, bOk := toFloat64(b)
	if aOk && bOk {
		return aNum < bNum
	}
	aInt, aOk := toInt64(a)
	bInt, bOk := toInt64(b)
	if aOk && bOk {
		return aInt < bInt
	}
	aStr, aOk := a.(string)
	bStr, bOk := b.(string)
	if aOk && bOk {
		return aStr < bStr
	}
	return false
}

func compareLE(a, b any) bool {
	aNum, aOk := toFloat64(a)
	bNum, bOk := toFloat64(b)
	if aOk && bOk {
		return aNum <= bNum
	}
	aInt, aOk := toInt64(a)
	bInt, bOk := toInt64(b)
	if aOk && bOk {
		return aInt <= bInt
	}
	aStr, aOk := a.(string)
	bStr, bOk := b.(string)
	if aOk && bOk {
		return aStr <= bStr
	}
	return false
}

func toFloat64(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case int32:
		return float64(n), true
	case uint:
		return float64(n), true
	case uint64:
		return float64(n), true
	case uint32:
		return float64(n), true
	default:
		return 0, false
	}
}

func toInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case int64:
		return n, true
	case int:
		return int64(n), true
	case int32:
		return int64(n), true
	case int16:
		return int64(n), true
	case int8:
		return int64(n), true
	case uint64:
		return int64(n), true
	case uint:
		return int64(n), true
	case uint32:
		return int64(n), true
	case uint16:
		return int64(n), true
	case uint8:
		return int64(n), true
	case float64:
		return int64(n), true
	case float32:
		return int64(n), true
	default:
		return 0, false
	}
}
