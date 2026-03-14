package netembeddb

import (
	"encoding/binary"
	"fmt"
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
	schemas map[string]string // table name -> type string
}

func NewServer(dataDir string, authKey string) *Server {
	return &Server{
		dataDir: dataDir,
		authKey: authKey,
		schemas: make(map[string]string),
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
	typeName, _ := r.ReadString()

	s.mu.Lock()
	s.schemas[table] = typeName
	s.mu.Unlock()

	w := NewWriter(nil)
	WriteResp(w, Response{Success: true, Data: []byte{}})
	conn.Write(w.Bytes())
}

func (s *Server) handleCreateTable(r *Reader, conn net.Conn) {
	table, _ := r.ReadString()
	typeName, _ := r.ReadString()

	s.mu.Lock()
	s.schemas[table] = typeName
	s.mu.Unlock()

	w := NewWriter(nil)
	WriteResp(w, Response{Success: true, Data: []byte{}})
	conn.Write(w.Bytes())
}

func (s *Server) handleListTables(r *Reader, conn net.Conn) {
	s.mu.RLock()
	tables := make([]string, 0, len(s.schemas))
	for name := range s.schemas {
		tables = append(tables, name)
	}
	s.mu.RUnlock()

	w := NewWriter(nil)
	w.WriteUint64(uint64(len(tables)))
	for _, t := range tables {
		w.WriteString(t)
	}

	WriteResp(w, Response{Success: true, Data: w.Bytes()})
	conn.Write(w.Bytes())
}

func (s *Server) handleInsert(r *Reader, conn net.Conn) {
	table, _ := r.ReadString()
	recordData, _ := r.ReadBytes()

	w := NewWriter(nil)

	typeName := s.getSchema(table)
	if typeName == "" {
		WriteError(w, fmt.Errorf("table %s not found", table))
		conn.Write(w.Bytes())
		return
	}

	record, err := decodeRecord(typeName, recordData)
	if err != nil {
		WriteError(w, fmt.Errorf("failed to decode record: %w", err))
		conn.Write(w.Bytes())
		return
	}

	id, err := s.insertRecord(table, record)
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

	typeName := s.getSchema(table)
	if typeName == "" {
		WriteError(w, fmt.Errorf("table %s not found", table))
		conn.Write(w.Bytes())
		return
	}

	record, err := s.getRecord(table, typeName, uint32(id))
	if err != nil {
		WriteResp(w, Response{Success: true, Data: []byte{}})
	} else {
		recordData, _ := encodeRecordByType(typeName, record)
		WriteResp(w, Response{Success: true, Data: recordData})
	}
	conn.Write(w.Bytes())
}

func (s *Server) handleUpdate(r *Reader, conn net.Conn) {
	table, _ := r.ReadString()
	id, _ := r.ReadUint64()
	recordData, _ := r.ReadBytes()

	w := NewWriter(nil)

	typeName := s.getSchema(table)
	if typeName == "" {
		WriteError(w, fmt.Errorf("table %s not found", table))
		conn.Write(w.Bytes())
		return
	}

	record, err := decodeRecord(typeName, recordData)
	if err != nil {
		WriteError(w, fmt.Errorf("failed to decode record: %w", err))
		conn.Write(w.Bytes())
		return
	}

	err = s.updateRecord(table, typeName, uint32(id), record)
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

	typeName := s.getSchema(table)
	if typeName == "" {
		WriteError(w, fmt.Errorf("table %s not found", table))
		conn.Write(w.Bytes())
		return
	}

	err := s.deleteRecord(table, typeName, uint32(id))
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

	typeName := s.getSchema(table)
	if typeName == "" {
		WriteError(w, fmt.Errorf("table %s not found", table))
		conn.Write(w.Bytes())
		return
	}

	value, err := decodeValue(valueData)
	if err != nil {
		WriteError(w, fmt.Errorf("failed to decode value: %w", err))
		conn.Write(w.Bytes())
		return
	}

	results, err := s.queryRecords(table, typeName, field, value)
	if err != nil {
		WriteError(w, fmt.Errorf("query failed: %w", err))
		conn.Write(w.Bytes())
		return
	}

	w.WriteUint64(uint64(len(results)))
	for _, rec := range results {
		recID := getRecordID(rec)
		w.WriteUint64(uint64(recID))
	}

	WriteResp(w, Response{Success: true, Data: w.Bytes()})
	conn.Write(w.Bytes())
}

func (s *Server) handleQueryGT(r *Reader, conn net.Conn) {
	table, _ := r.ReadString()
	field, _ := r.ReadString()
	valueData, _ := r.ReadBytes()
	inclusive, _ := r.ReadBool()

	w := NewWriter(nil)

	typeName := s.getSchema(table)
	if typeName == "" {
		WriteError(w, fmt.Errorf("table %s not found", table))
		conn.Write(w.Bytes())
		return
	}

	value, err := decodeValue(valueData)
	if err != nil {
		WriteError(w, fmt.Errorf("failed to decode value: %w", err))
		conn.Write(w.Bytes())
		return
	}

	results, err := s.queryRecordsGT(table, typeName, field, value, inclusive)
	if err != nil {
		WriteError(w, fmt.Errorf("query failed: %w", err))
		conn.Write(w.Bytes())
		return
	}

	w.WriteUint64(uint64(len(results)))
	for _, rec := range results {
		recID := getRecordID(rec)
		w.WriteUint64(uint64(recID))
	}

	WriteResp(w, Response{Success: true, Data: w.Bytes()})
	conn.Write(w.Bytes())
}

func (s *Server) handleQueryLT(r *Reader, conn net.Conn) {
	table, _ := r.ReadString()
	field, _ := r.ReadString()
	valueData, _ := r.ReadBytes()
	inclusive, _ := r.ReadBool()

	w := NewWriter(nil)

	typeName := s.getSchema(table)
	if typeName == "" {
		WriteError(w, fmt.Errorf("table %s not found", table))
		conn.Write(w.Bytes())
		return
	}

	value, err := decodeValue(valueData)
	if err != nil {
		WriteError(w, fmt.Errorf("failed to decode value: %w", err))
		conn.Write(w.Bytes())
		return
	}

	results, err := s.queryRecordsLT(table, typeName, field, value, inclusive)
	if err != nil {
		WriteError(w, fmt.Errorf("query failed: %w", err))
		conn.Write(w.Bytes())
		return
	}

	w.WriteUint64(uint64(len(results)))
	for _, rec := range results {
		recID := getRecordID(rec)
		w.WriteUint64(uint64(recID))
	}

	WriteResp(w, Response{Success: true, Data: w.Bytes()})
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

	typeName := s.getSchema(table)
	if typeName == "" {
		WriteError(w, fmt.Errorf("table %s not found", table))
		conn.Write(w.Bytes())
		return
	}

	min, err := decodeValue(minData)
	if err != nil {
		WriteError(w, fmt.Errorf("failed to decode min value: %w", err))
		conn.Write(w.Bytes())
		return
	}

	max, err := decodeValue(maxData)
	if err != nil {
		WriteError(w, fmt.Errorf("failed to decode max value: %w", err))
		conn.Write(w.Bytes())
		return
	}

	results, err := s.queryRecordsBetween(table, typeName, field, min, max, inclusiveMin, inclusiveMax)
	if err != nil {
		WriteError(w, fmt.Errorf("query failed: %w", err))
		conn.Write(w.Bytes())
		return
	}

	w.WriteUint64(uint64(len(results)))
	for _, rec := range results {
		recID := getRecordID(rec)
		w.WriteUint64(uint64(recID))
	}

	WriteResp(w, Response{Success: true, Data: w.Bytes()})
	conn.Write(w.Bytes())
}

func (s *Server) handleFilter(r *Reader, conn net.Conn) {
	table, _ := r.ReadString()

	w := NewWriter(nil)

	typeName := s.getSchema(table)
	if typeName == "" {
		WriteError(w, fmt.Errorf("table %s not found", table))
		conn.Write(w.Bytes())
		return
	}

	results, err := s.filterRecords(table, typeName)
	if err != nil {
		WriteError(w, fmt.Errorf("filter failed: %w", err))
		conn.Write(w.Bytes())
		return
	}

	w.WriteUint64(uint64(len(results)))
	for _, rec := range results {
		recID := getRecordID(rec)
		w.WriteUint64(uint64(recID))
	}

	WriteResp(w, Response{Success: true, Data: w.Bytes()})
	conn.Write(w.Bytes())
}

func (s *Server) handleScan(r *Reader, conn net.Conn) {
	table, _ := r.ReadString()

	w := NewWriter(nil)

	typeName := s.getSchema(table)
	if typeName == "" {
		WriteError(w, fmt.Errorf("table %s not found", table))
		conn.Write(w.Bytes())
		return
	}

	count, err := s.scanRecords(table, typeName)
	if err != nil {
		WriteError(w, fmt.Errorf("scan failed: %w", err))
		conn.Write(w.Bytes())
		return
	}

	w.WriteUint64(uint64(count))
	WriteResp(w, Response{Success: true, Data: w.Bytes()})
	conn.Write(w.Bytes())
}

func (s *Server) handleCount(r *Reader, conn net.Conn) {
	table, _ := r.ReadString()

	w := NewWriter(nil)

	typeName := s.getSchema(table)
	if typeName == "" {
		WriteError(w, fmt.Errorf("table %s not found", table))
		conn.Write(w.Bytes())
		return
	}

	count, err := s.countRecords(table, typeName)
	if err != nil {
		WriteError(w, fmt.Errorf("count failed: %w", err))
		conn.Write(w.Bytes())
		return
	}

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

func (s *Server) getSchema(table string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.schemas[table]
}

func (s *Server) getTable(table string, typeName string) (interface{}, error) {
	switch typeName {
	case "main.User":
		return embeddb.Use[User](s.db, table)
	default:
		return nil, fmt.Errorf("unknown type: %s", typeName)
	}
}

func (s *Server) insertRecord(table string, record interface{}) (uint32, error) {
	typeName := s.getSchema(table)
	switch typeName {
	case "main.User":
		tbl := record.(*User)
		tableRef, _ := embeddb.Use[User](s.db, table)
		return tableRef.Insert(tbl)
	default:
		return 0, fmt.Errorf("unknown type: %s", typeName)
	}
}

func (s *Server) getRecord(table string, typeName string, id uint32) (interface{}, error) {
	switch typeName {
	case "main.User":
		tableRef, _ := embeddb.Use[User](s.db, table)
		return tableRef.Get(id)
	default:
		return nil, fmt.Errorf("unknown type: %s", typeName)
	}
}

func (s *Server) updateRecord(table string, typeName string, id uint32, record interface{}) error {
	switch typeName {
	case "main.User":
		tableRef, _ := embeddb.Use[User](s.db, table)
		rec := record.(*User)
		return tableRef.Update(id, rec)
	default:
		return fmt.Errorf("unknown type: %s", typeName)
	}
}

func (s *Server) deleteRecord(table string, typeName string, id uint32) error {
	switch typeName {
	case "main.User":
		tableRef, _ := embeddb.Use[User](s.db, table)
		return tableRef.Delete(id)
	default:
		return fmt.Errorf("unknown type: %s", typeName)
	}
}

func (s *Server) queryRecords(table string, typeName string, field string, value interface{}) ([]interface{}, error) {
	switch typeName {
	case "main.User":
		tableRef, _ := embeddb.Use[User](s.db, table)
		return s.queryUserTable(tableRef, field, value)
	default:
		return nil, fmt.Errorf("unknown type: %s", typeName)
	}
}

func (s *Server) queryUserTable(table *embeddb.Table[User], field string, value interface{}) ([]interface{}, error) {
	results, err := table.Query(field, value)
	if err != nil {
		return nil, err
	}
	ret := make([]interface{}, len(results))
	for i := range results {
		r := results[i]
		ret[i] = &r
	}
	return ret, nil
}

func (s *Server) queryRecordsGT(table string, typeName string, field string, value interface{}, inclusive bool) ([]interface{}, error) {
	switch typeName {
	case "main.User":
		tableRef, _ := embeddb.Use[User](s.db, table)
		results, err := tableRef.QueryRangeGreaterThan(field, value, inclusive)
		if err != nil {
			return nil, err
		}
		ret := make([]interface{}, len(results))
		for i := range results {
			r := results[i]
			ret[i] = &r
		}
		return ret, nil
	default:
		return nil, fmt.Errorf("unknown type: %s", typeName)
	}
}

func (s *Server) queryRecordsLT(table string, typeName string, field string, value interface{}, inclusive bool) ([]interface{}, error) {
	switch typeName {
	case "main.User":
		tableRef, _ := embeddb.Use[User](s.db, table)
		results, err := tableRef.QueryRangeLessThan(field, value, inclusive)
		if err != nil {
			return nil, err
		}
		ret := make([]interface{}, len(results))
		for i := range results {
			r := results[i]
			ret[i] = &r
		}
		return ret, nil
	default:
		return nil, fmt.Errorf("unknown type: %s", typeName)
	}
}

func (s *Server) queryRecordsBetween(table string, typeName string, field string, min, max interface{}, inclusiveMin, inclusiveMax bool) ([]interface{}, error) {
	switch typeName {
	case "main.User":
		tableRef, _ := embeddb.Use[User](s.db, table)
		results, err := tableRef.QueryRangeBetween(field, min, max, inclusiveMin, inclusiveMax)
		if err != nil {
			return nil, err
		}
		ret := make([]interface{}, len(results))
		for i := range results {
			r := results[i]
			ret[i] = &r
		}
		return ret, nil
	default:
		return nil, fmt.Errorf("unknown type: %s", typeName)
	}
}

func (s *Server) filterRecords(table string, typeName string) ([]interface{}, error) {
	switch typeName {
	case "main.User":
		tableRef, _ := embeddb.Use[User](s.db, table)
		results, err := tableRef.Filter(func(u User) bool { return true })
		if err != nil {
			return nil, err
		}
		ret := make([]interface{}, len(results))
		for i := range results {
			r := results[i]
			ret[i] = &r
		}
		return ret, nil
	default:
		return nil, fmt.Errorf("unknown type: %s", typeName)
	}
}

func (s *Server) scanRecords(table string, typeName string) (int, error) {
	switch typeName {
	case "main.User":
		tableRef, _ := embeddb.Use[User](s.db, table)
		return tableRef.Count(), nil
	default:
		return 0, fmt.Errorf("unknown type: %s", typeName)
	}
}

func (s *Server) countRecords(table string, typeName string) (uint32, error) {
	switch typeName {
	case "main.User":
		tableRef, _ := embeddb.Use[User](s.db, table)
		return uint32(tableRef.Count()), nil
	default:
		return 0, fmt.Errorf("unknown type: %s", typeName)
	}
}

type User struct {
	ID    uint32 `db:"id,primary"`
	Name  string
	Age   int
	Email string
}

func getRecordID(record interface{}) uint32 {
	v := reflect.ValueOf(record)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	f := v.FieldByName("ID")
	if f.IsValid() {
		return uint32(f.Uint())
	}
	return 0
}

func decodeRecord(typeName string, data []byte) (interface{}, error) {
	switch typeName {
	case "main.User":
		return decodeUser(data)
	default:
		return nil, fmt.Errorf("unknown type: %s", typeName)
	}
}

func decodeUser(data []byte) (*User, error) {
	r := NewReader(newBytesReader(data))
	user := &User{}

	for {
		name, err := r.ReadString()
		if err != nil {
			break
		}
		_, err = r.ReadByte()
		if err != nil {
			break
		}

		switch name {
		case "ID":
			id, _ := r.ReadUint64()
			user.ID = uint32(id)
		case "Name":
			user.Name, _ = r.ReadString()
		case "Age":
			age, _ := r.ReadInt64()
			user.Age = int(age)
		case "Email":
			user.Email, _ = r.ReadString()
		}

		r.ReadByte()
	}

	return user, nil
}

func encodeRecordByType(typeName string, record interface{}) ([]byte, error) {
	switch typeName {
	case "main.User":
		return encodeUser(record.(*User)), nil
	default:
		return nil, fmt.Errorf("unknown type: %s", typeName)
	}
}

func encodeUser(user *User) []byte {
	var buffer []byte

	buffer = append(buffer, 0, embeddb.ValueStartMarker)
	buffer = embeddb.EncodeUvarint(buffer, uint64(user.ID))
	buffer = append(buffer, embeddb.ValueEndMarker)

	buffer = append(buffer, 1, embeddb.ValueStartMarker)
	buffer = embeddb.EncodeString(buffer, user.Name)
	buffer = append(buffer, embeddb.ValueEndMarker)

	buffer = append(buffer, 2, embeddb.ValueStartMarker)
	buffer = embeddb.EncodeVarint(buffer, int64(user.Age))
	buffer = append(buffer, embeddb.ValueEndMarker)

	buffer = append(buffer, 3, embeddb.ValueStartMarker)
	buffer = embeddb.EncodeString(buffer, user.Email)
	buffer = append(buffer, embeddb.ValueEndMarker)

	return buffer
}

type bytesReader struct {
	data []byte
	pos  int
}

func newBytesReader(data []byte) *bytesReader {
	return &bytesReader{data: data, pos: 0}
}

func (r *bytesReader) Read(p []byte) (n int, err error) {
	if r.pos >= len(r.data) {
		return 0, nil
	}
	n = copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}

func decodeValue(data []byte) (interface{}, error) {
	if len(data) == 0 {
		return nil, nil
	}

	r := NewReader(newBytesReader(data))
	marker, err := r.ReadByte()
	if err != nil {
		return nil, err
	}

	switch marker {
	case embeddb.ValueStartMarker:
		// Re-read after marker
		r.pos = 0
		r.ReadByte() // skip marker again
		val, _ := r.ReadString()
		return val, nil
	default:
		r.pos = 0
		val, _ := r.ReadString()
		return val, nil
	}
}

func (s *Server) TablePath(name string) string {
	return filepath.Join(s.dataDir, name+".db")
}

func (s *Server) CreateTable(name string) error {
	return nil
}

func (s *Server) GetTable(name string) (any, bool) {
	typeName := s.getSchema(name)
	if typeName == "" {
		return nil, false
	}
	t, err := s.getTable(name, typeName)
	return t, err == nil
}

func (s *Server) ListTables() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	tables := make([]string, 0, len(s.schemas))
	for name := range s.schemas {
		tables = append(tables, name)
	}
	return tables
}
