package netembeddb

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
)

type Server struct {
	mu      sync.RWMutex
	tables  map[string]*tableData
	dataDir string
	authKey string
	ln      net.Listener
	wg      sync.WaitGroup
}

type tableData struct {
	name    string
	records map[uint32][]byte
	nextID  uint32
}

func NewServer(dataDir string, authKey string) *Server {
	return &Server{
		tables:  make(map[string]*tableData),
		dataDir: dataDir,
		authKey: authKey,
	}
}

func (s *Server) Listen(addr string) error {
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
	return nil
}

func (s *Server) handleConn(conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()

	// Authenticate
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

		case OpCount:
			s.handleCount(r, conn)

		default:
			w := NewWriter(nil)
			WriteError(w, fmt.Errorf("unknown operation: %d", op))
			w.WriteTo(conn)
		}
	}
}

func (s *Server) handleRegister(r *Reader, conn net.Conn) {
	table, _ := r.ReadString()
	schemaData, _ := r.ReadBytes()

	_ = schemaData // Schema registration placeholder

	w := NewWriter(nil)
	s.mu.Lock()
	s.tables[table] = &tableData{name: table, records: make(map[uint32][]byte), nextID: 1}
	s.mu.Unlock()

	WriteResponse(w, Response{Success: true, Data: []byte{}})
	w.WriteTo(conn)
}

func (s *Server) handleCreateTable(r *Reader, conn net.Conn) {
	table, _ := r.ReadString()
	schemaName, _ := r.ReadString()

	_ = schemaName // Schema registration placeholder

	w := NewWriter(nil)
	s.mu.Lock()
	s.tables[table] = &tableData{name: table, records: make(map[uint32][]byte), nextID: 1}
	s.mu.Unlock()

	WriteResponse(w, Response{Success: true, Data: []byte{}})
	w.WriteTo(conn)
}

func (s *Server) handleListTables(r *Reader, conn net.Conn) {
	s.mu.RLock()
	tables := make([]string, 0, len(s.tables))
	for name := range s.tables {
		tables = append(tables, name)
	}
	s.mu.RUnlock()

	w := NewWriter(nil)
	w.WriteUint64(uint64(len(tables)))
	for _, t := range tables {
		w.WriteString(t)
	}

	WriteResponse(w, Response{Success: true, Data: w.Bytes()})
	w.WriteTo(conn)
}

func (s *Server) handleInsert(r *Reader, conn net.Conn) {
	table, _ := r.ReadString()
	recordData, _ := r.ReadBytes()

	s.mu.Lock()
	tbl, exists := s.tables[table]
	if !exists {
		s.mu.Unlock()
		w := NewWriter(nil)
		WriteError(w, fmt.Errorf("table %s not found", table))
		w.WriteTo(conn)
		return
	}

	id := tbl.nextID
	tbl.records[id] = recordData
	tbl.nextID++

	idBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(idBytes, id)
	s.mu.Unlock()

	w := NewWriter(nil)
	WriteResponse(w, Response{Success: true, Data: idBytes})
	w.WriteTo(conn)
}

func (s *Server) handleGet(r *Reader, conn net.Conn) {
	table, _ := r.ReadString()
	id, _ := r.ReadUint64()

	s.mu.RLock()
	tbl, exists := s.tables[table]
	s.mu.RUnlock()

	w := NewWriter(nil)
	if !exists {
		WriteError(w, fmt.Errorf("table %s not found", table))
		w.WriteTo(conn)
		return
	}

	s.mu.RLock()
	record, ok := tbl.records[uint32(id)]
	s.mu.RUnlock()

	if !ok {
		WriteResponse(w, Response{Success: true, Data: []byte{}})
	} else {
		WriteResponse(w, Response{Success: true, Data: record})
	}
	w.WriteTo(conn)
}

func (s *Server) handleUpdate(r *Reader, conn net.Conn) {
	table, _ := r.ReadString()
	id, _ := r.ReadUint64()
	recordData, _ := r.ReadBytes()

	s.mu.Lock()
	tbl, exists := s.tables[table]
	if exists {
		tbl.records[uint32(id)] = recordData
	}
	s.mu.Unlock()

	w := NewWriter(nil)
	if !exists {
		WriteError(w, fmt.Errorf("table %s not found", table))
	} else {
		WriteResponse(w, Response{Success: true, Data: []byte{}})
	}
	w.WriteTo(conn)
}

func (s *Server) handleDelete(r *Reader, conn net.Conn) {
	table, _ := r.ReadString()
	id, _ := r.ReadUint64()

	s.mu.Lock()
	tbl, exists := s.tables[table]
	if exists {
		delete(tbl.records, uint32(id))
	}
	s.mu.Unlock()

	w := NewWriter(nil)
	if !exists {
		WriteError(w, fmt.Errorf("table %s not found", table))
	} else {
		WriteResponse(w, Response{Success: true, Data: []byte{}})
	}
	w.WriteTo(conn)
}

func (s *Server) handleQuery(r *Reader, conn net.Conn) {
	// Simplified query - returns IDs where value matches (placeholder)
	w := NewWriter(nil)
	w.WriteUint64(0) // No results
	WriteResponse(w, Response{Success: true, Data: w.Bytes()})
	w.WriteTo(conn)
}

func (s *Server) handleQueryGT(r *Reader, conn net.Conn) {
	w := NewWriter(nil)
	w.WriteUint64(0)
	WriteResponse(w, Response{Success: true, Data: w.Bytes()})
	w.WriteTo(conn)
}

func (s *Server) handleQueryLT(r *Reader, conn net.Conn) {
	w := NewWriter(nil)
	w.WriteUint64(0)
	WriteResponse(w, Response{Success: true, Data: w.Bytes()})
	w.WriteTo(conn)
}

func (s *Server) handleQueryBetween(r *Reader, conn net.Conn) {
	w := NewWriter(nil)
	w.WriteUint64(0)
	WriteResponse(w, Response{Success: true, Data: w.Bytes()})
	w.WriteTo(conn)
}

func (s *Server) handleCount(r *Reader, conn net.Conn) {
	table, _ := r.ReadString()

	s.mu.RLock()
	tbl, exists := s.tables[table]
	var count uint32
	if exists {
		count = uint32(len(tbl.records))
	}
	s.mu.RUnlock()

	w := NewWriter(nil)
	if !exists {
		WriteError(w, fmt.Errorf("table %s not found", table))
	} else {
		data := make([]byte, 4)
		binary.BigEndian.PutUint32(data, count)
		WriteResponse(w, Response{Success: true, Data: data})
	}
	w.WriteTo(conn)
}

// TablePath returns the file path for a table
func (s *Server) TablePath(name string) string {
	return filepath.Join(s.dataDir, name+".db")
}

// CreateTable initializes a new table
func (s *Server) CreateTable(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.tables[name]; exists {
		return nil
	}

	s.tables[name] = &tableData{name: name, records: make(map[uint32][]byte), nextID: 1}
	return nil
}

// GetTable returns table data
func (s *Server) GetTable(name string) (*tableData, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.tables[name]
	return t, ok
}

// ListTables returns all table names
func (s *Server) ListTables() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	tables := make([]string, 0, len(s.tables))
	for name := range s.tables {
		tables = append(tables, name)
	}
	return tables
}
