package netembeddb

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"path/filepath"
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
}

type Record struct {
	Data []byte
}

func NewServer(dataDir string, authKey string) *Server {
	return &Server{
		dataDir: dataDir,
		authKey: authKey,
	}
}

func (s *Server) Listen(addr string) error {
	if err := os.MkdirAll(s.dataDir, 0755); err != nil {
		return fmt.Errorf("failed to create data directory: %w", err)
	}

	dbPath := filepath.Join(s.dataDir, "netembed.db")
	db, err := embeddb.Open(dbPath, embeddb.OpenOptions{Migrate: true, AutoIndex: false})
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

func (s *Server) getTable(name string) (*embeddb.Table[Record], error) {
	return embeddb.Use[Record](s.db, name)
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
	schemaData, _ := r.ReadBytes()

	_ = schemaData

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
	schemaName, _ := r.ReadString()

	_ = schemaName

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
		WriteError(w, fmt.Errorf("table %s not found: %w", table, err))
		conn.Write(w.Bytes())
		return
	}

	record := &Record{Data: recordData}
	id, err := tableRef.Insert(record)
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
		WriteError(w, fmt.Errorf("table %s not found: %w", table, err))
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
		WriteError(w, fmt.Errorf("table %s not found: %w", table, err))
		conn.Write(w.Bytes())
		return
	}

	record := &Record{Data: recordData}
	err = tableRef.Update(uint32(id), record)
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
		WriteError(w, fmt.Errorf("table %s not found: %w", table, err))
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

	w := NewWriter(nil)

	_, err := s.getTable(table)
	if err != nil {
		WriteError(w, fmt.Errorf("table %s not found: %w", table, err))
		conn.Write(w.Bytes())
		return
	}

	w.WriteUint64(0)
	WriteResp(w, Response{Success: true, Data: w.Bytes()})
	conn.Write(w.Bytes())
}

func (s *Server) handleQueryGT(r *Reader, conn net.Conn) {
	w := NewWriter(nil)
	w.WriteUint64(0)
	WriteResp(w, Response{Success: true, Data: w.Bytes()})
	conn.Write(w.Bytes())
}

func (s *Server) handleQueryLT(r *Reader, conn net.Conn) {
	w := NewWriter(nil)
	w.WriteUint64(0)
	WriteResp(w, Response{Success: true, Data: w.Bytes()})
	conn.Write(w.Bytes())
}

func (s *Server) handleQueryBetween(r *Reader, conn net.Conn) {
	w := NewWriter(nil)
	w.WriteUint64(0)
	WriteResp(w, Response{Success: true, Data: w.Bytes()})
	conn.Write(w.Bytes())
}

func (s *Server) handleFilter(r *Reader, conn net.Conn) {
	table, _ := r.ReadString()

	w := NewWriter(nil)

	tableRef, err := s.getTable(table)
	if err != nil {
		WriteError(w, fmt.Errorf("table %s not found: %w", table, err))
		conn.Write(w.Bytes())
		return
	}

	results, err := tableRef.Filter(func(r Record) bool {
		return true
	})
	if err != nil {
		WriteError(w, fmt.Errorf("filter failed: %w", err))
		conn.Write(w.Bytes())
		return
	}

	w.WriteUint64(uint64(len(results)))
	for _, record := range results {
		w.WriteBytes(record.Data)
	}

	WriteResp(w, Response{Success: true, Data: w.Bytes()})
	conn.Write(w.Bytes())
}

func (s *Server) handleScan(r *Reader, conn net.Conn) {
	table, _ := r.ReadString()

	w := NewWriter(nil)

	tableRef, err := s.getTable(table)
	if err != nil {
		WriteError(w, fmt.Errorf("table %s not found: %w", table, err))
		conn.Write(w.Bytes())
		return
	}

	count := 0
	err = tableRef.Scan(func(r Record) bool {
		count++
		return true
	})
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

	tableRef, err := s.getTable(table)
	if err != nil {
		WriteError(w, fmt.Errorf("table %s not found: %w", table, err))
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

func decodeValue(data []byte) (interface{}, error) {
	if len(data) == 0 {
		return nil, nil
	}

	firstByte := data[0]

	switch firstByte {
	case 'i', 0x00:
		val, _, err := embeddb.DecodeVarint(data)
		return val, err
	case 'u':
		val, _, err := embeddb.DecodeUvarint(data)
		return val, err
	case 'f':
		val, _, err := embeddb.DecodeFloat64(data)
		return val, err
	case 's':
		val, _, err := embeddb.DecodeString(data)
		return val, err
	case 'b':
		val, _, err := embeddb.DecodeBool(data)
		return val, err
	default:
		return nil, fmt.Errorf("unknown type marker: %c", firstByte)
	}
}
