package netembeddb

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/yay101/embeddb"
	"github.com/yay101/netembeddb/protocol"
)

type Server struct {
	mu      sync.RWMutex
	db      *embeddb.DB
	dbPath  string
	authKey string
	lnTCP   net.Listener
	lnUnix  net.Listener
	wg      sync.WaitGroup
	closed  bool
}

func NewServer(dbPath string, authKey string) *Server {
	return &Server{
		dbPath:  dbPath,
		authKey: authKey,
	}
}

func (s *Server) Listen(tcpAddr, unixAddr string) error {
	if err := os.MkdirAll(filepath.Dir(s.dbPath), 0755); err != nil {
		return fmt.Errorf("failed to create db directory: %w", err)
	}

	db, err := embeddb.Open(s.dbPath, embeddb.OpenOptions{Migrate: true, AutoIndex: true})
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	s.db = db

	if tcpAddr != "" {
		ln, err := net.Listen("tcp", tcpAddr)
		if err != nil {
			return fmt.Errorf("failed to listen on TCP %s: %w", tcpAddr, err)
		}
		s.lnTCP = ln
	}

	if unixAddr != "" {
		os.Remove(unixAddr)
		ln, err := net.Listen("unix", unixAddr)
		if err != nil {
			return fmt.Errorf("failed to listen on Unix %s: %w", unixAddr, err)
		}
		s.lnUnix = ln
		os.Chmod(unixAddr, 0777)
	}

	s.wg.Add(1)
	go s.serve(s.lnTCP)
	s.wg.Add(1)
	go s.serve(s.lnUnix)

	return nil
}

func (s *Server) serve(ln net.Listener) {
	defer s.wg.Done()
	if ln == nil {
		return
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			if s.closed {
				return
			}
			continue
		}

		s.wg.Add(1)
		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()

	if s.authKey != "" {
		if err := authenticate(conn, s.authKey); err != nil {
			writeAuthResponse(conn, err)
			return
		}
	}
	writeAuthResponse(conn, nil)

	r := protocol.NewReader(conn)

	for {
		msgType, err := r.ReadByte()
		if err != nil {
			return
		}

		switch msgType {
		case protocol.OpInsert:
			s.handleInsert(r, conn)
		case protocol.OpGet:
			s.handleGet(r, conn)
		case protocol.OpUpdate:
			s.handleUpdate(r, conn)
		case protocol.OpDelete:
			s.handleDelete(r, conn)
		case protocol.OpQuery:
			s.handleQuery(r, conn)
		case protocol.OpQueryRange:
			s.handleQueryRange(r, conn)
		case protocol.OpQueryPaged:
			s.handleQueryPaged(r, conn)
		case protocol.OpFilter:
			s.handleFilter(r, conn)
		case protocol.OpFilterPaged:
			s.handleFilterPaged(r, conn)
		case protocol.OpScan:
			s.handleScan(r, conn)
		case protocol.OpCount:
			s.handleCount(r, conn)
		case protocol.OpVacuum:
			s.handleVacuum(r, conn)
		case protocol.OpCreateTable:
			s.handleCreateTable(r, conn)
		case protocol.OpListTables:
			s.handleListTables(r, conn)
		case protocol.OpCreateIndex:
			s.handleCreateIndex(r, conn)
		case protocol.OpDropIndex:
			s.handleDropIndex(r, conn)
		default:
			s.writeError(conn, fmt.Errorf("unknown operation: %d", msgType))
		}
	}
}

func (s *Server) Close() error {
	s.closed = true

	if s.lnTCP != nil {
		s.lnTCP.Close()
	}
	if s.lnUnix != nil {
		s.lnUnix.Close()
	}

	s.wg.Wait()

	if s.db != nil {
		if err := s.db.Sync(); err != nil {
			return err
		}
		return s.db.Close()
	}
	return nil
}

func (s *Server) Wait() {
	s.wg.Wait()
}

func (s *Server) Vacuum() error {
	return s.db.Vacuum()
}

func (s *Server) Sync() error {
	return s.db.Sync()
}

func (s *Server) Table(name string) (*embeddb.Table[any], error) {
	return embeddb.Use[any](s.db, name)
}

func (s *Server) writeError(conn net.Conn, err error) {
	w := protocol.NewWriter()
	w.WriteByte(protocol.OpError)
	w.WriteString(err.Error())
	conn.Write(w.Bytes())
}

func (s *Server) writeSuccess(conn net.Conn, data []byte) {
	w := protocol.NewWriter()
	w.WriteByte(protocol.OpResponse)
	w.WriteBytes(data)
	conn.Write(w.Bytes())
}

const authTimeout = 5 * time.Second

func authenticate(conn net.Conn, authKey string) error {
	if authKey == "" {
		conn.Write([]byte{1})
		return nil
	}

	conn.SetReadDeadline(time.Now().Add(authTimeout))
	defer conn.SetReadDeadline(time.Time{})

	r := protocol.NewReader(conn)
	keyLen, err := r.ReadUvarint()
	if err != nil {
		return fmt.Errorf("failed to read key length: %w", err)
	}

	if keyLen > 1024 {
		return fmt.Errorf("auth key too long")
	}

	keyData, err := r.ReadBytes()
	if err != nil {
		return fmt.Errorf("failed to read key: %w", err)
	}

	if string(keyData) != authKey {
		return fmt.Errorf("invalid auth key")
	}

	return nil
}

func writeAuthResponse(conn net.Conn, err error) {
	conn.SetWriteDeadline(time.Now().Add(authTimeout))
	defer conn.SetWriteDeadline(time.Time{})

	if err != nil {
		conn.Write([]byte{0})
		w := protocol.NewWriter()
		w.WriteString(err.Error())
		conn.Write(w.Bytes())
		return
	}
	conn.Write([]byte{1})
}
