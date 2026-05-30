package netembeddb

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/yay101/embeddb"
	embedcore "github.com/yay101/embeddbcore"
	"github.com/yay101/netembeddb/protocol"
)

type indexableField struct {
	name string
	kind reflect.Kind
}

type registeredSchema struct {
	layout         *embedcore.StructLayout
	indexableFields []indexableField
	pkType         reflect.Kind
	primaryKey     string
}

func (rs *registeredSchema) findField(name string) *indexableField {
	for i := range rs.indexableFields {
		if rs.indexableFields[i].name == name {
			return &rs.indexableFields[i]
		}
	}
	return nil
}

type Server struct {
	db       *embeddb.DB
	listener net.Listener
	sockPath string
	authKey  string
	mu       sync.Mutex
	closed   bool
	schemas  map[string]*registeredSchema
	activeTx *embeddb.Transaction
}

type ServerOptions struct {
	AuthKey string
}

func NewServer(filename string, opts ...ServerOptions) (*Server, error) {
	var authKey string
	if len(opts) > 0 {
		authKey = opts[0].AuthKey
	}

	db, err := embeddb.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	return &Server{
		db:      db,
		authKey: authKey,
		schemas: make(map[string]*registeredSchema),
	}, nil
}

func (s *Server) Listen(addr, sockPath string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var listener net.Listener
	var err error

	if addr != "" {
		listener, err = net.Listen("tcp", addr)
		if err != nil {
			return fmt.Errorf("failed to listen on TCP: %w", err)
		}
	}

	if sockPath != "" {
		unixListener, err := net.Listen("unix", sockPath)
		if err != nil {
			if listener != nil {
				listener.Close()
			}
			return fmt.Errorf("failed to listen on unix socket: %w", err)
		}
		if listener != nil {
			go s.handleListener(listener)
			listener = unixListener
		} else {
			listener = unixListener
		}
	}

	if listener == nil {
		return fmt.Errorf("no address or socket path provided")
	}

	s.listener = listener
	s.sockPath = sockPath

	go s.handleListener(listener)
	return nil
}

func (s *Server) handleListener(listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			s.mu.Lock()
			if s.closed {
				s.mu.Unlock()
				return
			}
			s.mu.Unlock()
			continue
		}
		go s.handleConnection(conn)
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()

	enc := protocol.NewEncoder(conn)

	if len(s.authKey) > 0 {
		buf := make([]byte, len(s.authKey))
		conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		n, err := conn.Read(buf)
		if err != nil || string(buf[:n]) != s.authKey {
			enc.EncodeResponse(&protocol.Response{Success: false, Error: "auth failed"})
			return
		}
		enc.EncodeResponse(&protocol.Response{Success: true})
	}

	for {
		conn.SetReadDeadline(time.Now().Add(30 * time.Second))

		hdr := make([]byte, 5)
		_, err := conn.Read(hdr)
		if err != nil {
			return
		}

		op := protocol.OpCode(hdr[0])
		dataLen := binary.BigEndian.Uint32(hdr[1:5])

		var data []byte
		if dataLen > 0 {
			data = make([]byte, dataLen)
			_, err = conn.Read(data)
			if err != nil {
				return
			}
		}

		var resp *protocol.Response
		switch op {
		case protocol.OpClose:
			return
		case protocol.OpRegisterSchema:
			resp = s.handleRegisterSchema(data)
		case protocol.OpInsert:
			resp = s.handleInsert(data)
		case protocol.OpGet:
			resp = s.handleGet(data)
		case protocol.OpUpdate:
			resp = s.handleUpdate(data)
		case protocol.OpDelete:
			resp = s.handleDelete(data)
		case protocol.OpCount:
			resp = s.handleCount(data)
		case protocol.OpUpsert:
			resp = s.handleUpsert(data)
		case protocol.OpInsertMany:
			resp = s.handleInsertMany(data)
		case protocol.OpInsertManyBulk:
			resp = s.handleInsertManyBulk(data)
		case protocol.OpDeleteMany:
			resp = s.handleDeleteMany(data)
		case protocol.OpUpdateMany:
			resp = s.handleUpdateMany(data)
		case protocol.OpQuery:
			resp = s.handleQuery(data)
		case protocol.OpQueryRange:
			resp = s.handleQueryRange(data)
		case protocol.OpQueryLike:
			resp = s.handleQueryLike(data)
		case protocol.OpScan, protocol.OpAll:
			resp = s.handleScan(data)
		case protocol.OpDrop:
			resp = s.handleDrop(data)
		case protocol.OpCreateIndex:
			resp = s.handleCreateIndex(data)
		case protocol.OpDropIndex:
			resp = s.handleDropIndex(data)
		case protocol.OpGetIndexed:
			resp = s.handleGetIndexed(data)
		case protocol.OpVacuum:
			resp = s.handleVacuum(data)
		case protocol.OpSync:
			resp = s.handleSync()
		case protocol.OpBegin:
			resp = s.handleBegin()
		case protocol.OpCommit:
			resp = s.handleCommit()
		case protocol.OpRollback:
			resp = s.handleRollback()
		case protocol.OpBackup:
			resp = s.handleBackup(data)
		case protocol.OpStats:
			resp = s.handleStats()
		default:
			resp = &protocol.Response{Success: false, Error: fmt.Sprintf("unknown op: %x", op)}
		}

		if err := enc.EncodeResponse(resp); err != nil {
			return
		}
	}
}

func (s *Server) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.closed = true

	if s.listener != nil {
		s.listener.Close()
	}

	if s.sockPath != "" {
		os.Remove(s.sockPath)
	}

	if s.db != nil {
		return s.db.Close()
	}

	return nil
}

type storedRecord struct {
	ID   uint32 `db:"id,primary"`
	Data []byte `db:"data"`
}

func (s *Server) getTable(tableName string) (*embeddb.Table[storedRecord], *registeredSchema, error) {
	t, err := embeddb.Use[storedRecord](s.db, tableName)
	if err != nil {
		return nil, nil, err
	}
	schema := s.schemas[tableName]
	return t, schema, nil
}

func (s *Server) handleRegisterSchema(data []byte) *protocol.Response {
	pos := 0

	tableName, n, err := protocol.DecodeString(data)
	if err != nil {
		return &protocol.Response{Success: false, Error: err.Error()}
	}
	pos += n

	primaryKey, n, err := protocol.DecodeString(data[pos:])
	if err != nil {
		return &protocol.Response{Success: false, Error: err.Error()}
	}
	pos += n

	if pos >= len(data) {
		return &protocol.Response{Success: false, Error: "missing pk type"}
	}
	pkType := reflect.Kind(data[pos])
	pos++

	if pos >= len(data) {
		return &protocol.Response{Success: false, Error: "missing field count"}
	}
	fieldCount := int(data[pos])
	pos++

	fields := make([]embedcore.FieldOffset, 0, fieldCount)
	var indexable []indexableField

	for i := 0; i < fieldCount && pos < len(data); i++ {
		name, n, err := protocol.DecodeString(data[pos:])
		if err != nil {
			break
		}
		pos += n

		_, n, err = protocol.DecodeString(data[pos:])
		if err != nil {
			break
		}
		pos += n

		kindVal, n, err := protocol.DecodeUint64(data[pos:])
		if err != nil {
			break
		}
		pos += n
		kind := reflect.Kind(kindVal)

		isPrimary := (name == primaryKey)
		fields = append(fields, embedcore.FieldOffset{
			Name:    name,
			Type:    kind,
			Primary: isPrimary,
			Offset:  1,
		})

		if !isPrimary {
			indexable = append(indexable, indexableField{name: name, kind: kind})
		}
	}

	layout := &embedcore.StructLayout{
		Fields:     fields,
		PrimaryKey: primaryKey,
		PKType:     pkType,
	}

	s.schemas[tableName] = &registeredSchema{
		layout:         layout,
		indexableFields: indexable,
		pkType:         pkType,
		primaryKey:     primaryKey,
	}
	return &protocol.Response{Success: true}
}

func (s *Server) handleInsert(data []byte) *protocol.Response {
	pos := 0
	tableName, n, err := protocol.DecodeString(data)
	if err != nil {
		return &protocol.Response{Success: false, Error: err.Error()}
	}
	pos += n

	recData := data[pos:]

	s.mu.Lock()
	defer s.mu.Unlock()

	t, schema, err := s.getTable(tableName)
	if err != nil {
		return &protocol.Response{Success: false, Error: err.Error()}
	}

	id, err := t.Insert(&storedRecord{Data: recData})
	if err != nil {
		return &protocol.Response{Success: false, Error: err.Error()}
	}

	s.createSecondaryKeys(t, schema, recData, id)

	respData := make([]byte, 4)
	binary.BigEndian.PutUint32(respData, id)
	return &protocol.Response{Success: true, Data: respData}
}

func (s *Server) handleGet(data []byte) *protocol.Response {
	pos := 0
	tableName, n, err := protocol.DecodeString(data)
	if err != nil {
		return &protocol.Response{Success: false, Error: err.Error()}
	}
	pos += n

	if pos+4 > len(data) {
		return &protocol.Response{Success: false, Error: "missing record id"}
	}
	id := binary.BigEndian.Uint32(data[pos:])

	s.mu.Lock()
	defer s.mu.Unlock()

	t, _, err := s.getTable(tableName)
	if err != nil {
		return &protocol.Response{Success: false, Error: err.Error()}
	}

	rec, err := t.Get(id)
	if err != nil {
		return &protocol.Response{Success: false, Error: err.Error()}
	}

	return &protocol.Response{Success: true, Data: rec.Data}
}

func (s *Server) handleUpdate(data []byte) *protocol.Response {
	pos := 0
	tableName, n, err := protocol.DecodeString(data)
	if err != nil {
		return &protocol.Response{Success: false, Error: err.Error()}
	}
	pos += n

	if pos+4 > len(data) {
		return &protocol.Response{Success: false, Error: "missing record id"}
	}
	id := binary.BigEndian.Uint32(data[pos:])
	pos += 4

	recData := data[pos:]

	s.mu.Lock()
	defer s.mu.Unlock()

	t, schema, err := s.getTable(tableName)
	if err != nil {
		return &protocol.Response{Success: false, Error: err.Error()}
	}

	oldRec, err := t.Get(id)
	if err == nil && schema != nil {
		s.deleteSecondaryKeys(t, schema, oldRec.Data, id)
	}

	err = t.Update(id, &storedRecord{ID: id, Data: recData})
	if err != nil {
		return &protocol.Response{Success: false, Error: err.Error()}
	}

	s.createSecondaryKeys(t, schema, recData, id)

	return &protocol.Response{Success: true}
}

func (s *Server) handleDelete(data []byte) *protocol.Response {
	pos := 0
	tableName, n, err := protocol.DecodeString(data)
	if err != nil {
		return &protocol.Response{Success: false, Error: err.Error()}
	}
	pos += n

	if pos+4 > len(data) {
		return &protocol.Response{Success: false, Error: "missing record id"}
	}
	id := binary.BigEndian.Uint32(data[pos:])

	s.mu.Lock()
	defer s.mu.Unlock()

	t, schema, err := s.getTable(tableName)
	if err != nil {
		return &protocol.Response{Success: false, Error: err.Error()}
	}

	rec, err := t.Get(id)
	if err == nil && schema != nil {
		s.deleteSecondaryKeys(t, schema, rec.Data, id)
	}

	err = t.Delete(id)
	if err != nil {
		return &protocol.Response{Success: false, Error: err.Error()}
	}

	return &protocol.Response{Success: true}
}

func (s *Server) handleCount(data []byte) *protocol.Response {
	tableName, _, err := protocol.DecodeString(data)
	if err != nil {
		return &protocol.Response{Success: false, Error: err.Error()}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	t, _, err := s.getTable(tableName)
	if err != nil {
		return &protocol.Response{Success: false, Error: err.Error()}
	}

	count := t.Count()
	respData := make([]byte, 8)
	binary.BigEndian.PutUint64(respData, uint64(count))
	return &protocol.Response{Success: true, Data: respData}
}

func (s *Server) handleUpsert(data []byte) *protocol.Response {
	pos := 0
	tableName, n, err := protocol.DecodeString(data)
	if err != nil {
		return &protocol.Response{Success: false, Error: err.Error()}
	}
	pos += n

	if pos+4 > len(data) {
		return &protocol.Response{Success: false, Error: "missing record id"}
	}
	id := binary.BigEndian.Uint32(data[pos:])
	pos += 4

	recData := data[pos:]

	s.mu.Lock()
	defer s.mu.Unlock()

	t, schema, err := s.getTable(tableName)
	if err != nil {
		return &protocol.Response{Success: false, Error: err.Error()}
	}

	_, getErr := t.Get(id)
	if getErr == nil {
		oldRec, _ := t.Get(id)
		if oldRec != nil && schema != nil {
			s.deleteSecondaryKeys(t, schema, oldRec.Data, id)
		}
		if err := t.Update(id, &storedRecord{ID: id, Data: recData}); err != nil {
			return &protocol.Response{Success: false, Error: err.Error()}
		}
		s.createSecondaryKeys(t, schema, recData, id)
		respData := make([]byte, 5)
		respData[0] = 0
		binary.BigEndian.PutUint32(respData[1:5], 0)
		return &protocol.Response{Success: true, Data: respData}
	}

	recordID, err := t.Insert(&storedRecord{Data: recData})
	if err != nil {
		return &protocol.Response{Success: false, Error: err.Error()}
	}

	s.createSecondaryKeys(t, schema, recData, recordID)

	respData := make([]byte, 5)
	respData[0] = 1
	binary.BigEndian.PutUint32(respData[1:5], recordID)
	return &protocol.Response{Success: true, Data: respData}
}

func (s *Server) handleInsertMany(data []byte) *protocol.Response {
	pos := 0
	tableName, n, err := protocol.DecodeString(data)
	if err != nil {
		return &protocol.Response{Success: false, Error: err.Error()}
	}
	pos += n

	count, n2, err := protocol.DecodeUint64(data[pos:])
	if err != nil {
		return &protocol.Response{Success: false, Error: err.Error()}
	}
	pos += n2

	records := make([][]byte, 0, count)
	for i := uint64(0); i < count && pos < len(data); i++ {
		rec, n3, err := protocol.DecodeBytes(data[pos:])
		if err != nil {
			break
		}
		records = append(records, rec)
		pos += n3
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	t, schema, err := s.getTable(tableName)
	if err != nil {
		return &protocol.Response{Success: false, Error: err.Error()}
	}

	ids := make([]uint32, 0, len(records))
	for _, recData := range records {
		id, err := t.Insert(&storedRecord{Data: recData})
		if err != nil {
			continue
		}
		ids = append(ids, id)
		s.createSecondaryKeys(t, schema, recData, id)
	}

	respData := make([]byte, 4+4*len(ids))
	binary.BigEndian.PutUint32(respData, uint32(len(ids)))
	for i, id := range ids {
		binary.BigEndian.PutUint32(respData[4+i*4:], id)
	}
	return &protocol.Response{Success: true, Data: respData}
}

func (s *Server) handleInsertManyBulk(data []byte) *protocol.Response {
	return s.handleInsertMany(data)
}

func (s *Server) handleDeleteMany(data []byte) *protocol.Response {
	pos := 0
	tableName, n, err := protocol.DecodeString(data)
	if err != nil {
		return &protocol.Response{Success: false, Error: err.Error()}
	}
	pos += n

	count, n2, err := protocol.DecodeUint64(data[pos:])
	if err != nil {
		return &protocol.Response{Success: false, Error: err.Error()}
	}
	pos += n2

	ids := make([]uint32, 0, count)
	for i := uint64(0); i < count && pos+4 <= len(data); i++ {
		ids = append(ids, binary.BigEndian.Uint32(data[pos:]))
		pos += 4
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	t, schema, err := s.getTable(tableName)
	if err != nil {
		return &protocol.Response{Success: false, Error: err.Error()}
	}

	deleted := 0
	for _, id := range ids {
		if schema != nil {
			rec, err := t.Get(id)
			if err == nil {
				s.deleteSecondaryKeys(t, schema, rec.Data, id)
			}
		}
		if err := t.Delete(id); err == nil {
			deleted++
		}
	}

	respData := make([]byte, 4)
	binary.BigEndian.PutUint32(respData, uint32(deleted))
	return &protocol.Response{Success: true, Data: respData}
}

func (s *Server) handleUpdateMany(data []byte) *protocol.Response {
	pos := 0
	tableName, n, err := protocol.DecodeString(data)
	if err != nil {
		return &protocol.Response{Success: false, Error: err.Error()}
	}
	pos += n

	count, n2, err := protocol.DecodeUint64(data[pos:])
	if err != nil {
		return &protocol.Response{Success: false, Error: err.Error()}
	}
	pos += n2

	s.mu.Lock()
	defer s.mu.Unlock()

	t, schema, err := s.getTable(tableName)
	if err != nil {
		return &protocol.Response{Success: false, Error: err.Error()}
	}

	updated := 0
	for i := uint64(0); i < count && pos < len(data); i++ {
		if pos+4 > len(data) {
			break
		}
		id := binary.BigEndian.Uint32(data[pos:])
		pos += 4

		recData, n3, err := protocol.DecodeBytes(data[pos:])
		if err != nil {
			break
		}
		pos += n3

		if schema != nil {
			oldRec, err := t.Get(id)
			if err == nil {
				s.deleteSecondaryKeys(t, schema, oldRec.Data, id)
			}
		}

		if err := t.Update(id, &storedRecord{ID: id, Data: recData}); err != nil {
			continue
		}

		s.createSecondaryKeys(t, schema, recData, id)
		updated++
	}

	respData := make([]byte, 4)
	binary.BigEndian.PutUint32(respData, uint32(updated))
	return &protocol.Response{Success: true, Data: respData}
}

func (s *Server) handleQuery(data []byte) *protocol.Response {
	pos := 0
	tableName, n, err := protocol.DecodeString(data)
	if err != nil {
		return &protocol.Response{Success: false, Error: err.Error()}
	}
	pos += n

	fieldName, n, err := protocol.DecodeString(data[pos:])
	if err != nil {
		return &protocol.Response{Success: false, Error: err.Error()}
	}
	pos += n

	valueStr, _, err := protocol.DecodeString(data[pos:])
	if err != nil {
		return &protocol.Response{Success: false, Error: err.Error()}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	return s.queryExact(tableName, fieldName, valueStr)
}

func (s *Server) handleQueryRange(data []byte) *protocol.Response {
	pos := 0
	tableName, n, err := protocol.DecodeString(data)
	if err != nil {
		return &protocol.Response{Success: false, Error: err.Error()}
	}
	pos += n

	fieldName, n, err := protocol.DecodeString(data[pos:])
	if err != nil {
		return &protocol.Response{Success: false, Error: err.Error()}
	}
	pos += n

	if pos >= len(data) {
		return &protocol.Response{Success: false, Error: "missing flags"}
	}
	flags := data[pos]
	pos++

	valueStr, n, err := protocol.DecodeString(data[pos:])
	if err != nil {
		return &protocol.Response{Success: false, Error: err.Error()}
	}
	pos += n

	var maxValueStr string
	if flags&protocol.RangeFlagBtwn != 0 {
		maxValueStr, n, err = protocol.DecodeString(data[pos:])
		if err != nil {
			return &protocol.Response{Success: false, Error: err.Error()}
		}
		pos += n
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	isGT := flags&protocol.RangeFlagGT != 0
	inclusive := flags&protocol.RangeFlagIncl != 0

	if flags&protocol.RangeFlagBtwn != 0 {
		return s.queryRangeBetween(tableName, fieldName, valueStr, maxValueStr, inclusive, flags&protocol.RangeFlagIncl != 0)
	}
	if isGT {
		return s.queryRangeGT(tableName, fieldName, valueStr, inclusive)
	}
	return s.queryRangeLT(tableName, fieldName, valueStr, inclusive)
}

func (s *Server) handleQueryLike(data []byte) *protocol.Response {
	pos := 0
	tableName, n, err := protocol.DecodeString(data)
	if err != nil {
		return &protocol.Response{Success: false, Error: err.Error()}
	}
	pos += n

	fieldName, n, err := protocol.DecodeString(data[pos:])
	if err != nil {
		return &protocol.Response{Success: false, Error: err.Error()}
	}
	pos += n

	pattern, _, err := protocol.DecodeString(data[pos:])
	if err != nil {
		return &protocol.Response{Success: false, Error: err.Error()}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	return s.queryLike(tableName, fieldName, pattern)
}

func (s *Server) handleScan(data []byte) *protocol.Response {
	tableName, _, err := protocol.DecodeString(data)
	if err != nil {
		return &protocol.Response{Success: false, Error: err.Error()}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	t, _, err := s.getTable(tableName)
	if err != nil {
		return &protocol.Response{Success: false, Error: err.Error()}
	}

	all, err := t.All()
	if err != nil {
		return &protocol.Response{Success: false, Error: err.Error()}
	}

	results := make([][]byte, 0, len(all))
	for _, rec := range all {
		results = append(results, rec.Data)
	}

	if results == nil {
		results = [][]byte{}
	}
	return &protocol.Response{Success: true, Data: encodeBytesSlice(results)}
}

func (s *Server) handleDrop(data []byte) *protocol.Response {
	tableName, _, err := protocol.DecodeString(data)
	if err != nil {
		return &protocol.Response{Success: false, Error: err.Error()}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	t, _, err := s.getTable(tableName)
	if err != nil {
		return &protocol.Response{Success: false, Error: err.Error()}
	}

	if err := t.Drop(); err != nil {
		return &protocol.Response{Success: false, Error: err.Error()}
	}

	delete(s.schemas, tableName)
	return &protocol.Response{Success: true}
}

func (s *Server) handleCreateIndex(data []byte) *protocol.Response {
	pos := 0
	tableName, n, err := protocol.DecodeString(data)
	if err != nil {
		return &protocol.Response{Success: false, Error: err.Error()}
	}
	pos += n

	fieldName, n, err := protocol.DecodeString(data[pos:])
	if err != nil {
		return &protocol.Response{Success: false, Error: err.Error()}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	t, _, err := s.getTable(tableName)
	if err != nil {
		return &protocol.Response{Success: false, Error: err.Error()}
	}

	err = t.CreateIndex(fieldName)
	if err != nil {
		return &protocol.Response{Success: false, Error: err.Error()}
	}

	return &protocol.Response{Success: true}
}

func (s *Server) handleDropIndex(data []byte) *protocol.Response {
	pos := 0
	tableName, n, err := protocol.DecodeString(data)
	if err != nil {
		return &protocol.Response{Success: false, Error: err.Error()}
	}
	pos += n

	fieldName, n, err := protocol.DecodeString(data[pos:])
	if err != nil {
		return &protocol.Response{Success: false, Error: err.Error()}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	t, _, err := s.getTable(tableName)
	if err != nil {
		return &protocol.Response{Success: false, Error: err.Error()}
	}

	err = t.DropIndex(fieldName)
	if err != nil {
		return &protocol.Response{Success: false, Error: err.Error()}
	}

	return &protocol.Response{Success: true}
}

func (s *Server) handleGetIndexed(data []byte) *protocol.Response {
	tableName, _, err := protocol.DecodeString(data)
	if err != nil {
		return &protocol.Response{Success: false, Error: err.Error()}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	t, _, err := s.getTable(tableName)
	if err != nil {
		return &protocol.Response{Success: false, Error: err.Error()}
	}

	fields := t.GetIndexedFields()

	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(len(fields)))
	for _, f := range fields {
		buf = append(buf, protocol.EncodeString(f)...)
	}
	return &protocol.Response{Success: true, Data: buf}
}

func (s *Server) handleVacuum(data []byte) *protocol.Response {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.db.Vacuum(); err != nil {
		return &protocol.Response{Success: false, Error: err.Error()}
	}
	return &protocol.Response{Success: true}
}

func (s *Server) handleSync() *protocol.Response {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.db.Sync(); err != nil {
		return &protocol.Response{Success: false, Error: err.Error()}
	}
	return &protocol.Response{Success: true}
}

func (s *Server) handleBegin() *protocol.Response {
	s.mu.Lock()
	defer s.mu.Unlock()

	tx := s.db.Begin()
	if tx == nil {
		return &protocol.Response{Success: false, Error: "failed to begin transaction"}
	}
	if s.activeTx != nil {
		return &protocol.Response{Success: false, Error: "transaction already in progress"}
	}
	s.activeTx = tx
	return &protocol.Response{Success: true}
}

func (s *Server) handleCommit() *protocol.Response {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.activeTx == nil {
		return &protocol.Response{Success: false, Error: "no active transaction"}
	}
	if err := s.activeTx.Commit(); err != nil {
		return &protocol.Response{Success: false, Error: err.Error()}
	}
	s.activeTx = nil
	return &protocol.Response{Success: true}
}

func (s *Server) handleRollback() *protocol.Response {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.activeTx == nil {
		return &protocol.Response{Success: false, Error: "no active transaction"}
	}
	if err := s.activeTx.Rollback(); err != nil {
		return &protocol.Response{Success: false, Error: err.Error()}
	}
	s.activeTx = nil
	return &protocol.Response{Success: true}
}

func (s *Server) handleBackup(data []byte) *protocol.Response {
	destPath, _, err := protocol.DecodeString(data)
	if err != nil {
		return &protocol.Response{Success: false, Error: err.Error()}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.db.Backup(destPath); err != nil {
		return &protocol.Response{Success: false, Error: err.Error()}
	}
	return &protocol.Response{Success: true}
}

func (s *Server) handleStats() *protocol.Response {
	s.mu.Lock()
	defer s.mu.Unlock()

	dbStats := s.db.Stats()

	tables := make([]protocol.TableStatsInfo, 0, len(dbStats.Tables))
	for _, ts := range dbStats.Tables {
		tables = append(tables, protocol.TableStatsInfo{
			Name:        ts.Name,
			RecordCount: ts.RecordCount,
			TableID:     ts.TableID,
		})
	}

	info := &protocol.StatsInfo{
		Tables:     tables,
		FileSize:   dbStats.FileSize,
		WALSize:    dbStats.WALSize,
		IndexKeys:  dbStats.IndexKeys,
		BTreeDepth: dbStats.BTreeDepth,
	}

	return &protocol.Response{Success: true, Data: protocol.EncodeStats(info)}
}

func (s *Server) createSecondaryKeys(t *embeddb.Table[storedRecord], schema *registeredSchema, recData []byte, recordID uint32) {
	if schema == nil || len(schema.indexableFields) == 0 {
		return
	}

	tableID := t.TableID()
	pkKey := embeddb.EncodePrimaryKey(tableID, recordID)
	offset, err := t.GetRawIndex(pkKey)
	if err != nil {
		return
	}

	remaining := recData
	for len(remaining) > 0 {
		name, value, rem, err := embedcore.DecodeTLVField(remaining)
		if err != nil {
			break
		}
		remaining = rem

		field := schema.findField(name)
		if field == nil {
			continue
		}

		valStr := protocol.DecodeFieldValueAsString(value, field.kind)
		if valStr == "" && field.kind != reflect.String {
			continue
		}

		secKey := embeddb.EncodeSecondaryKey(tableID, field.name, valStr, recordID)
		t.InsertRawIndex(secKey, offset)
	}
}

func (s *Server) deleteSecondaryKeys(t *embeddb.Table[storedRecord], schema *registeredSchema, recData []byte, recordID uint32) {
	if schema == nil || len(schema.indexableFields) == 0 {
		return
	}

	tableID := t.TableID()

	remaining := recData
	for len(remaining) > 0 {
		name, value, rem, err := embedcore.DecodeTLVField(remaining)
		if err != nil {
			break
		}
		remaining = rem

		field := schema.findField(name)
		if field == nil {
			continue
		}

		valStr := protocol.DecodeFieldValueAsString(value, field.kind)
		if valStr == "" {
			continue
		}

		secKey := embeddb.EncodeSecondaryKey(tableID, field.name, valStr, recordID)
		t.DeleteRawIndex(secKey)
	}
}

func (s *Server) queryExact(tableName, fieldName, valueStr string) *protocol.Response {
	t, schema, err := s.getTable(tableName)
	if err != nil {
		return &protocol.Response{Success: false, Error: err.Error()}
	}

	field := schema.findField(fieldName)
	if field == nil {
		return &protocol.Response{Success: false, Error: fmt.Sprintf("field %s not found", fieldName)}
	}

	tableID := t.TableID()
	prefix := embeddb.EncodeSecondaryKeyPrefixWithValue(tableID, fieldName, valueStr)
	endKey := make([]byte, len(prefix))
	copy(endKey, prefix)
	if len(endKey) > 0 {
		endKey[len(endKey)-1]++
	}

	var results [][]byte
	t.ScanRawIndex(prefix, endKey, func(key []byte, val uint64) bool {
		_, _, _, secRecordID, ok := embeddb.ParseSecondaryKey(key)
		if ok {
			rec, err := t.Get(secRecordID)
			if err == nil {
				fieldVal := getFieldStrFromTLV(rec.Data, fieldName, field.kind)
				if fieldVal == valueStr {
					results = append(results, rec.Data)
				}
			}
		}
		return true
	})

	if results == nil {
		results = [][]byte{}
	}
	return &protocol.Response{Success: true, Data: encodeBytesSlice(results)}
}

func (s *Server) queryRangeGT(tableName, fieldName, valueStr string, inclusive bool) *protocol.Response {
	t, schema, err := s.getTable(tableName)
	if err != nil {
		return &protocol.Response{Success: false, Error: err.Error()}
	}

	field := schema.findField(fieldName)
	if field == nil {
		return &protocol.Response{Success: false, Error: fmt.Sprintf("field %s not found", fieldName)}
	}

	tableID := t.TableID()
	prefix := embeddb.EncodeSecondaryKeyPrefix(tableID, fieldName)
	endKey := make([]byte, len(prefix)+1)
	copy(endKey, prefix)
	endKey[len(prefix)] = 0xFF

	var results [][]byte
	t.ScanRawIndex(prefix, endKey, func(key []byte, val uint64) bool {
		_, _, _, secRecordID, ok := embeddb.ParseSecondaryKey(key)
		if ok {
			rec, err := t.Get(secRecordID)
			if err == nil {
				fieldVal := getFieldStrFromTLV(rec.Data, fieldName, field.kind)
				cmp := compareStrValues(fieldVal, valueStr, field.kind)
				if cmp > 0 || (inclusive && cmp == 0) {
					results = append(results, rec.Data)
				}
			}
		}
		return true
	})

	if results == nil {
		results = [][]byte{}
	}
	return &protocol.Response{Success: true, Data: encodeBytesSlice(results)}
}

func (s *Server) queryRangeLT(tableName, fieldName, valueStr string, inclusive bool) *protocol.Response {
	t, schema, err := s.getTable(tableName)
	if err != nil {
		return &protocol.Response{Success: false, Error: err.Error()}
	}

	field := schema.findField(fieldName)
	if field == nil {
		return &protocol.Response{Success: false, Error: fmt.Sprintf("field %s not found", fieldName)}
	}

	tableID := t.TableID()
	prefix := embeddb.EncodeSecondaryKeyPrefix(tableID, fieldName)
	endKey := make([]byte, len(prefix)+1)
	copy(endKey, prefix)
	endKey[len(prefix)] = 0xFF

	var results [][]byte
	t.ScanRawIndex(prefix, endKey, func(key []byte, val uint64) bool {
		_, _, _, secRecordID, ok := embeddb.ParseSecondaryKey(key)
		if ok {
			rec, err := t.Get(secRecordID)
			if err == nil {
				fieldVal := getFieldStrFromTLV(rec.Data, fieldName, field.kind)
				cmp := compareStrValues(fieldVal, valueStr, field.kind)
				if cmp < 0 || (inclusive && cmp == 0) {
					results = append(results, rec.Data)
				}
			}
		}
		return true
	})

	if results == nil {
		results = [][]byte{}
	}
	return &protocol.Response{Success: true, Data: encodeBytesSlice(results)}
}

func (s *Server) queryRangeBetween(tableName, fieldName, minStr, maxStr string, inclusiveMin, inclusiveMax bool) *protocol.Response {
	t, schema, err := s.getTable(tableName)
	if err != nil {
		return &protocol.Response{Success: false, Error: err.Error()}
	}

	field := schema.findField(fieldName)
	if field == nil {
		return &protocol.Response{Success: false, Error: fmt.Sprintf("field %s not found", fieldName)}
	}

	tableID := t.TableID()
	prefix := embeddb.EncodeSecondaryKeyPrefix(tableID, fieldName)
	endKey := make([]byte, len(prefix)+1)
	copy(endKey, prefix)
	endKey[len(prefix)] = 0xFF

	var results [][]byte
	t.ScanRawIndex(prefix, endKey, func(key []byte, val uint64) bool {
		_, _, _, secRecordID, ok := embeddb.ParseSecondaryKey(key)
		if ok {
			rec, err := t.Get(secRecordID)
			if err == nil {
				fieldVal := getFieldStrFromTLV(rec.Data, fieldName, field.kind)
				cmpMin := compareStrValues(fieldVal, minStr, field.kind)
				cmpMax := compareStrValues(fieldVal, maxStr, field.kind)
				aboveMin := cmpMin > 0 || (inclusiveMin && cmpMin == 0)
				belowMax := cmpMax < 0 || (inclusiveMax && cmpMax == 0)
				if aboveMin && belowMax {
					results = append(results, rec.Data)
				}
			}
		}
		return true
	})

	if results == nil {
		results = [][]byte{}
	}
	return &protocol.Response{Success: true, Data: encodeBytesSlice(results)}
}

func (s *Server) queryLike(tableName, fieldName, pattern string) *protocol.Response {
	t, _, err := s.getTable(tableName)
	if err != nil {
		return &protocol.Response{Success: false, Error: err.Error()}
	}

	all, err := t.All()
	if err != nil {
		return &protocol.Response{Success: false, Error: err.Error()}
	}

	var results [][]byte
	for _, rec := range all {
		valStr := getFieldStrFromTLV(rec.Data, fieldName, reflect.String)
		if matchLike(valStr, pattern) {
			results = append(results, rec.Data)
		}
	}

	if results == nil {
		results = [][]byte{}
	}
	return &protocol.Response{Success: true, Data: encodeBytesSlice(results)}
}

func getFieldStrFromTLV(data []byte, fieldName string, kind reflect.Kind) string {
	remaining := data
	for len(remaining) > 0 {
		name, value, rem, err := embedcore.DecodeTLVField(remaining)
		if err != nil {
			break
		}
		remaining = rem
		if name == fieldName {
			return protocol.DecodeFieldValueAsString(value, kind)
		}
	}
	return ""
}

func matchLike(s, pattern string) bool {
	if pattern == "" {
		return s == ""
	}
	if pattern == "%" {
		return true
	}
	sLower := strings.ToLower(s)
	patternLower := strings.ToLower(pattern)
	if len(pattern) >= 2 && pattern[0] == '%' && pattern[len(pattern)-1] == '%' {
		return strings.Contains(sLower, strings.Trim(patternLower, "%"))
	}
	if strings.HasPrefix(pattern, "%") {
		return strings.HasSuffix(sLower, strings.Trim(patternLower, "%"))
	}
	if strings.HasSuffix(pattern, "%") {
		return strings.HasPrefix(sLower, strings.Trim(patternLower, "%"))
	}
	return sLower == patternLower
}

func compareStrValues(a, b string, kind reflect.Kind) int {
	switch kind {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		av, _ := strconv.ParseInt(a, 10, 64)
		bv, _ := strconv.ParseInt(b, 10, 64)
		if av < bv { return -1 }
		if av > bv { return 1 }
		return 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		av, _ := strconv.ParseUint(a, 10, 64)
		bv, _ := strconv.ParseUint(b, 10, 64)
		if av < bv { return -1 }
		if av > bv { return 1 }
		return 0
	case reflect.Float32, reflect.Float64:
		av, _ := strconv.ParseFloat(a, 64)
		bv, _ := strconv.ParseFloat(b, 64)
		if av < bv { return -1 }
		if av > bv { return 1 }
		return 0
	case reflect.String:
		if a < b { return -1 }
		if a > b { return 1 }
		return 0
	case reflect.Bool:
		av := a == "true" || a == "1"
		bv := b == "true" || b == "1"
		if !av && bv { return -1 }
		if av && !bv { return 1 }
		return 0
	}
	return strings.Compare(a, b)
}


