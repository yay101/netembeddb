package netembeddb

import (
	"fmt"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/yay101/netembeddb/protocol"
)

func TestServerListen(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	sockPath := filepath.Join(tmpDir, "test.sock")

	server := NewServer(dbPath, "")
	err := server.Listen("", sockPath)
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer server.Close()

	time.Sleep(100 * time.Millisecond)

	conn, err := net.Dial("unix", sockPath)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	conn.Close()

	t.Log("Server listening and accepting connections")
}

func TestServerWithTCP(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	server := NewServer(dbPath, "")
	err := server.Listen("localhost:19999", "")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer server.Close()

	time.Sleep(100 * time.Millisecond)

	conn, err := net.DialTimeout("tcp", "localhost:19999", time.Second)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	conn.Close()

	t.Log("Server listening on TCP and accepting connections")
}

func TestClientInsertGet(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	sockPath := filepath.Join(tmpDir, "test.sock")

	server := NewServer(dbPath, "")
	err := server.Listen("", sockPath)
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		server.Wait()
	}()

	time.Sleep(100 * time.Millisecond)

	client, err := Dial(sockPath)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer client.Close()

	err = client.CreateTable("users")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	data := []byte(`{"name":"Alice","age":30}`)
	id, err := client.Insert("users", data)
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}
	t.Logf("Inserted record with ID: %d", id)

	retrieved, err := client.Get("users", id)
	if err != nil {
		t.Fatalf("failed to get: %v", err)
	}

	if string(retrieved) != string(data) {
		t.Errorf("expected %s, got %s", data, retrieved)
	}

	count, err := client.Count("users")
	if err != nil {
		t.Fatalf("failed to count: %v", err)
	}
	if count != 1 {
		t.Errorf("expected count 1, got %d", count)
	}

	client.Close()
	server.Close()
}

func TestProtocolCodec(t *testing.T) {
	w := protocol.NewWriter()
	w.WriteByte(protocol.OpInsert)
	w.WriteString("users")
	w.WriteBytes([]byte(`{"name":"Bob"}`))

	data := w.Bytes()
	if len(data) < 3 {
		t.Fatalf("expected at least 3 bytes, got %d", len(data))
	}

	if data[0] != protocol.OpInsert {
		t.Errorf("expected OpInsert (%d), got %d", protocol.OpInsert, data[0])
	}

	t.Logf("Encoded message: %v", data)
}

func TestLocalMode(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test_local.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	err = db.CreateTable("users")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	table, err := db.Use("users")
	if err != nil {
		t.Fatalf("failed to use table: %v", err)
	}

	id1, err := table.Insert([]byte(`{"name":"Alice","age":30}`))
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}
	if id1 == 0 {
		t.Error("expected non-zero ID")
	}

	_, err = table.Insert([]byte(`{"name":"Bob","age":25}`))
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	data, err := table.Get(id1)
	if err != nil {
		t.Fatalf("failed to get: %v", err)
	}
	if string(data) != `{"name":"Alice","age":30}` {
		t.Errorf("expected Alice data, got %s", string(data))
	}

	count := table.Count()
	if count != 2 {
		t.Errorf("expected count 2, got %d", count)
	}

	err = table.Delete(id1)
	if err != nil {
		t.Fatalf("failed to delete: %v", err)
	}

	count = table.Count()
	if count != 2 {
		t.Errorf("expected count 2 (includes deleted), got %d", count)
	}

	t.Log("Local mode tests passed")
}

func TestClientConcurrent(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test_concurrent.db")
	sockPath := filepath.Join(tmpDir, "test.sock")

	server := NewServer(dbPath, "")
	err := server.Listen("", sockPath)
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer server.Close()

	time.Sleep(100 * time.Millisecond)

	setupClient, err := Dial(sockPath)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}

	err = setupClient.CreateTable("counters")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}
	setupClient.Close()

	var wg sync.WaitGroup
	numGoroutines := 10
	insertsPerGoroutine := 50

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			client, err := Dial(sockPath)
			if err != nil {
				t.Errorf("dial failed: %v", err)
				return
			}
			defer client.Close()

			for j := 0; j < insertsPerGoroutine; j++ {
				data := []byte(fmt.Sprintf(`{"id":%d,"value":%d}`, id, j))
				_, err := client.Insert("counters", data)
				if err != nil {
					t.Errorf("insert failed: %v", err)
					return
				}
			}
		}(i)
	}

	wg.Wait()

	countClient, err := Dial(sockPath)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer countClient.Close()

	count, err := countClient.Count("counters")
	if err != nil {
		t.Fatalf("failed to count: %v", err)
	}

	expected := numGoroutines * insertsPerGoroutine
	if count != uint32(expected) {
		t.Errorf("expected %d records, got %d", expected, count)
	}

	t.Logf("Concurrent test passed: %d records inserted", count)
}
