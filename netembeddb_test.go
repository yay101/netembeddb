package netembeddb

import (
	"os"
	"testing"
	"time"
)

type TestUser struct {
	ID    uint32 `db:"id,primary"`
	Name  string `db:"name"`
	Email string `db:"email"`
}

type TestProduct struct {
	ID    uint32  `db:"id,primary"`
	Name  string  `db:"name"`
	Price float64 `db:"price"`
	Stock int     `db:"stock"`
}

type TestAllTypes struct {
	ID       uint32  `db:"id,primary"`
	IntField int     `db:"int_field"`
	Int8     int8    `db:"int8"`
	Int16    int16   `db:"int16"`
	Int32    int32   `db:"int32"`
	Int64    int64   `db:"int64"`
	Uint     uint    `db:"uint"`
	Uint8    uint8   `db:"uint8"`
	Uint16   uint16  `db:"uint16"`
	Uint32   uint32  `db:"uint32"`
	Uint64   uint64  `db:"uint64"`
	F32      float32 `db:"f32"`
	F64      float64 `db:"f64"`
	Flag     bool    `db:"flag"`
	Text     string  `db:"text"`
	Stamp    time.Time `db:"stamp"`
}

func TestEncodeDecode(t *testing.T) {
	layout, err := SchemaFromStruct[TestUser]("users")
	if err != nil {
		t.Fatalf("SchemaFromStruct failed: %v", err)
	}

	record := &TestUser{
		ID:    1,
		Name:  "Alice",
		Email: "alice@example.com",
	}

	encoded, err := EncodeRecord(record, layout)
	if err != nil {
		t.Fatalf("EncodeRecord failed: %v", err)
	}

	if len(encoded) == 0 {
		t.Error("encoded data is empty")
	}

	decoded := &TestUser{}
	err = DecodeRecord(encoded, layout, decoded)
	if err != nil {
		t.Fatalf("DecodeRecord failed: %v", err)
	}

	if decoded.ID != record.ID {
		t.Errorf("ID mismatch: got %d, want %d", decoded.ID, record.ID)
	}
	if decoded.Name != record.Name {
		t.Errorf("Name mismatch: got %s, want %s", decoded.Name, record.Name)
	}
}

func TestAllTypesEncodeDecode(t *testing.T) {
	layout, err := SchemaFromStruct[TestAllTypes]("alltypes")
	if err != nil {
		t.Fatalf("SchemaFromStruct failed: %v", err)
	}

	record := &TestAllTypes{
		ID:       1,
		IntField: -42,
		Int8:     -8,
		Int16:    -16,
		Int32:    -32,
		Int64:    -64,
		Uint:     100,
		Uint8:    8,
		Uint16:   16,
		Uint32:   32,
		Uint64:   64,
		F32:      3.14,
		F64:      2.718281828,
		Flag:     true,
		Text:     "hello world",
		Stamp:    time.Unix(1700000000, 123456789),
	}

	encoded, err := EncodeRecord(record, layout)
	if err != nil {
		t.Fatalf("EncodeRecord failed: %v", err)
	}

	if len(encoded) == 0 {
		t.Error("encoded data is empty")
	}

	decoded := &TestAllTypes{}
	err = DecodeRecord(encoded, layout, decoded)
	if err != nil {
		t.Fatalf("DecodeRecord failed: %v", err)
	}

	checks := []struct {
		name string
		got  any
		want any
	}{
		{"ID", decoded.ID, record.ID},
		{"IntField", decoded.IntField, int(-42)},
		{"Int8", decoded.Int8, int8(-8)},
		{"Int16", decoded.Int16, int16(-16)},
		{"Int32", decoded.Int32, int32(-32)},
		{"Int64", decoded.Int64, int64(-64)},
		{"Uint", decoded.Uint, uint(100)},
		{"Uint8", decoded.Uint8, uint8(8)},
		{"Uint16", decoded.Uint16, uint16(16)},
		{"Uint32", decoded.Uint32, uint32(32)},
		{"Uint64", decoded.Uint64, uint64(64)},
		{"F32", decoded.F32, float32(3.14)},
		{"F64", decoded.F64, 2.718281828},
		{"Flag", decoded.Flag, true},
		{"Text", decoded.Text, "hello world"},
		{"Stamp", decoded.Stamp, time.Unix(1700000000, 123456789)},
	}

	for _, c := range checks {
		if want, ok := c.want.(float32); ok {
			if c.got.(float32) != want {
				t.Errorf("%s mismatch: got %v, want %v", c.name, c.got, c.want)
			}
		} else if c.got != c.want {
			t.Errorf("%s mismatch: got %v (%T), want %v (%T)", c.name, c.got, c.got, c.want, c.want)
		}
	}
}

func TestServerClient(t *testing.T) {
	tmpFile := "/tmp/netembeddb_test_" + os.Getenv("USER") + ".db"
	os.Remove(tmpFile)
	defer os.Remove(tmpFile)

	sockPath := "/tmp/netembeddb_test_" + os.Getenv("USER") + ".sock"
	os.Remove(sockPath)
	defer os.Remove(sockPath)

	server, err := NewServer(tmpFile)
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}
	defer server.Close()

	err = server.Listen("", sockPath)
	if err != nil {
		t.Fatalf("Listen failed: %v", err)
	}

	client, err := Dial(sockPath)
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	defer client.Close()

	layout, err := SchemaFromStruct[TestUser]("users")
	if err != nil {
		t.Fatalf("SchemaFromStruct failed: %v", err)
	}

	err = client.RegisterSchema("users", layout)
	if err != nil {
		t.Fatalf("RegisterSchema failed: %v", err)
	}

	record := &TestUser{
		ID:    1,
		Name:  "Alice",
		Email: "alice@example.com",
	}

	encoded, err := EncodeRecord(record, layout)
	if err != nil {
		t.Fatalf("EncodeRecord failed: %v", err)
	}

	id, err := client.Insert("users", encoded)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	if id != 1 {
		t.Errorf("expected ID 1, got %d", id)
	}

	data, err := client.Get("users", id)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	decoded := &TestUser{}
	err = DecodeRecord(data, layout, decoded)
	if err != nil {
		t.Fatalf("DecodeRecord failed: %v", err)
	}

	if decoded.Name != "Alice" {
		t.Errorf("expected Name 'Alice', got '%s'", decoded.Name)
	}

	record2 := &TestUser{
		ID:    1,
		Name:  "Bob",
		Email: "bob@example.com",
	}
	encoded2, _ := EncodeRecord(record2, layout)

	err = client.Update("users", id, encoded2)
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	data, err = client.Get("users", id)
	if err != nil {
		t.Fatalf("Get after update failed: %v", err)
	}

	decoded = &TestUser{}
	DecodeRecord(data, layout, decoded)
	if decoded.Name != "Bob" {
		t.Errorf("expected Name 'Bob', got '%s'", decoded.Name)
	}

	count, err := client.Count("users")
	if err != nil {
		t.Fatalf("Count failed: %v", err)
	}
	if count != 1 {
		t.Errorf("expected count 1, got %d", count)
	}

	err = client.Delete("users", id)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	count, err = client.Count("users")
	if err != nil {
		t.Fatalf("Count after delete failed: %v", err)
	}
	if count != 0 {
		t.Errorf("expected count 0, got %d", count)
	}
}

func TestRemoteTable(t *testing.T) {
	tmpFile := "/tmp/netembeddb_remote_" + os.Getenv("USER") + ".db"
	os.Remove(tmpFile)
	defer os.Remove(tmpFile)

	sockPath := "/tmp/netembeddb_remote_" + os.Getenv("USER") + ".sock"
	os.Remove(sockPath)
	defer os.Remove(sockPath)

	server, err := NewServer(tmpFile)
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}
	defer server.Close()

	err = server.Listen("", sockPath)
	if err != nil {
		t.Fatalf("Listen failed: %v", err)
	}

	rdb, err := DialRemote(sockPath)
	if err != nil {
		t.Fatalf("DialRemote failed: %v", err)
	}
	defer rdb.Close()

	products, err := Use[TestProduct](rdb, "products")
	if err != nil {
		t.Fatalf("Use failed: %v", err)
	}

	p1 := &TestProduct{Name: "Widget", Price: 9.99, Stock: 100}
	id1, err := products.Insert(p1)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}
	if id1 != 1 {
		t.Errorf("expected ID 1, got %d", id1)
	}

	p2 := &TestProduct{Name: "Gadget", Price: 19.99, Stock: 50}
	id2, err := products.Insert(p2)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}
	if id2 != 2 {
		t.Errorf("expected ID 2, got %d", id2)
	}

	result, err := products.Get(id1)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if result.Name != "Widget" {
		t.Errorf("expected Name 'Widget', got '%s'", result.Name)
	}

	count, err := products.Count()
	if err != nil {
		t.Fatalf("Count failed: %v", err)
	}
	if count != 2 {
		t.Errorf("expected count 2, got %d", count)
	}

	p1Update := &TestProduct{ID: id1, Name: "SuperWidget", Price: 14.99, Stock: 75}
	err = products.Update(id1, p1Update)
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	result, err = products.Get(id1)
	if err != nil {
		t.Fatalf("Get after update failed: %v", err)
	}
	if result.Name != "SuperWidget" {
		t.Errorf("expected Name 'SuperWidget', got '%s'", result.Name)
	}

	err = products.Delete(id2)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	_, err = products.Get(id2)
	if err == nil {
		t.Error("expected error for deleted record, got nil")
	}

	err = products.Drop()
	if err != nil {
		t.Fatalf("Drop failed: %v", err)
	}
}

func TestStats(t *testing.T) {
	tmpFile := "/tmp/netembeddb_stats_" + os.Getenv("USER") + ".db"
	os.Remove(tmpFile)
	defer os.Remove(tmpFile)

	sockPath := "/tmp/netembeddb_stats_" + os.Getenv("USER") + ".sock"
	os.Remove(sockPath)
	defer os.Remove(sockPath)

	server, err := NewServer(tmpFile)
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}
	defer server.Close()

	err = server.Listen("", sockPath)
	if err != nil {
		t.Fatalf("Listen failed: %v", err)
	}

	rdb, err := DialRemote(sockPath)
	if err != nil {
		t.Fatalf("DialRemote failed: %v", err)
	}
	defer rdb.Close()

	table, err := Use[TestUser](rdb, "stats_users")
	if err != nil {
		t.Fatalf("Use failed: %v", err)
	}

	table.Insert(&TestUser{Name: "Alice", Email: "alice@example.com"})
	table.Insert(&TestUser{Name: "Bob", Email: "bob@example.com"})

	stats, err := rdb.Stats()
	if err != nil {
		t.Fatalf("Stats failed: %v", err)
	}

	found := false
	for _, tbl := range stats.Tables {
		if tbl.Name == "stats_users" {
			found = true
			if tbl.RecordCount != 2 {
				t.Errorf("expected RecordCount 2, got %d", tbl.RecordCount)
			}
		}
	}
	if !found {
		t.Error("stats_users table not found in stats")
	}
	if stats.FileSize == 0 {
		t.Error("expected non-zero FileSize")
	}

	t.Logf("FileSize=%d, IndexKeys=%d, BTreeDepth=%d, Tables=%d",
		stats.FileSize, stats.IndexKeys, stats.BTreeDepth, len(stats.Tables))

	table.Drop()
}
