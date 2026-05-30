# netembeddb

Network-embedded database. Makes [embeddb](https://github.com/yay101/embeddb)'s typed `Table[T]` API work over TCP and Unix sockets.

```
Client (typed Go structs) ──TLV wire──► Server ──► embeddb DB
```

## Quick Start

```go
// Server
server, _ := netembeddb.NewServer("my.db")
server.Listen("", "/tmp/mydb.sock")
defer server.Close()

// Client
rdb, _ := netembeddb.DialRemote("/tmp/mydb.sock")
defer rdb.Close()

type User struct {
    ID    uint32 `db:"id,primary"`
    Name  string `db:"name"`
    Email string `db:"email"`
}

// Use[T]() registers the schema and returns a typed handler.
users, _ := netembeddb.Use[User](rdb, "users")

// CRUD — types match embeddb's Table[T] API.
id, _ := users.Insert(&User{Name: "Alice", Email: "alice@example.com"})
user, _ := users.Get(id)
users.Update(id, &User{Name: "Bob", Email: "bob@example.com"})
users.Delete(id)

// Queries use secondary indexes automatically.
results, _ := users.Query("Name", "Alice")
gtResults, _ := users.QueryRangeGreaterThan("ID", 5, false)

// Batch operations.
ids, _ := users.InsertMany([]*User{{Name: "Eve"}, {Name: "Dan"}})
users.DeleteMany(ids)

// Index management.
users.CreateIndex("Email")
users.DropIndex("Email")

// Drop the table.
users.Drop()
```

## API

### `RemoteDB`
| Method | Description |
|--------|-------------|
| `DialRemote(addr)` | Connect to server (TCP or Unix socket) |
| `Close()` | Close connection |
| `Vacuum()` | Compact database file |
| `Sync()` | Force flush to disk |
| `Begin()` / `Commit()` / `Rollback()` | Transaction control |
| `Backup(destPath)` | Create a backup |

### `RemoteTable[T]`
| Method | Description |
|--------|-------------|
| `Name()` | Table name |
| `Insert(record)` → `(uint32, error)` | Insert, returns auto-assigned PK |
| `Get(id)` → `(*T, error)` | Fetch by primary key |
| `Update(id, record)` | Replace record |
| `Delete(id)` | Remove record |
| `Upsert(id, record)` | Insert or update |
| `Count()` → `(uint64, error)` | Row count |
| `Drop()` | Drop table |
| `InsertMany([]*T)` | Batch insert |
| `InsertManyBulk([]*T)` | Bulk insert (single B-tree rebuild) |
| `DeleteMany([]uint32)` | Batch delete |
| `Query(field, value)` → `([]T, error)` | Exact match |
| `QueryRangeGreaterThan(field, value, inclusive)` → `([]T, error)` | GT range |
| `QueryRangeLessThan(field, value, inclusive)` → `([]T, error)` | LT range |
| `QueryRangeBetween(field, min, max, inclMin, inclMax)` → `([]T, error)` | Between range |
| `QueryLike(field, pattern)` → `([]T, error)` | SQL LIKE matching |
| `All()` → `([]T, error)` | All records |
| `CreateIndex(field)` | Build secondary index |
| `DropIndex(field)` | Remove secondary index |
| `GetIndexedFields()` → `([]string, error)` | List indexed fields |

### Low-level `Client`
For users who want raw byte-level control:

| Method | Description |
|--------|-------------|
| `Dial(addr)` | Raw connection |
| `RegisterSchema(name, layout)` | Register struct layout |
| `Insert(name, data)` / `Get(name, id)` / `Update(name, id, data)` / `Delete(name, id)` | Raw CRUD |
| `Query(name, field, value)` / `QueryRange(...)` / `QueryLike(...)` / `Scan(name)` | Raw queries |
| `CreateIndex(name, field)` / `DropIndex(name, field)` | Raw index ops |
| `EncodeRecord(record, layout)` / `DecodeRecord(data, layout, result)` | Encode/decode helpers |

## Supported Field Types

`int`, `int8-64`, `uint`, `uint8-64`, `float32`, `float64`, `bool`, `string`, `time.Time`

## Architecture

The server stores records as opaque TLV-encoded field payloads inside embeddb's native record format. Secondary indexes are managed through embeddb's B-tree via the raw index API (`InsertRawIndex`/`DeleteRawIndex`/`ScanRawIndex`). This avoids per-client-type compilation on the server — there's a single `storedRecord` type for all tables.

Field encoding on the wire uses `embeddbcore.EncodeTLVField`, the same TLV format embeddb uses internally. Values are prefixed with type tags for correct decode without type information.

## Wire Protocol

| Op | Code | Description |
|----|------|-------------|
| Insert | `0x01` | tableName + TLV payload |
| Get | `0x02` | tableName + PK |
| Update | `0x03` | tableName + PK + TLV payload |
| Delete | `0x04` | tableName + PK |
| Count | `0x05` | tableName |
| RegisterSchema | `0x06` | tableName + layout |
| Query | `0x0A` | tableName + fieldName + value |
| QueryRange | `0x0B` | tableName + fieldName + flags + value [+max] |
| QueryLike | `0x0C` | tableName + fieldName + pattern |
| Upsert | `0x0D` | tableName + PK + TLV payload |
| InsertMany | `0x0E` | tableName + count + [TLV payloads] |
| Scan/All | `0x12/0x13` | tableName |
| Drop | `0x14` | tableName |
| CreateIndex/DropIndex | `0x15/0x16` | tableName + fieldName |
| Begin/Commit/Rollback | `0x1C-0x1E` | (none) |
| Backup | `0x1A` | destPath |
| Close | `0xFF` | (none) |

Frame: `[1 byte op][4 bytes payload len BE][N bytes payload]`

## Requirements

- Go 1.25+
- `embeddb` v1.11.0+
- `embeddbcore` v0.5.5+
