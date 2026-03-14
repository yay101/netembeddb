# NetEmbedDB

A networked embedded database server written in Go, built on top of [embeddb](https://github.com/yay101/embeddb). NetEmbedDB provides a client-server architecture with full CRUD operations, accessible over TCP or Unix sockets.

## Features

- **Networked Architecture**: Client-server model supporting TCP and Unix socket connections
- **Authentication**: Secure key-based authentication for client connections
- **Persistence**: Uses embeddb for efficient embedded storage
- **Binary Protocol**: Uses embeddb's native binary encoding for efficient network communication
- **Full CRUD**: Create, Read, Update, Delete operations
- **Query Support**: Filter, Scan, and Count operations
- **Type Support**: Integers, unsigned integers, floats, strings, booleans

## Architecture

NetEmbedDB wraps embeddb to provide networked access to an embedded database. Records are stored as raw JSON bytes in embeddb, enabling flexible schema-less storage.

The client and server communicate using embeddb's native binary encoding format for primitives:

| Type | Encoding |
|------|----------|
| int, int8, int16, int32, int64 | Varint |
| uint, uint8, uint16, uint32, uint64 | Uvarint |
| float32, float64 | Float64bits → Uvarint |
| string | Length (uvarint) + bytes |
| bool | 1 byte (0 or 1) |

**Data Storage**: Records are stored as JSON bytes (`RawRecord{Data []byte}`). This allows storing arbitrary data without requiring predefined struct schemas.

## Installation

### Prerequisites

- Go 1.24.1 or later
- embeddb (automatically fetched as dependency)

### Build

```bash
cd netembeddb
go build -o netembeddb ./...
```

## Usage

### Starting the Server

```go
package main

import (
    "fmt"
    "netembeddb"
)

func main() {
    server := netembeddb.NewServer("./data", "my-secret-key")
    
    if err := server.Listen(":8080"); err != nil {
        fmt.Println("Server error:", err)
        return
    }
    
    select {}
}
```

Or use a Unix socket:

```go
if err := server.Listen("/tmp/netembeddb.sock"); err != nil {
    fmt.Println("Server error:", err)
    return
}
```

### Connecting as a Client

NetEmbedDB stores records as JSON bytes, so you can pass `[]byte` or `string` directly, or use any struct that can be JSON-marshaled.

```go
package main

import (
    "encoding/json"
    "fmt"
    "netembeddb"
)

func main() {
    client, err := netembeddb.Connect("localhost:8080", "my-secret-key")
    if err != nil {
        fmt.Println("Connection error:", err)
        return
    }
    defer client.Close()
    
    // Create table (schema parameter is ignored, using JSON storage)
    if err := client.CreateTable("users", nil); err != nil {
        fmt.Println("Create table error:", err)
        return
    }
    
    // Insert using JSON
    user := map[string]any{
        "name":  "John Doe",
        "age":   30,
        "email": "john@example.com",
    }
    userData, _ := json.Marshal(user)
    
    id, err := client.Insert("users", userData)
    if err != nil {
        fmt.Println("Insert error:", err)
        return
    }
    fmt.Println("Inserted record with ID:", id)
    
    // Or use string directly
    id2, _ := client.Insert("users", []byte(`{"name":"Jane","age":25}`))
    fmt.Println("Inserted record 2 with ID:", id2)
    
    // Get record
    data, err := client.Get("users", id)
    if err != nil {
        fmt.Println("Get error:", err)
        return
    }
    fmt.Println("Retrieved record:", string(data))
    
    // Update record
    user["age"] = 31
    userData, _ = json.Marshal(user)
    if err := client.Update("users", id, userData); err != nil {
        fmt.Println("Update error:", err)
        return
    }
    
    // Count records
    count, err := client.Count("users")
    if err != nil {
        fmt.Println("Count error:", err)
        return
    }
    fmt.Println("Total records:", count)
    
    // Query by exact field match
    ids, err := client.Query("users", "name", "John Doe")
    if err != nil {
        fmt.Println("Query error:", err)
    } else {
        fmt.Println("Users named John Doe:", ids)
    }

    // Query records greater than (age > 25)
    ids, err = client.QueryGT("users", "age", 25, false)
    if err != nil {
        fmt.Println("QueryGT error:", err)
    } else {
        fmt.Println("Users older than 25:", ids)
    }

    // Query records less than (age < 30)
    ids, err = client.QueryLT("users", "age", 30, true) // inclusive
    if err != nil {
        fmt.Println("QueryLT error:", err)
    } else {
        fmt.Println("Users age <= 30:", ids)
    }

    // Query records between values
    ids, err = client.QueryBetween("users", "age", 20, 40, true, true)
    if err != nil {
        fmt.Println("QueryBetween error:", err)
    } else {
        fmt.Println("Users age 20-40:", ids)
    }
    
    // Delete record
    if err := client.Delete("users", id); err != nil {
        fmt.Println("Delete error:", err)
        return
    }
}
```

## Protocol Operations

| Operation | Code | Description |
|-----------|------|-------------|
| `OpRegister` | 0x01 | Register a table schema |
| `OpCreateTable` | 0x0E | Create a new table |
| `OpListTables` | 0x0D | List all tables |
| `OpInsert` | 0x02 | Insert a new record |
| `OpGet` | 0x03 | Get a record by ID |
| `OpUpdate` | 0x04 | Update an existing record |
| `OpDelete` | 0x05 | Delete a record by ID |
| `OpQuery` | 0x06 | Query records by exact field match |
| `OpQueryGT` | 0x07 | Query records greater than value |
| `OpQueryLT` | 0x08 | Query records less than value |
| `OpQueryBetween` | 0x09 | Query records between two values |
| `OpFilter` | 0x0A | Filter records |
| `OpScan` | 0x0B | Scan all records |
| `OpCount` | 0x0C | Count records in a table |
| `OpClose` | 0x0F | Close connection |
| `OpVacuum` | 0x10 | Vacuum the database |

## Project Structure

```
netembeddb/
├── auth.go          # Authentication logic
├── client.go        # Client implementation using embeddb encoding
├── protocol.go      # Protocol definitions and encoding helpers
├── server.go        # Server implementation
├── protocol_test.go # Protocol tests
├── go.mod           # Go module definition
└── README.md        # This file
```

## Dependencies

- [embeddb](https://github.com/yay101/embeddb) - Embedded database library

## License

MIT License
