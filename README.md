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

NetEmbedDB wraps embeddb to provide networked access to an embedded database. The client and server communicate using embeddb's native binary encoding format:

```
[fieldKey:1][valueStartMarker:1][encodedValue...][valueEndMarker:1]
```

Where:
- `fieldKey`: Single byte identifying the field
- `valueStartMarker`: 0x1E (record field value start)
- `valueEndMarker`: 0x1F (record field value end)
- Values encoded as: int→varint, uint→uvarint, float→float64bits→uvarint, string→length+data, bool→byte

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

```go
package main

import (
    "fmt"
    "netembeddb"
)

type User struct {
    Name  string
    Age   int
    Email string
}

func main() {
    client, err := netembeddb.Connect("localhost:8080", "my-secret-key")
    if err != nil {
        fmt.Println("Connection error:", err)
        return
    }
    defer client.Close()
    
    if err := client.CreateTable("users", User{}); err != nil {
        fmt.Println("Create table error:", err)
        return
    }
    
    id, err := client.Insert("users", User{
        Name:  "John Doe",
        Age:   30,
        Email: "john@example.com",
    })
    if err != nil {
        fmt.Println("Insert error:", err)
        return
    }
    fmt.Println("Inserted record with ID:", id)
    
    data, err := client.Get("users", id)
    if err != nil {
        fmt.Println("Get error:", err)
        return
    }
    fmt.Println("Retrieved record data:", data)
    
    if err := client.Update("users", id, User{
        Name:  "Jane Doe",
        Age:   31,
        Email: "jane@example.com",
    }); err != nil {
        fmt.Println("Update error:", err)
        return
    }
    
    count, err := client.Count("users")
    if err != nil {
        fmt.Println("Count error:", err)
        return
    }
    fmt.Println("Total records:", count)

    // Note: Query operations (Query, QueryGT, QueryLT, QueryBetween) require
    // indexes on the server side and are currently placeholders.
    // Use Filter for server-side scanning of records.
    
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
| `OpQuery` | 0x06 | Query records (placeholder) |
| `OpQueryGT` | 0x07 | Query greater than (placeholder) |
| `OpQueryLT` | 0x08 | Query less than (placeholder) |
| `OpQueryBetween` | 0x09 | Query between (placeholder) |
| `OpFilter` | 0x0A | Filter records |
| `OpScan` | 0x0B | Scan all records |
| `OpCount` | 0x0C | Count records in a table |
| `OpClose` | 0x0F | Close connection |
| `OpVacuum` | 0x10 | Vacuum the database |

## Data Types

The protocol supports the following data types using embeddb's encoding:

| Type | Encoding |
|------|----------|
| int, int8, int16, int32, int64 | Varint |
| uint, uint8, uint16, uint32, uint64 | Uvarint |
| float32, float64 | Float64bits → Uvarint |
| string | Length (uvarint) + bytes |
| bool | 1 byte (0 or 1) |

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
