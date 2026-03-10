# NetEmbedDB

A lightweight, networked embedded database server written in Go. NetEmbedDB provides a simple key-value store with table-based data organization, accessible over TCP or Unix sockets.

## Features

- **Networked Architecture**: Client-server model supporting TCP and Unix socket connections
- **Authentication**: Secure key-based authentication for client connections
- **Table Management**: Create, list, and manage multiple tables
- **CRUD Operations**: Full Create, Read, Update, Delete functionality
- **Query Support**: Equality, range (GT/LT), and range-between queries
- **Type Support**: Integers, unsigned integers, floats, strings, booleans, times, and slices
- **Binary Protocol**: Efficient binary encoding/decoding for network communication

## Installation

### Prerequisites

- Go 1.24.1 or later

### Clone and Build

```bash
git clone <repository-url>
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
    // Create a new server with data directory and authentication key
    server := netembeddb.NewServer("./data", "my-secret-key")
    
    // Start listening on a TCP address
    if err := server.Listen(":8080"); err != nil {
        fmt.Println("Server error:", err)
        return
    }
    
    // Or use a Unix socket
    // if err := server.Listen("/tmp/netembeddb.sock"); err != nil {
    //     fmt.Println("Server error:", err)
    //     return
    // }
    
    // Keep the server running
    select {}
}
```

### Connecting as a Client

```go
package main

import (
    "fmt"
    "netembeddb"
)

type MyRecord struct {
    Name  string
    Age   int
    Email string
}

func main() {
    // Connect to the server
    client, err := netembeddb.Connect("localhost:8080", "my-secret-key")
    if err != nil {
        fmt.Println("Connection error:", err)
        return
    }
    defer client.Close()
    
    // Create a table
    if err := client.CreateTable("users", MyRecord{}); err != nil {
        fmt.Println("Create table error:", err)
        return
    }
    
    // Insert a record
    id, err := client.Insert("users", MyRecord{
        Name:  "John Doe",
        Age:   30,
        Email: "john@example.com",
    })
    if err != nil {
        fmt.Println("Insert error:", err)
        return
    }
    fmt.Println("Inserted record with ID:", id)
    
    // Get a record
    data, err := client.Get("users", id)
    if err != nil {
        fmt.Println("Get error:", err)
        return
    }
    fmt.Println("Retrieved record:", string(data))
    
    // Update a record
    if err := client.Update("users", id, MyRecord{
        Name:  "Jane Doe",
        Age:   31,
        Email: "jane@example.com",
    }); err != nil {
        fmt.Println("Update error:", err)
        return
    }
    
    // Query records
    ids, err := client.Query("users", "Name", "Jane Doe")
    if err != nil {
        fmt.Println("Query error:", err)
        return
    }
    fmt.Println("Found", len(ids), "records")
    
    // Count records
    count, err := client.Count("users")
    if err != nil {
        fmt.Println("Count error:", err)
        return
    }
    fmt.Println("Total records:", count)
    
    // List all tables
    tables, err := client.ListTables()
    if err != nil {
        fmt.Println("List tables error:", err)
        return
    }
    fmt.Println("Tables:", tables)
    
    // Delete a record
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
| `OpQuery` | 0x06 | Query records by field value |
| `OpQueryGT` | 0x07 | Query records greater than value |
| `OpQueryLT` | 0x08 | Query records less than value |
| `OpQueryBetween` | 0x09 | Query records between two values |
| `OpCount` | 0x0C | Count records in a table |
| `OpClose` | 0x0F | Close connection |

## Data Types

The protocol supports the following data types:

| Marker | Type | Description |
|--------|------|-------------|
| `i` | Int | Signed integers (int, int8, int16, int32, int64) |
| `u` | Uint | Unsigned integers (uint, uint8, uint16, uint32, uint64) |
| `f` | Float | Floating point (float32, float64) |
| `s` | String | UTF-8 strings |
| `b` | Bool | Boolean values |
| `t` | Time | Time values (Unix nanoseconds) |
| `l` | Slice | Slices of supported types |
| `B` | Bytes | Raw bytes |

## Project Structure

```
netembeddb/
├── auth.go          # Authentication logic
├── client.go        # Client implementation
├── protocol.go      # Protocol definitions and encoding
├── server.go        # Server implementation
├── go.mod           # Go module definition
├── go.sum           # Go dependencies
└── README.md        # This file
```

## License

This project is licensed under the MIT License.