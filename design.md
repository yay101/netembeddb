# netembeddb — Design Document

## Architecture

```
┌─────────────────────┐       ┌──────────────────────────┐
│  RemoteDB           │       │  Server                  │
│  RemoteTable[T]     │       │                          │
│  (typed client)     │◄─────►│  embeddb.DB              │
│                     │  TCP/ │  Table[storedRecord]     │
│  Encode/Decode      │  Unix │   ├─ PK index (auto)     │
│  TLV fields         │       │   └─ Secondary indexes   │
│                     │       │      (manually managed)  │
└─────────────────────┘       └──────────────────────────┘
```

## Encoding (Wire Format)

The custom `0x1E`/`0x1F` marker encoding is replaced by embeddbcore's native TLV encoding.
Field payloads on the wire use `embeddbcore.EncodeTLVField(name, value)` for each field,
which is the same format embeddb uses internally.

### Why TLV instead of custom markers

1. **Reuse**: embeddbcore already has battle-tested encode/decode for all field types
2. **Consistency**: Server can parse TLV fields to create secondary index keys without
   needing a separate decode path
3. **Simplicity**: Removes ~200 lines of duplicate field encoding in client.go

### Field value encoding on wire

Each field value is first encoded to bytes using embeddbcore's type-aware functions
(`EncodeVarint`, `EncodeUvarint`, `EncodeString`, `EncodeFloat64`, `EncodeBool`,
`EncodeTime`, `EncodeSlice`, `EncodeBytes`), then wrapped in TLV.

## Protocol

### Frame format

```
[1 byte opcode][4 bytes payload len (big-endian)][N bytes payload]
```

### Response format

```
[1 byte success flag (0/1)][8 bytes data/error len (big-endian)][N bytes data/error]
```

### Data serialization primitives

| Type     | Wire format                                              |
|----------|----------------------------------------------------------|
| string   | [4 bytes len (BE)][N bytes UTF-8]                       |
| uint64   | variable-length (binary.PutUvarint)                      |
| int64    | zigzag-encoded uint64                                    |
| float64  | uint64 bits via math.Float64bits, as uvarint             |
| bool     | [1 byte]                                                 |
| bytes    | [4 bytes len (BE)][N bytes]                             |

### Operation codes

| Code | Operation          | Payload                                 |
|------|--------------------|-----------------------------------------|
| 0x01 | Insert             | tableName + TLV field payload           |
| 0x02 | Get                | tableName + PK (4 bytes BE for uint32)  |
| 0x03 | Update             | tableName + PK + TLV field payload      |
| 0x04 | Delete             | tableName + PK                          |
| 0x05 | Count              | tableName                               |
| 0x06 | RegisterSchema     | tableName + layout metadata             |
| 0x07 | Vacuum             | (none)                                  |
| 0x08 | Sync               | (none)                                  |
| 0x0A | Query              | tableName + fieldName + value           |
| 0x0B | QueryRange         | tableName + fieldName + range params    |
| 0x0C | QueryLike          | tableName + fieldName + pattern         |
| 0x0D | Upsert             | tableName + PK + TLV field payload      |
| 0x0E | InsertMany         | tableName + count + [TLV payloads]      |
| 0x0F | InsertManyBulk     | tableName + count + [TLV payloads]      |
| 0x10 | DeleteMany         | tableName + count + [PKs]               |
| 0x11 | UpdateMany         | tableName + count + [PK + TLV payload]  |
| 0x12 | Scan               | tableName                               |
| 0x13 | All                | tableName                               |
| 0x14 | Drop               | tableName                               |
| 0x15 | CreateIndex        | tableName + fieldName                   |
| 0x16 | DropIndex          | tableName + fieldName                   |
| 0x17 | GetIndexedFields   | tableName                               |
| 0x18 | GetVersion         | tableName + PK + version                |
| 0x19 | ListVersions       | tableName + PK                          |
| 0x1A | Backup             | destPath                                |
| 0x1B | Stats              | (none)                                  |
| 0x1C | Begin              | (none)                                  |
| 0x1D | Commit             | (none)                                  |
| 0x1E | Rollback           | (none)                                  |
| 0xFF | Close              | (none)                                  |

### Range query payload format

Range queries include direction and inclusivity flags:

```
tableName + fieldName + [1 byte flags] + value [+ max_value]
flags: bit 0 = greater-than (1) / less-than (0)
       bit 1 = inclusive (1) / exclusive (0)
       bit 2 = between query (has max_value)
```

### Scan/All response format

Returns a count-prefixed list of records:

```
[4 bytes count (BE)][record1 TLV payload][record2 TLV payload]...
```

## Server Internals

### storedRecord

```go
type storedRecord struct {
    ID   uint32 `db:"id,primary"`
    Data []byte `db:"data"`
}
```

The server uses `storedRecord` as its concrete type for `embeddb.Use[storedRecord]`.
The `Data` field contains TLV-encoded field payloads (not raw struct bytes).

### Secondary index management

Since `storedRecord` has no indexed fields beyond the PK, the server manually
manages secondary indexes by:

1. On insert: decode TLV payload, extract field value strings, create
   `encodeSecondaryKey(tableID, fieldName, value, recordID)` entries
2. On update: delete old secondary keys, insert new ones
3. On delete: delete all secondary keys for the record
4. On query: scan secondary keys with appropriate prefix

### Schema registration

`RegisterSchema` transmits the `StructLayout` (field names and types).
The server stores this to know:
- Which fields are non-primary, non-slice (eligible for auto-indexing)
- Field types for correct index value encoding/decoding
- The primary key type for scan operations

## Client Architecture

### Typed Client (RemoteTable[T])

Uses Go 1.24+ generic methods:

```go
type RemoteDB struct { client *Client }

func (r *RemoteDB) Use[T any](tableName string) (*RemoteTable[T], error) {
    layout, _ := embeddbcore.ComputeStructLayout(*new(T))
    r.client.RegisterSchema(tableName, layout)
    return &RemoteTable[T]{db: r, tableName: tableName, layout: layout}, nil
}
```

`RemoteTable[T]` mirrors `Table[T]`'s API surface exactly. All methods:
1. Encode the record to TLV payload
2. Serialize arguments using protocol primitives
3. Send the operation over the wire
4. Decode the response back to typed values

### Field encoding

The client uses `embeddbcore`'s existing field encoding functions instead of the
current duplicated `getFieldValue`/`setFieldValue`/`encodeFieldValue`:

```go
func encodeRecord(record *T, layout *embeddbcore.StructLayout) ([]byte, error) {
    var buf []byte
    for _, field := range layout.Fields {
        if field.IsStruct && !field.IsTime { continue }
        if field.Primary { continue } // PK handled separately
        val, err := embeddbcore.GetFieldValue(record, field)
        if err != nil { continue }
        encoded := encodeFieldToBytes(val, field.Type)
        buf = embeddbcore.EncodeTLVField(buf, field.Name, encoded)
    }
    return buf, nil
}
```

## Design Decisions

1. **Server stays on `storedRecord`**: No per-client-type compilation. One concrete
   type handles all tables. Secondary indexes managed explicitly using the layout.

2. **TLV on the wire**: Same format as embeddb's internal storage. Single code path
   for encode/decode. Server can parse TLV to extract field values for index creation.

3. **Generic methods for client**: `RemoteTable[T]` gives compile-time type safety
   identical to embeddb's `Table[T]`. The user never touches raw bytes.

4. **Request-response protocol**: Single connection, sequential request/response.
   Future enhancement: multiplexing with request IDs.
