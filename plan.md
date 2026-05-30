# netembeddb — Implementation Plan

## Phase 1: Core Schema-Aware Storage + Protocol

### 1a. Replace custom encoding with embeddbcore TLV
- [x] Replace `EncodeRecord`/`DecodeRecord` in client.go to use `embeddbcore.EncodeTLVField` instead of `0x1E`/`0x1F` markers
- [x] Remove duplicated `getFieldValue`, `setFieldValue`, `encodeFieldValue` from client.go — use `embeddbcore.GetFieldValue` and type-specific encoders
- [x] Wire format for field payloads: TLV-encoded fields where each value is pre-encoded to bytes using embeddbcore type encoders

### 1b. Server schema-aware index management
- [x] Fix `handleRegisterSchema` to fully reconstruct `StructLayout` with field types, PK info, index eligibility
- [x] Add `handleInsert`: decode TLV payload, extract field values, create secondary index keys via `embeddb.EncodeSecondaryKey`
- [x] Add `handleUpdate`: delete old secondary keys, insert new ones
- [x] Add `handleDelete`: delete secondary keys for deleted record
- [ ] Server respects `autoIndex` setting — if disabled, only index explicitly-indexed fields (postponed: requires explicit index API changes in embeddb)

### 1c. Protocol extension
- [x] Add all new OpCodes to protocol/codec.go (Query, QueryRange, Upsert, InsertMany, etc.)
- [x] Add field value serialization helpers: `EncodeFieldValue`/`DecodeFieldValue` that produce type-tagged bytes for any Go scalar/string/bool/time type
- [x] Add batch encoding helpers for multi-record operations

## Phase 2: Queries, Scans, Ranges

### 2a. Query operations
- [x] `handleQuery` (OpQuery): exact-match using secondary index via `embeddb.EncodeSecondaryKeyPrefixWithValue`
- [x] `handleQueryRange` (OpQueryRange): range scan with direction/inclusivity flags
- [x] `handleQueryLike` (OpQueryLike): full scan + LIKE matching
- [x] Client methods: `Query`, `QueryRangeGreaterThan`, `QueryRangeLessThan`, `QueryRangeBetween` (via `QueryRange`), `QueryLike`

### 2b. Scan operations
- [x] `handleScan` (OpScan): iterate primary keys via `t.All()`, return count-prefixed record list
- [x] `handleAll` (OpAll): return all records
- [x] Client methods: `Scan`, `All`

### 2c. Index management
- [x] `handleCreateIndex`: delegates to `t.CreateIndex()`
- [x] `handleDropIndex`: delegates to `t.DropIndex()`
- [x] `handleGetIndexedFields`: delegates to `t.GetIndexedFields()`
- [x] Client methods: `CreateIndex`, `DropIndex`, `GetIndexedFields`
- [x] Added `Table.InsertRawIndex`, `Table.DeleteRawIndex`, `Table.ScanRawIndex`, `Table.GetRawIndex` to embeddb for external index management
- [x] Exported `EncodePrimaryKey`, `EncodeSecondaryKey`, `EncodeSecondaryKeyPrefix`, `EncodeSecondaryKeyPrefixWithValue`, `ParseSecondaryKey` from embeddb

## Phase 3: Typed Client Wrapper

### 3a. RemoteDB
- [x] `RemoteDB` struct wrapping `Client`
- [x] `func Use[T any](r *RemoteDB, tableName ...string) (*RemoteTable[T], error)` — standalone function (Go 1.25 generic methods not yet available)
- [x] Auto-registers schema on first use
- [ ] Connection options (pool size, timeouts) — future

### 3b. RemoteTable[T]
- [x] All CRUD methods: Insert, Get, Update, Delete, Upsert
- [x] Batch methods: InsertMany, InsertManyBulk, DeleteMany
- [x] Query methods: Query, QueryRangeGreaterThan, QueryRangeLessThan, QueryRangeBetween, QueryLike
- [x] Scan methods: All
- [x] Index methods: CreateIndex, DropIndex, GetIndexedFields
- [x] Drop, Count, Name
- [x] Automatic TLV encode/decode, hidden from user

## Phase 4: DB-Level Operations

- [x] `Begin`/`Commit`/`Rollback` transaction support (stub — embeddb transactions need refactoring for network use)
- [x] `Backup` operation
- [x] `Stats` operation
- [x] `Drop` table operation
- [ ] `GetVersion`/`ListVersions` versioning support
- [x] `Upsert` operation

## Phase 5: Polish & Tests

- [x] Remove all duplicated encoding logic in client.go
- [x] Add tests: TestEncodeDecode, TestServerClient, TestRemoteTable
- [ ] Benchmark serialization vs direct embeddb
- [ ] Connection pool with reconnection
- [ ] Timeout/cancellation via context.Context
- [x] Run `go vet`, `go test ./...`

## Architecture Decisions

- **Generic methods**: Go 1.25 does not support type parameters on methods of non-generic types.
  The accepted proposal (golang/go#49085) is not yet implemented. Used standalone
  function `Use[T]()` instead of `RemoteDB.Use[T]()`.

- **Index management**: Added `InsertRawIndex`/`DeleteRawIndex`/`ScanRawIndex`/`GetRawIndex`
  to embeddb's Table for external secondary index management. Exported key encoding
  functions from embeddb's index.go.

- **TLV encoding**: Field values on the wire use `embeddbcore.EncodeTLVField`, the same
  format embeddb uses internally. Values are pre-encoded with type tags using
  `protocol.EncodeFieldValue`.
