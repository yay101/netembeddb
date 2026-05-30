package netembeddb

import (
	"fmt"
	"reflect"

	embedcore "github.com/yay101/embeddbcore"
	"github.com/yay101/netembeddb/protocol"
)

type RemoteDB struct {
	client *Client
}

func DialRemote(addr string, opts ...ClientOptions) (*RemoteDB, error) {
	client, err := Dial(addr, opts...)
	if err != nil {
		return nil, err
	}
	return &RemoteDB{client: client}, nil
}

func (r *RemoteDB) Close() error {
	return r.client.Close()
}

func resolveTypeName[T any]() string {
	var instance T
	t := reflect.TypeOf(instance)
	if t == nil {
		return ""
	}
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Name() != "" {
		return t.Name()
	}
	return t.String()
}

func Use[T any](r *RemoteDB, tableName ...string) (*RemoteTable[T], error) {
	layout, err := SchemaFromStruct[T]("")
	if err != nil {
		return nil, fmt.Errorf("schema: %w", err)
	}

	var name string
	if len(tableName) > 0 {
		name = tableName[0]
	}
	if name == "" {
		name = resolveTypeName[T]()
	}
	if name == "" {
		return nil, fmt.Errorf("could not resolve table name")
	}

	if err := r.client.RegisterSchema(name, layout); err != nil {
		return nil, fmt.Errorf("register schema: %w", err)
	}

	return &RemoteTable[T]{
		remote:    r,
		tableName: name,
		layout:    layout,
	}, nil
}

func (r *RemoteDB) Vacuum() error {
	return r.client.Vacuum()
}

func (r *RemoteDB) Sync() error {
	return r.client.Sync()
}

func (r *RemoteDB) Begin() error {
	return r.client.Begin()
}

func (r *RemoteDB) Commit() error {
	return r.client.Commit()
}

func (r *RemoteDB) Rollback() error {
	return r.client.Rollback()
}

func (r *RemoteDB) Backup(destPath string) error {
	return r.client.Backup(destPath)
}

type RemoteTable[T any] struct {
	remote    *RemoteDB
	tableName string
	layout    *embedcore.StructLayout
}

func (rt *RemoteTable[T]) Name() string {
	return rt.tableName
}

func (rt *RemoteTable[T]) Drop() error {
	return rt.remote.client.Drop(rt.tableName)
}

func (rt *RemoteTable[T]) Insert(record *T) (uint32, error) {
	encoded, err := encodeTLVRecord(record, rt.layout)
	if err != nil {
		return 0, fmt.Errorf("encode: %w", err)
	}
	return rt.remote.client.Insert(rt.tableName, encoded)
}

func (rt *RemoteTable[T]) Get(id uint32) (*T, error) {
	data, err := rt.remote.client.Get(rt.tableName, id)
	if err != nil {
		return nil, err
	}
	var result T
	if err := decodeTLVRecord(data, rt.layout, &result); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	return &result, nil
}

func (rt *RemoteTable[T]) Update(id uint32, record *T) error {
	encoded, err := encodeTLVRecord(record, rt.layout)
	if err != nil {
		return fmt.Errorf("encode: %w", err)
	}
	return rt.remote.client.Update(rt.tableName, id, encoded)
}

func (rt *RemoteTable[T]) Delete(id uint32) error {
	return rt.remote.client.Delete(rt.tableName, id)
}

func (rt *RemoteTable[T]) Upsert(id uint32, record *T) (uint32, bool, error) {
	encoded, err := encodeTLVRecord(record, rt.layout)
	if err != nil {
		return 0, false, fmt.Errorf("encode: %w", err)
	}
	return rt.remote.client.Upsert(rt.tableName, id, encoded)
}

func (rt *RemoteTable[T]) InsertMany(records []*T) ([]uint32, error) {
	encoded := make([][]byte, len(records))
	for i, rec := range records {
		var err error
		encoded[i], err = encodeTLVRecord(rec, rt.layout)
		if err != nil {
			return nil, fmt.Errorf("encode record %d: %w", i, err)
		}
	}
	return rt.remote.client.InsertMany(rt.tableName, encoded)
}

func (rt *RemoteTable[T]) InsertManyBulk(records []*T) ([]uint32, error) {
	encoded := make([][]byte, len(records))
	for i, rec := range records {
		var err error
		encoded[i], err = encodeTLVRecord(rec, rt.layout)
		if err != nil {
			return nil, fmt.Errorf("encode record %d: %w", i, err)
		}
	}
	return rt.remote.client.InsertManyBulk(rt.tableName, encoded)
}

func (rt *RemoteTable[T]) DeleteMany(ids []uint32) (int, error) {
	return rt.remote.client.DeleteMany(rt.tableName, ids)
}

func (rt *RemoteTable[T]) Count() (uint64, error) {
	return rt.remote.client.Count(rt.tableName)
}

func (rt *RemoteTable[T]) Query(fieldName string, value any) ([]T, error) {
	raw, err := rt.remote.client.Query(rt.tableName, fieldName, value)
	if err != nil {
		return nil, err
	}
	return rt.decodeResults(raw)
}

func (rt *RemoteTable[T]) QueryRangeGreaterThan(fieldName string, value any, inclusive bool) ([]T, error) {
	raw, err := rt.remote.client.QueryRange(rt.tableName, fieldName, "gt", value, inclusive, nil)
	if err != nil {
		return nil, err
	}
	return rt.decodeResults(raw)
}

func (rt *RemoteTable[T]) QueryRangeLessThan(fieldName string, value any, inclusive bool) ([]T, error) {
	raw, err := rt.remote.client.QueryRange(rt.tableName, fieldName, "lt", value, inclusive, nil)
	if err != nil {
		return nil, err
	}
	return rt.decodeResults(raw)
}

func (rt *RemoteTable[T]) QueryRangeBetween(fieldName string, min, max any, inclusiveMin, inclusiveMax bool) ([]T, error) {
	raw, err := rt.remote.client.QueryRange(rt.tableName, fieldName, "btwn", min, inclusiveMin, max)
	if err != nil {
		return nil, err
	}
	return rt.decodeResults(raw)
}

func (rt *RemoteTable[T]) QueryLike(fieldName string, pattern string) ([]T, error) {
	raw, err := rt.remote.client.QueryLike(rt.tableName, fieldName, pattern)
	if err != nil {
		return nil, err
	}
	return rt.decodeResults(raw)
}

func (rt *RemoteTable[T]) All() ([]T, error) {
	raw, err := rt.remote.client.All(rt.tableName)
	if err != nil {
		return nil, err
	}
	return rt.decodeResults(raw)
}

func (rt *RemoteTable[T]) CreateIndex(fieldName string) error {
	return rt.remote.client.CreateIndex(rt.tableName, fieldName)
}

func (rt *RemoteTable[T]) DropIndex(fieldName string) error {
	return rt.remote.client.DropIndex(rt.tableName, fieldName)
}

func (rt *RemoteTable[T]) GetIndexedFields() ([]string, error) {
	return rt.remote.client.GetIndexedFields(rt.tableName)
}

func (rt *RemoteTable[T]) decodeResults(raw [][]byte) ([]T, error) {
	results := make([]T, 0, len(raw))
	for _, data := range raw {
		var result T
		if err := decodeTLVRecord(data, rt.layout, &result); err != nil {
			continue
		}
		results = append(results, result)
	}
	if results == nil {
		results = []T{}
	}
	return results, nil
}

func (rt *RemoteTable[T]) RemoteEncode(record *T) ([]byte, error) {
	return encodeTLVRecord(record, rt.layout)
}

func (rt *RemoteTable[T]) RemoteDecode(data []byte) (*T, error) {
	var result T
	if err := decodeTLVRecord(data, rt.layout, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (rt *RemoteTable[T]) EncodeFieldValue(fieldName string, record *T) ([]byte, error) {
	encoded, err := encodeTLVRecord(record, rt.layout)
	if err != nil {
		return nil, err
	}
	remaining := encoded
	for len(remaining) > 0 {
		name, value, rem, err := embedcore.DecodeTLVField(remaining)
		if err != nil {
			break
		}
		remaining = rem
		if name == fieldName {
			return value, nil
		}
	}
	return nil, fmt.Errorf("field %s not found", fieldName)
}

func (rt *RemoteTable[T]) EncodeFieldValueString(fieldName string, val any) ([]byte, error) {
	for _, field := range rt.layout.Fields {
		if field.Name == fieldName {
			return protocol.EncodeFieldValue(val, field.Type)
		}
	}
	return nil, fmt.Errorf("field %s not found", fieldName)
}
