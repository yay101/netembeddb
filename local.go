package netembeddb

import (
	"github.com/yay101/embeddb"
	"github.com/yay101/netembeddb/protocol"
)

type DB struct {
	db *embeddb.DB
}

func Open(path string) (*DB, error) {
	db, err := embeddb.Open(path, embeddb.OpenOptions{Migrate: true, AutoIndex: true})
	if err != nil {
		return nil, err
	}
	return &DB{db: db}, nil
}

func (db *DB) Close() error {
	return db.db.Close()
}

func (db *DB) Sync() error {
	return db.db.Sync()
}

func (db *DB) Vacuum() error {
	return db.db.Vacuum()
}

func (db *DB) Use(name string) (*Table, error) {
	table, err := embeddb.Use[Record](db.db, name)
	if err != nil {
		return nil, err
	}
	return &Table{table: table}, nil
}

func (db *DB) CreateTable(name string) error {
	_, err := embeddb.Use[Record](db.db, name)
	return err
}

type Table struct {
	table *embeddb.Table[Record]
}

func (t *Table) Insert(data []byte) (uint32, error) {
	record := Record{Data: data}
	id, err := t.table.Insert(&record)
	return id, err
}

func (t *Table) Get(id uint32) ([]byte, error) {
	record, err := t.table.Get(id)
	if err != nil || record == nil {
		return nil, err
	}
	return record.Data, nil
}

func (t *Table) Update(id uint32, data []byte) error {
	record := Record{Data: data}
	return t.table.Update(id, &record)
}

func (t *Table) Delete(id uint32) error {
	return t.table.Delete(id)
}

func (t *Table) Query(field string, value interface{}) ([]uint32, error) {
	results, err := t.table.Query(field, value)
	if err != nil {
		return nil, err
	}
	ids := make([]uint32, len(results))
	for i, rec := range results {
		ids[i] = rec.ID
	}
	return ids, nil
}

func (t *Table) QueryPaged(field string, value interface{}, offset, limit int) (*PagedResult, error) {
	result, err := t.table.QueryPaged(field, value, offset, limit)
	if err != nil {
		return nil, err
	}
	return &PagedResult{
		TotalCount: uint32(result.TotalCount),
		HasMore:    result.HasMore,
		Records:    result.Records,
	}, nil
}

func (t *Table) QueryRangeGreaterThan(field string, min interface{}, inclusive bool) ([]uint32, error) {
	results, err := t.table.QueryRangeGreaterThan(field, min, inclusive)
	if err != nil {
		return nil, err
	}
	ids := make([]uint32, len(results))
	for i, rec := range results {
		ids[i] = rec.ID
	}
	return ids, nil
}

func (t *Table) QueryRangeLessThan(field string, max interface{}, inclusive bool) ([]uint32, error) {
	results, err := t.table.QueryRangeLessThan(field, max, inclusive)
	if err != nil {
		return nil, err
	}
	ids := make([]uint32, len(results))
	for i, rec := range results {
		ids[i] = rec.ID
	}
	return ids, nil
}

func (t *Table) QueryRangeBetween(field string, min, max interface{}, inclusiveMin, inclusiveMax bool) ([]uint32, error) {
	results, err := t.table.QueryRangeBetween(field, min, max, inclusiveMin, inclusiveMax)
	if err != nil {
		return nil, err
	}
	ids := make([]uint32, len(results))
	for i, rec := range results {
		ids[i] = rec.ID
	}
	return ids, nil
}

func (t *Table) Filter(op protocol.FilterOperator, field string, value interface{}) ([]uint32, error) {
	results, err := t.table.Query(field, value)
	if err != nil {
		return nil, err
	}
	ids := make([]uint32, 0)
	for _, rec := range results {
		data := rec.Data
		fieldVal := decodeValue(data)
		if compareValues(fieldVal, value, op) {
			ids = append(ids, rec.ID)
		}
	}
	return ids, nil
}

func (t *Table) FilterPaged(op protocol.FilterOperator, field string, value interface{}, offset, limit int) (*PagedResult, error) {
	result, err := t.table.QueryPaged(field, value, offset, limit)
	if err != nil {
		return nil, err
	}
	filtered := make([]Record, 0)
	for _, rec := range result.Records {
		data := rec.Data
		fieldVal := decodeValue(data)
		if compareValues(fieldVal, value, op) {
			filtered = append(filtered, rec)
		}
	}
	return &PagedResult{
		TotalCount: uint32(result.TotalCount),
		HasMore:    result.HasMore,
		Records:    filtered,
	}, nil
}

func (t *Table) Scan(fn func([]byte) bool) error {
	return t.table.Scan(func(rec Record) bool {
		return fn(rec.Data)
	})
}

func (t *Table) Count() int {
	return t.table.Count()
}

func (t *Table) CreateIndex(field string) error {
	return t.table.CreateIndex(field)
}

func (t *Table) DropIndex(field string) error {
	return t.table.DropIndex(field)
}

type PagedResult struct {
	TotalCount uint32
	HasMore    bool
	Records    []Record
}
