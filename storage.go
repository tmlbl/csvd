package main

import (
	"encoding/json"
	"fmt"

	"github.com/dgraph-io/badger/v4"
)

type TableDef struct {
	Name    string
	Columns []string
}

type Store struct {
	db *badger.DB
}

func NewStore(dir string) (*Store, error) {
	opts := badger.DefaultOptions(dir)
	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("opening badger DB: %w", err)
	}
	return &Store{
		db: db,
	}, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func tableDefKey(name string) []byte {
	return []byte(fmt.Sprintf("tabledef:%s", name))
}

func (s *Store) WriteTableDef(def *TableDef) error {
	key := tableDefKey(def.Name)
	value, err := json.Marshal(def)
	if err != nil {
		return err
	}
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	})
}

func (s *Store) ReadTableDef(name string) (*TableDef, error) {
	key := tableDefKey(name)
	var def TableDef
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &def)
		})
	})
	if err != nil {
		return nil, err
	}
	return &def, nil
}

func (s *Store) ListTableDefs() ([]TableDef, error) {
	defs := []TableDef{}
	err := s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		prefix := tableDefKey("")
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			var def TableDef
			item := it.Item()
			err := item.Value(func(val []byte) error {
				return json.Unmarshal(val, &def)
			})
			if err != nil {
				return err
			}
			defs = append(defs, def)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return defs, nil
}

func rowKey(table, pkey string) []byte {
	return []byte(fmt.Sprintf("row:%s:%s", table, pkey))
}

func (s *Store) WriteRow(table string, data []byte) error {
	var pix = 0
	for i, b := range data {
		if b == ',' {
			pix = i
			break
		}
	}
	if pix < 1 {
		return fmt.Errorf("could not find primary key")
	}

	pkey := string(data[:pix])
	key := rowKey(table, pkey)

	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, data)
	})
}

type RowIter struct {
	prefix   []byte
	txn      *badger.Txn
	it       *badger.Iterator
	notFirst bool
}

func (r *RowIter) Next() bool {
	if r.notFirst {
		r.it.Next()
	} else {
		r.notFirst = true
	}
	return r.it.ValidForPrefix(r.prefix)
}

func (r *RowIter) Value() []byte {
	var val []byte
	item := r.it.Item()
	err := item.Value(func(v []byte) error {
		val = v
		return nil
	})
	if err != nil {
		panic(err)
	}
	return val
}

func (r *RowIter) Close() {
	r.it.Close()
	r.txn.Discard()
}

func (s *Store) ScanRows(table string) (*RowIter, error) {
	var ri RowIter
	txn := s.db.NewTransaction(false)

	it := txn.NewIterator(badger.DefaultIteratorOptions)
	prefix := rowKey(table, "")
	it.Seek(prefix)
	ri = RowIter{
		prefix: prefix,
		txn:    txn,
		it:     it,
	}

	return &ri, nil
}
