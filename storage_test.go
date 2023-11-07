package main

import (
	"testing"

	"github.com/matryer/is"
)

func TestWriteTableDef(t *testing.T) {
	is := is.New(t)

	def := TableDef{
		Name:    "test",
		Columns: []string{"foo", "bar"},
	}

	dir := t.TempDir()
	store, err := NewStore(dir)
	is.NoErr(err)

	is.NoErr(store.WriteTableDef(&def))
	d2, err := store.ReadTableDef(def.Name)
	is.NoErr(err)

	is.Equal(d2.Name, def.Name)
	is.Equal(d2.Columns, def.Columns)
}

func TestListTableDefs(t *testing.T) {
	is := is.New(t)

	def := TableDef{
		Name:    "test1",
		Columns: []string{"foo", "bar"},
	}

	def2 := TableDef{
		Name:    "test2",
		Columns: []string{"bing", "bong"},
	}

	dir := t.TempDir()
	store, err := NewStore(dir)
	is.NoErr(err)

	is.NoErr(store.WriteTableDef(&def))
	is.NoErr(store.WriteTableDef(&def2))

	list, err := store.ListTableDefs()
	is.NoErr(err)

	is.Equal(len(list), 2)
}

func TestScanRows(t *testing.T) {
	is := is.New(t)

	def := TableDef{
		Name:    "test",
		Columns: []string{"foo", "bar"},
	}

	dir := t.TempDir()
	store, err := NewStore(dir)
	is.NoErr(err)

	is.NoErr(store.WriteTableDef(&def))

	rows := []string{
		"1,2",
		"3,4",
		"5,6",
	}

	for _, r := range rows {
		is.NoErr(store.WriteRow("test", []byte(r)))
	}

	it, err := store.ScanRows("test")
	is.NoErr(err)
	defer it.Close()

	i := 0
	for it.Next() {
		is.Equal(string(it.Value()), rows[i])
		i++
	}

	is.NoErr(store.DeleteTable(def.Name))
	it, err = store.ScanRows(def.Name)
	is.NoErr(err)
	is.Equal(it.Next(), false)
}

func TestTagTables(t *testing.T) {
	is := is.New(t)

	def := TableDef{
		Name:    "test1",
		Columns: []string{"foo", "bar"},
	}

	def2 := TableDef{
		Name:    "test2",
		Columns: []string{"bing", "bong"},
	}

	dir := t.TempDir()
	store, err := NewStore(dir)
	is.NoErr(err)

	is.NoErr(store.WriteTableDef(&def))
	is.NoErr(store.WriteTableDef(&def2))

	is.NoErr(store.TagTable(def.Name, "foo"))
	list, err := store.ListTableDefsByTag("foo")
	is.NoErr(err)
	is.Equal(len(list), 1)
	is.Equal(list[0].Name, def.Name)
}
