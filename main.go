package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
)

func main() {
	dir := flag.String("d", "/tmp/csvd", "database directory")
	flag.Parse()

	r := chi.NewRouter()
	csvd, err := NewCSVD(*dir)
	if err != nil {
		log.Fatalln(err)
	}

	r.Get("/tables", csvd.handleListTables)
	r.Post("/tables/{name}", csvd.handlePostData)
	r.Get("/tables/{name}", csvd.handleReadRows)
	r.Delete("/tables/{name}", csvd.handleDeleteData)

	r.Post("/tables/{table}/tags/{tag}", csvd.handleTagTable)
	r.Get("/tags", csvd.handleListTags)

	http.ListenAndServe(":3737", r)
}

type CSVD struct {
	store *Store
}

func NewCSVD(dir string) (*CSVD, error) {
	log.Printf("opening data directory %s\n", dir)
	store, err := NewStore(dir)
	if err != nil {
		return nil, err
	}

	return &CSVD{store}, nil
}

func (c *CSVD) handlePostData(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")

	s := bufio.NewScanner(r.Body)
	if !s.Scan() {
		w.WriteHeader(400)
		w.Write([]byte("could not read header row"))
		return
	}

	def, err := c.store.ReadTableDef(name)
	if err != nil {
		header := s.Bytes()
		columns := strings.Split(string(header), ",")
		if len(columns) < 1 {
			w.WriteHeader(400)
			w.Write([]byte("no columns in header row"))
			return
		}
		// cannot contain | in column name
		for _, c := range columns {
			if strings.Contains(c, "|") {
				w.WriteHeader(400)
				w.Write([]byte("column name cannot contain |"))
				return
			}
		}
		def = &TableDef{
			Name:    name,
			Columns: columns,
		}
		err = c.store.WriteTableDef(def)
		if err != nil {
			w.WriteHeader(500)
			w.Write([]byte(
				fmt.Sprintf("writing table def: %s", err),
			))
			return
		}
	}

	for s.Scan() {
		err = c.store.WriteRow(name, s.Bytes())
		if err != nil {
			w.WriteHeader(500)
			w.Write([]byte(
				fmt.Sprintf("writing row: %s", err),
			))
			return
		}
	}
}

func (c *CSVD) handleReadRows(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")

	def, err := c.store.ReadTableDef(name)
	if err != nil {
		w.WriteHeader(404)
		w.Write([]byte(err.Error()))
		return
	}

	it, err := c.store.ScanRows(name)
	if err != nil {
		w.WriteHeader(500)
		w.Write([]byte(err.Error()))
		return
	}

	w.Write([]byte(strings.Join(def.Columns, ",")))
	w.Write([]byte{'\n'})

	for it.Next() {
		val := it.Value()
		if val != nil {
			w.Write(it.Value())
			w.Write([]byte{'\n'})
		}
	}
}

func (c *CSVD) handleListTables(w http.ResponseWriter, r *http.Request) {
	var defs []TableDef
	var err error

	tag := r.URL.Query().Get("tag")
	if tag != "" {
		defs, err = c.store.ListTableDefsByTag(tag)
	} else {
		defs, err = c.store.ListTableDefs()
	}
	if err != nil {
		w.WriteHeader(500)
		w.Write([]byte(
			fmt.Sprintf("listing table defs: %s", err),
		))
		return
	}
	w.Header().Add("Content-Type", "text/csv")
	w.Write([]byte("name,columns\n"))

	for _, d := range defs {
		columns := strings.Join(d.Columns, "|")
		w.Write([]byte(fmt.Sprintf("%s,%s\n", d.Name, columns)))
	}
}

func (c *CSVD) handleTagTable(w http.ResponseWriter, r *http.Request) {
	table := chi.URLParam(r, "table")
	tag := chi.URLParam(r, "tag")

	err := c.store.TagTable(table, tag)
	if err != nil {
		w.WriteHeader(500)
		w.Write([]byte(
			fmt.Sprintf("tagging table: %s", err),
		))
		return
	}
}

func (c *CSVD) handleListTags(w http.ResponseWriter, r *http.Request) {
	infos, err := c.store.GetTagInfo()
	if err != nil {
		w.WriteHeader(500)
		w.Write([]byte(
			fmt.Sprintf("tagging table: %s", err),
		))
		return
	}
	w.Header().Add("Content-Type", "text/csv")
	w.Write([]byte("name,n_tables\n"))

	for _, info := range infos {
		data := fmt.Sprintf("%s,%d\n", info.Name, info.NumTables)
		w.Write([]byte(data))
	}
}

func (c *CSVD) handleDeleteData(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")

	s := bufio.NewScanner(r.Body)
	if !s.Scan() {
		// if there is no data, delete the whole table
		err := c.store.DeleteTable(name)
		if err != nil {
			w.WriteHeader(500)
			w.Write([]byte(
				fmt.Sprintf("deleting table: %s", err),
			))
			return
		}
		w.Write([]byte("table deleted"))
		return
	}
}
