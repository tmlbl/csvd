package main

import (
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

	csvd, err := NewCSVD(*dir)
	if err != nil {
		log.Fatalln(err)
	}

	http.ListenAndServe(":3737", newRouter(csvd))
}

func newRouter(csvd *CSVD) *chi.Mux {
	r := chi.NewRouter()

	r.Get("/tables", csvd.handleListTables)
	r.Post("/tables/{name}", csvd.handlePostData)
	r.Get("/tables/{name}", csvd.handleReadRows)
	r.Delete("/tables/{name}", csvd.handleDeleteData)

	r.Post("/tables/{table}/tags/{tag}", csvd.handleTagTable)
	r.Delete("/tables/{table}/tags/{tag}", csvd.handleDeleteTag)
	r.Get("/tags", csvd.handleListTags)

	return r
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

func (c *CSVD) handleDeleteTag(w http.ResponseWriter, r *http.Request) {
	table := chi.URLParam(r, "table")
	tag := chi.URLParam(r, "tag")

	err := c.store.DeleteTag(table, tag)
	if err != nil {
		w.WriteHeader(500)
		w.Write([]byte(
			fmt.Sprintf("deleting tag: %s", err),
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
