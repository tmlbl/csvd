package main

import (
	"bufio"
	"bytes"
	"fmt"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
)

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
	} else {
		for s.Scan() {
			row := s.Bytes()
			ix := bytes.Index(row, []byte{','})
			if ix < 0 {
				// assume the whole row is the primary key
				ix = len(row)
			}
			pkey := string(row[:ix])

			err := c.store.DeleteRow(name, pkey)
			if err != nil {
				w.WriteHeader(500)
				w.Write([]byte(
					fmt.Sprintf("deleting row %s: %s",
						pkey, err),
				))
				return
			}
		}
	}
}
