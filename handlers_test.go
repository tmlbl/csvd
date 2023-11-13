package main

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/matryer/is"
)

func testBody() io.Reader {
	return bytes.NewBuffer([]byte(strings.Join([]string{
		"email,name",
		"tim@example.com,Tim",
		"jim@example.com,Jimbo",
	}, "\n")))
}

func TestCreateTable(t *testing.T) {
	is := is.New(t)

	dir := t.TempDir()
	csvd, err := NewCSVD(dir)
	is.NoErr(err)

	body := testBody()
	w := httptest.NewRecorder()
	r, err := http.NewRequest(http.MethodPost, "/tables/test", body)
	is.NoErr(err)

	router := newRouter(csvd)
	router.ServeHTTP(w, r)

	is.Equal(w.Result().StatusCode, 200)

	def, err := csvd.store.ReadTableDef("test")
	is.NoErr(err)
	is.Equal(def.Name, "test")
	is.Equal(len(def.Columns), 2)

	it, err := csvd.store.ScanRows("test")
	is.NoErr(err)
	foundJim := false
	for it.Next() {
		if strings.HasPrefix(string(it.Value()), "jim") {
			foundJim = true
		}
	}
	is.True(foundJim)

	w = httptest.NewRecorder()
	body = bytes.NewBuffer([]byte("email\njim@example.com"))
	r, err = http.NewRequest(http.MethodDelete, "/tables/test", body)
	is.NoErr(err)

	router.ServeHTTP(w, r)
}

func TestDeleteRows(t *testing.T) {
	is := is.New(t)

	dir := t.TempDir()
	csvd, err := NewCSVD(dir)
	is.NoErr(err)

	body := testBody()
	w := httptest.NewRecorder()
	r, err := http.NewRequest(http.MethodPost, "/tables/test", body)
	is.NoErr(err)

	router := newRouter(csvd)
	router.ServeHTTP(w, r)

	is.Equal(w.Result().StatusCode, 200)

	// delete Tim
	deleteBody := bytes.NewBuffer([]byte("email\ntim@example.com"))
	w = httptest.NewRecorder()
	r, err = http.NewRequest(http.MethodDelete, "/tables/test", deleteBody)
	is.NoErr(err)

	router.ServeHTTP(w, r)

	is.Equal(w.Result().StatusCode, 200)

	it, err := csvd.store.ScanRows("test")
	is.NoErr(err)
	foundJim := false
	foundTim := false
	for it.Next() {
		if strings.HasPrefix(string(it.Value()), "jim") {
			foundJim = true
		} else if strings.HasPrefix(string(it.Value()), "tim") {
			foundTim = true
		}
	}
	is.True(foundJim)
	is.True(!foundTim)
}
