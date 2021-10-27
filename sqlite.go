package main

// import (
// 	"database/sql"

// 	_ "github.com/mattn/go-sqlite3"
// )

// type sqlitestorage struct {
// 	db *sql.DB
// }

// func newSqlite() (*sqlitestorage, error) {
// 	db, err := sql.Open("sqlite3", "./data.sqlite")
// 	db.Exec("create table if not exists members(id integer, vector blob)")
// 	db.Exec("create table if not exists movies(id integer, vector blob")
// 	return &sqlitestorage{db}, err
// }

// func (*sqlitestorage) name() string {
// 	return "sqlite"
// }

// func (*sqlitestorage) query(memberids []uint32, movieids []uint32) ([]output, error) {
// 	vs := make([]output, 0, len(memberids)*len(movieids))
// 	return vs, nil

// }

// func (*sqlitestorage) queryRange(low uint32, high uint32, movieids []uint32) ([]output, error) {
// 	vs := make([]output, 0, int(high-low)*len(movieids))
// 	return vs, nil
// }

// func (s *sqlitestorage) setMember(id uint32, v vector) error {
// 	_, err := s.db.Exec("insert into members (id, vector) values ($1, $2)", id, v.toBytes())
// 	return err
// }

// func (s *sqlitestorage) setMovie(id uint32, v vector) error {
// 	_, err := s.db.Exec("insert into movies (id, vector) values ($1, $2)", id, v.toBytes())
// 	return err
// }
