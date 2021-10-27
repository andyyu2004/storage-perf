package main

import (
	"log"
	"testing"
)

func BenchmarkPg(b *testing.B) {
	pg, err := newPg()
	for i := 0; i < b.N; i++ {
		if err != nil {
			log.Fatal(err)
		}
		if err := query(pg); err != nil {
			log.Fatal(err)
		}
	}
}

func BenchmarkBadger(b *testing.B) {
	badger, err := newBadger()
	for i := 0; i < b.N; i++ {
		if err != nil {
			log.Fatal(err)
		}
		if err := query(badger); err != nil {
			log.Fatal(err)
		}
	}
}

func BenchmarkPebble(b *testing.B) {
	pebble, err := newPebble()
	for i := 0; i < b.N; i++ {
		if err != nil {
			log.Fatal(err)
		}
		if err := query(pebble); err != nil {
			log.Fatal(err)
		}
	}
}
