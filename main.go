package main

import (
	"log"
)

const N_MEMBERS = 100_000_000
const N_MOVIES = 25_000

// Number of elements in each vector
const K = 10

const MOVIE_QUERY_SIZE = 100
const MEMBER_QUERY_SIZE = 10000

type storage interface {
	name() string
	query(memberids []uint32, movieids []uint32) ([]output, error)
	queryRange(low uint32, high uint32, movieids []uint32) ([]output, error)
	insertRandomMembers(n int) error
	insertRandomMovies(n int) error
}

// Uncomment `insert`s to run the insertion code once as it's pretty slow
func main() {
	pebble, err := newPebble()
	if err != nil {
		log.Fatal(err)
	}
	// if err := insert(pebble); err != nil {
	// 	log.Fatal(err)
	// }

	pg, err := newPg()
	if err != nil {
		log.Fatal(err)
	}
	// if err := insert(pg); err != nil {
	// 	log.Fatal(err)
	// }

	backends := []storage{
		pebble,
		pg,
	}

	for _, backend := range backends {
		if err := queryRange(backend); err != nil {
			log.Fatal(err)
		}
		if err := query(backend); err != nil {
			log.Fatal(err)
		}
	}

}

func insert(s storage) error {
	t, err := timed(func() error {
		if err := s.insertRandomMembers(N_MEMBERS); err != nil {
			return err
		}
		if err := s.insertRandomMovies(N_MOVIES); err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		return err
	}

	println(s.name(), "insert time", t.Milliseconds())
	return nil
}

func queryRange(s storage) error {
	t, err := timed(func() error {
		movies := makeRange(0, MOVIE_QUERY_SIZE)
		_, err := s.queryRange(0, MEMBER_QUERY_SIZE, movies)
		return err
	})

	if err != nil {
		return err
	}

	println(s.name(), "range query time:", t.Milliseconds())
	return nil
}

func query(s storage) error {
	t, err := timed(func() error {
		members := makeRange(0, MEMBER_QUERY_SIZE)
		movies := makeRange(0, MOVIE_QUERY_SIZE)
		_, err := s.query(members, movies)
		return err
	})

	if err != nil {
		return err
	}

	println(s.name(), "query time ", t.Milliseconds())
	return nil
}
