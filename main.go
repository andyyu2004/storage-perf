package main

import (
	"log"
)

const N_MEMBERS = 50_000_000
const N_MOVIES = 25_000

// Number of elements in each vector
const K = 10

const MOVIE_QUERY_SIZE = 5
const MEMBER_QUERY_SIZE = 10000

type storage interface {
	name() string
	query(memberids []uint32, movieids []uint32) ([]output, error)
	queryRange(low uint32, high uint32, movieids []uint32) ([]output, error)
	setMember(id uint32, v vector) error
	setMovie(id uint32, v vector) error
}

func main() {
	pebble, err := newPebble()
	if err != nil {
		log.Fatal(err)
	}

	backends := []storage{
		pebble,
	}

	for _, backend := range backends {
		// if err := insert(s); err != nil {
		// 	log.Fatal(err)
		// }

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
		var err error
		for i := 0; i < N_MEMBERS; i++ {
			println(i)
			err = s.setMember(uint32(i), randomvec())
			if err != nil {
				return err
			}
		}

		for i := 0; i < N_MOVIES; i++ {
			err = s.setMovie(uint32(i), randomvec())
			if err != nil {
				return err
			}
		}
		return err
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
		members := makeRange(0, 10000)
		movies := makeRange(0, 5)
		_, err := s.query(members, movies)
		return err
	})

	if err != nil {
		return err
	}

	println(s.name(), "query time ", t.Milliseconds())
	return nil
}
