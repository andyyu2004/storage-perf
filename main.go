package main

import (
	"log"
	"time"
)

const N_MEMBERS = 50_000_000
const N_MOVIES = 25_000

// Number of elements in each vector
const K = 10

const MOVIE_QUERY_SIZE = 5
const MEMBER_QUERY_SIZE = 20000

type storage interface {
	name() string
	query(memberids []uint32, movieids []uint32) ([]output, error)
	memberPropensities(movie uint32) ([]output, error)
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

	badger, err := newBadger()
	if err != nil {
		log.Fatal(err)
	}
	// if err := insert(badger); err != nil {
	// 	log.Fatal(err)
	// }

	pg, err := newPg()
	if err != nil {
		log.Fatal(err)
	}
	// if err := insert(pg); err != nil {
	// 	log.Fatal(err)
	// }

	_ = pebble
	_ = pg
	_ = badger
	backends := []storage{
		badger,
		pebble,
		pg,
	}

	for _, backend := range backends {
		if err := query(backend); err != nil {
			log.Fatal(err)
		}
		if err := queryRange(backend); err != nil {
			log.Fatal(err)
		}

		if err := queryMemberPropensities(backend); err != nil {
			log.Fatal(err)
		}
		println(backend.name(), "done")
		time.Sleep(time.Second * 2)
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
		data, err := s.query(members, movies)
		if err != nil {
			return err
		}
		expectedLen := MEMBER_QUERY_SIZE * MOVIE_QUERY_SIZE
		if len(data) != expectedLen {
			log.Fatalf("%s wrong number of query results: expected %d, got %d", s.name(), expectedLen, len(data))
		}
		return nil
	})

	if err != nil {
		return err
	}

	println(s.name(), "query time ", t.Milliseconds())
	return nil
}

func queryMemberPropensities(s storage) error {
	t, err := timed(func() error {
		data, err := s.memberPropensities(3)
		_ = data
		// expectedLen := N_MEMBERS
		// if len(data) != expectedLen {
		// 	log.Fatalf("wrong number of propensity results: expected %d, got %d", expectedLen, len(data))
		// }
		return err
	})

	if err != nil {
		return err
	}

	println(s.name(), "all propensities query time ", t.Milliseconds())
	return nil
}
