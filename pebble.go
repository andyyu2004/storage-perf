package main

import (
	"encoding/binary"
	"log"

	"github.com/cockroachdb/pebble"
)

type pebblestorage struct {
	memberdb *pebble.DB
	moviedb  *pebble.DB
}

func newPebble() (*pebblestorage, error) {
	memberdb, err := pebble.Open("pebble_members", &pebble.Options{})
	if err != nil {
		return nil, err
	}
	moviedb, err := pebble.Open("pebble_movies", &pebble.Options{})
	return &pebblestorage{memberdb, moviedb}, err
}

func (s *pebblestorage) name() string {
	return "pebble"
}

func (s *pebblestorage) query(memberids []uint32, movieids []uint32) ([]output, error) {
	capacity := len(memberids) * len(movieids)
	vs := make([]output, 0, capacity)
	ch := make(chan output)

	for _, movie := range movieids {
		go func(movie uint32) {
			for _, member := range memberids {
				v := s.getMember(member)
				w := s.getMovie(movie)
				propensity := v.dot(w)
				// vs = append(vs, output{member, movie, propensity})
				ch <- output{member, movie, propensity}
			}
		}(movie)
	}

	for i := 0; i < capacity; i++ {
		vs = append(vs, <-ch)
	}

	return vs, nil
}

func (s *pebblestorage) queryModel(memberids []uint32, models []MovieModel) ([]output, error) {
	vs := make([]output, 0, len(memberids)*len(models))
	return vs, nil
}

func (s *pebblestorage) memberPropensities(movie uint32) ([]output, error) {
	vs := make([]output, N_MEMBERS)
	iter := s.memberdb.NewIter(&pebble.IterOptions{})
	i := 0
	for iter.First(); iter.Valid(); iter.Next() {
		println(s.name(), "member propensity", i)
		i++
		if i == 1_000_000 {
			break
		}
		member := binary.BigEndian.Uint32(iter.Key())
		v := vecFromBytes(iter.Value())
		w := s.getMovie(movie)
		propensity := v.dot(w)
		vs = append(vs, output{member, movie, propensity})
	}
	return vs, nil
}

func (s *pebblestorage) queryRange(low uint32, high uint32, movieids []uint32) ([]output, error) {
	vs := make([]output, 0, int(high-low)*len(movieids))
	iter := s.memberdb.NewIter(&pebble.IterOptions{LowerBound: uint32ToBeBytes(low), UpperBound: uint32ToBeBytes(high)})
	for _, movie := range movieids {
		for iter.First(); iter.Valid(); iter.Next() {
			member := binary.BigEndian.Uint32(iter.Key())
			v := vecFromBytes(iter.Value())
			w := s.getMovie(movie)
			propensity := v.dot(w)
			vs = append(vs, output{member, movie, propensity})
		}
	}
	return vs, nil
}

func get(db *pebble.DB, id uint32) vector {
	bytes, closer, err := db.Get(uint32ToBeBytes(id))
	if err != nil {
		log.Fatal(err)
	}
	if err := closer.Close(); err != nil {
		log.Fatal(err)
	}
	return vecFromBytes(bytes)
}

func (s *pebblestorage) getMember(id uint32) vector {
	return get(s.memberdb, id)
}

func (s *pebblestorage) getMovie(id uint32) vector {
	return get(s.moviedb, id)
}

func set(db *pebble.DB, id uint32, v vector) error {
	return db.Set(uint32ToBeBytes(id), v.toBytes(), &pebble.WriteOptions{})
}

func (s *pebblestorage) setMember(id uint32, v vector) error {
	return set(s.memberdb, id, v)
}

func (s *pebblestorage) setMovie(id uint32, v vector) error {

	return set(s.moviedb, id, v)
}

func (s *pebblestorage) insertRandomMembers(n int) error {
	t, err := timed(func() error {
		var err error
		for i := 0; i < n; i++ {
			println("pebble insert (counting up)", i)
			err = s.setMember(uint32(i), randomvec())
			if err != nil {
				return err
			}
		}
		return nil
	})

	if err != nil {
		return err
	}

	println(s.name(), "members insert time", t.Milliseconds())
	return nil
}

func (s *pebblestorage) insertRandomMovies(n int) error {
	t, err := timed(func() error {
		var err error
		for i := 0; i < n; i++ {
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

	println(s.name(), "movie insert time", t.Milliseconds())
	return nil
}
