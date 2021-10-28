package main

import (
	"encoding/binary"
	"log"

	"github.com/dgraph-io/badger/v3"
)

type badgerstorage struct {
	memberdb *badger.DB
	moviedb  *badger.DB
}

// May require increasing ulimit: `ulimit -n -S 65536` should be enough
func newBadger() (*badgerstorage, error) {
	memberdb, err := badger.Open(badger.DefaultOptions("badger_members"))
	if err != nil {
		return nil, err
	}
	moviedb, err := badger.Open(badger.DefaultOptions("badger_movies"))
	return &badgerstorage{memberdb, moviedb}, err
}

func (s *badgerstorage) name() string {
	return "badger"
}

func (s *badgerstorage) query(memberids []uint32, movieids []uint32) ([]output, error) {
	capacity := len(memberids) * len(movieids)
	vs := make([]output, 0, capacity)
	ch := make(chan output)
	for _, movie := range movieids {
		go func(movie uint32) {
			for _, member := range memberids {
				v := s.getMember(member)
				w := s.getMovie(movie)
				propensity := v.dot(w)
				ch <- output{member, movie, propensity}
			}
		}(movie)
	}

	for i := 0; i < capacity; i++ {
		vs = append(vs, <-ch)
	}
	return vs, nil
}

func (s *badgerstorage) queryRange(low uint32, high uint32, movieids []uint32) ([]output, error) {
	vs := make([]output, 0, int(high-low)*len(movieids))
	return vs, nil
}

func (s *badgerstorage) queryModel(memberids []uint32, models []MovieModel) ([]output, error) {
	vs := make([]output, 0, len(memberids)*len(models))
	return vs, nil
}

func (s *badgerstorage) memberPropensities(movie uint32) ([]output, error) {
	vs := make([]output, N_MEMBERS)
	err := s.memberdb.View(func(txn *badger.Txn) error {
		iter := txn.NewIterator(badger.IteratorOptions{})
		i := 0
		for iter.Rewind(); iter.Valid(); iter.Next() {
			println(s.name(), "member propensity", i)
			i++
			if i == 1_000_000 {
				break
			}
			member := binary.BigEndian.Uint32(iter.Item().Key())
			v := vecFromBytes(iter.Item().Key())
			w := s.getMovie(movie)
			propensity := v.dot(w)
			vs = append(vs, output{member, movie, propensity})
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return vs, nil
}

func badgerGet(db *badger.DB, id uint32) vector {
	var vector vector
	db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(uint32ToBeBytes(id))
		if err != nil {
			log.Fatal(err)
		}
		return item.Value(func(val []byte) error {
			vector = vecFromBytes(val)
			return nil
		})
	})
	return vector
}

func (s *badgerstorage) getMember(id uint32) vector {
	return badgerGet(s.memberdb, id)
}

func (s *badgerstorage) getMovie(id uint32) vector {
	return badgerGet(s.moviedb, id)
}

func (s *badgerstorage) insertRandomMembers(n int) error {
	t, err := timed(func() error {
		batch := s.memberdb.NewWriteBatch()
		for i := 0; i < n; i++ {
			println("badger insert (counting up)", i)
			err := batch.Set(uint32ToBeBytes(uint32(i)), randomvec().toBytes())
			// err := s.memberdb.Update(func(txn *badger.Txn) error {
			// 	if err := txn.Set(uint32ToBeBytes(uint32(i)), randomvec().toBytes()); err != nil {
			// 		return err
			// 	}
			// 	return nil
			// })

			if err != nil {
				return err
			}
		}
		return batch.Flush()
	})

	if err != nil {
		return err
	}

	println(s.name(), "members insert time", t.Milliseconds())
	return nil
}

func (s *badgerstorage) insertRandomMovies(n int) error {
	t, err := timed(func() error {
		return s.moviedb.Update(func(txn *badger.Txn) error {
			for i := 0; i < n; i++ {
				if err := txn.Set(uint32ToBeBytes(uint32(i)), randomvec().toBytes()); err != nil {
					return err
				}
			}
			return nil
		})
	})

	if err != nil {
		return err
	}

	println(s.name(), "movie insert time", t.Milliseconds())
	return nil
}
