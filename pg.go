package main

import (
	"context"
	"fmt"
	"math"

	"github.com/jackc/pgx/v4"
)

type pgstorage struct {
	db *pgx.Conn
}

func newPg() (*pgstorage, error) {
	ctx := context.Background()
	dsn := "host=localhost user=user password=password dbname=postgres sslmode=disable"
	connConfig, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to parse DSN config %v", err)
	}
	db, err := pgx.ConnectConfig(ctx, connConfig)
	if err != nil {
		return nil, err
	}
	_, err = db.Exec(ctx, "create table if not exists members(id integer primary key, vector bytea not null)")
	if err != nil {
		return nil, err
	}
	_, err = db.Exec(ctx, "create table if not exists movies(id integer primary key, vector bytea)")
	if err != nil {
		return nil, err
	}
	return &pgstorage{db}, err
}

func (*pgstorage) name() string {
	return "pg"
}

func (s *pgstorage) queryModel(memberids []uint32, models []MovieModel) ([]output, error) {
	vs := make([]output, 0, len(memberids)*len(models))
	query := `select id, vector from movies where id = any($1)`

	var row struct {
		id     uint32
		vector []byte
	}

	// this contains the averaged vectors for each model
	movieVectors := make([]vector, 0, len(models))
	for _, model := range models {
		rows, err := s.db.Query(context.Background(), query, model.movies)
		if err != nil {
			return nil, err
		}

		v := vector{}
		for rows.Next() {
			if err := rows.Scan(&row.id, &row.vector); err != nil {
				return nil, err
			}
			v.addAssign(vecFromBytes(row.vector))
		}
		v.divAssign(float64(len(model.movies)))
		movieVectors = append(movieVectors, v)
		rows.Close()
	}

	query = `select id, vector from members where members.id = any($1)`
	rows, err := s.db.Query(context.Background(), query, memberids)
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		if err := rows.Scan(&row.id, &row.vector); err != nil {
			return nil, err
		}

		for _, v := range movieVectors {
			// Don't really have a movie id as it's a model
			// We could insert each model as a movie maybe
			vs = append(vs, output{member: row.id, movie: math.MaxUint32, propensity: v.dot(vecFromBytes(row.vector))})
		}
	}

	return vs, nil
}

type row struct {
	member_id     uint32
	movie_id      uint32
	member_vector []byte
	movie_vector  []byte
}

func (s *pgstorage) query(memberids []uint32, movieids []uint32) ([]output, error) {
	vs := make([]output, 0, len(memberids)*len(movieids))
	query := `select members.id as member_id, movies.id as movie_id, members.vector as member_vector, movies.vector as movie_vector
			  from members cross join movies where members.id = any($1) and movies.id = any($2)`
	rows, err := s.db.Query(context.Background(), query, memberids, movieids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var x row
		if err := rows.Scan(&x.member_id, &x.movie_id, &x.member_vector, &x.movie_vector); err != nil {
			return nil, err
		}

		member_vector := vecFromBytes(x.member_vector)
		movie_vector := vecFromBytes(x.movie_vector)
		propensity := member_vector.dot(movie_vector)
		vs = append(vs, output{
			member:     x.member_id,
			movie:      x.movie_id,
			propensity: propensity,
		})
	}
	return vs, nil
}

func (s *pgstorage) memberPropensities(movie uint32) ([]output, error) {
	vs := make([]output, 0, N_MEMBERS)
	query := `select members.id as member_id, movies.id as movie_id, members.vector as member_vector, movies.vector as movie_vector
			  from members cross join movies where movies.id = $1`
	rows, err := s.db.Query(context.Background(), query, movie)
	if err != nil {
		return nil, err
	}
	// defer rows.Close()

	i := 0
	for rows.Next() {
		println(s.name(), "member propensity", i)
		i++

		if i == 1_000_000 {
			break
		}
		var x struct {
			member_id     uint32
			movie_id      uint32
			member_vector []byte
			movie_vector  []byte
		}

		if err := rows.Scan(&x.member_id, &x.movie_id, &x.member_vector, &x.movie_vector); err != nil {
			return nil, err
		}

		member_vector := vecFromBytes(x.member_vector)
		movie_vector := vecFromBytes(x.movie_vector)
		propensity := member_vector.dot(movie_vector)
		vs = append(vs, output{
			member:     x.member_id,
			movie:      x.movie_id,
			propensity: propensity,
		})
	}
	return vs, nil
}

func (s *pgstorage) queryRange(low uint32, high uint32, movieids []uint32) ([]output, error) {
	vs := make([]output, 0, int(high-low)*len(movieids))
	query := `select members.id as member_id, movies.id as movie_id, members.vector as member_vector, movies.vector as movie_vector
			  from members cross join movies where members.id between $1 and $2 and movies.id = any($3)`
	rows, err := s.db.Query(context.Background(), query, low, high, movieids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var x struct {
			member_id     uint32
			movie_id      uint32
			member_vector []byte
			movie_vector  []byte
		}

		if err := rows.Scan(&x.member_id, &x.movie_id, &x.member_vector, &x.movie_vector); err != nil {
			return nil, err
		}

		member_vector := vecFromBytes(x.member_vector)
		movie_vector := vecFromBytes(x.movie_vector)
		propensity := member_vector.dot(movie_vector)
		vs = append(vs, output{
			member:     x.member_id,
			movie:      x.movie_id,
			propensity: propensity,
		})
	}

	return vs, nil
}

func (s *pgstorage) insertRandomMembers(n int) error {
	ctx := context.Background()
	_, err := s.db.CopyFrom(ctx, pgx.Identifier{"members"}, []string{"id", "vector"}, &randomSource{n})
	return err
}

func (s *pgstorage) insertRandomMovies(n int) error {
	ctx := context.Background()
	_, err := s.db.CopyFrom(ctx, pgx.Identifier{"movies"}, []string{"id", "vector"}, &randomSource{n})
	return err
}

type randomSource struct {
	n int
}

func (s *randomSource) Next() bool {
	s.n--
	return s.n >= 0
}

func (s *randomSource) Err() error { return nil }

func (s *randomSource) Values() ([]interface{}, error) {
	v := randomvec()
	println("pg insert (counting down)", s.n)
	return []interface{}{s.n, v.toBytes()}, nil
}
