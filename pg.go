package main

type pgstorage struct{}

func (*pgstorage) name() string {
	return "pg"
}

func (*pgstorage) query(memberids []uint32, movieids []uint32) ([]output, error) {
	vs := make([]output, 0, len(memberids)*len(movieids))
	return vs, nil

}

func (*pgstorage) queryRange(low uint32, high uint32, movieids []uint32) ([]output, error) {
	vs := make([]output, 0, int(high-low)*len(movieids))
	return vs, nil
}

func (*pgstorage) setMember(id uint32, v vector) error { return nil }
func (*pgstorage) setMovie(id uint32, v vector) error  { return nil }

type output struct {
	member     uint32
	movie      uint32
	propensity float64
}
