package main

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"time"
)

type output struct {
	member     uint32
	movie      uint32
	propensity float64
}

func randomModels() []MovieModel {
	models := make([]MovieModel, MODEL_QUERY_SIZE)
	for i := 0; i < MODEL_QUERY_SIZE; i++ {
		models[i] = randomModel()
	}
	return models
}

func randomModel() MovieModel {
	n := rand.Int() % 20
	movies := make([]uint32, n)
	for i := 0; i < n; i++ {
		movies[i] = rand.Uint32() % N_MOVIES
	}
	return MovieModel{movies}

}

func uint32ToBeBytes(u uint32) []byte {
	bytes := make([]byte, 4)
	binary.BigEndian.PutUint32(bytes, u)
	return bytes
}

type vector struct {
	Points [K]float64
}

func (v vector) dot(w vector) float64 {
	dot := 0.0
	for i := 0; i < K; i++ {
		dot += v.Points[i] * w.Points[i]
	}
	return dot
}

func (v vector) addAssign(w vector) {
	for i := 0; i < K; i++ {
		v.Points[i] += w.Points[i]
	}
}

func (v vector) divAssign(divisor float64) {
	for i := 0; i < K; i++ {
		v.Points[i] /= divisor
	}
}

func (v vector) toBytes() []byte {
	buf := bytes.NewBuffer([]byte{})
	binary.Write(buf, binary.LittleEndian, v)
	return buf.Bytes()
}

func vecFromBytes(buf []byte) vector {
	var v vector
	binary.Read(bytes.NewBuffer(buf), binary.LittleEndian, &v)
	return v
}

func timed(f func() error) (time.Duration, error) {
	start := time.Now()
	err := f()
	t := time.Now()
	elapsed := t.Sub(start)
	return elapsed, err
}

func makeRange(min, max uint32) []uint32 {
	a := make([]uint32, max-min)
	for i := range a {
		a[i] = min + uint32(i)
	}
	return a
}

func randomvec() vector {
	v := vector{}
	for i := 0; i < K; i++ {
		v.Points[i] = rand.Float64()
	}
	return v
}
