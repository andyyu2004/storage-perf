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
	a := make([]uint32, max-min+1)
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
