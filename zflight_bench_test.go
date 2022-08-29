package zflight

import (
	"sync"
	"testing"

	"golang.org/x/sync/singleflight"
)

func BenchmarkSerial(b *testing.B) {
	g := New[int](newValue, resetValue)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ref, _, err := g.Do(i, func(w *int) error {
			*w = i
			return nil
		})
		if err != nil {
			b.Error(err)
		}

		if i != ref.Value {
			b.Errorf("want %d, got %d", i, ref.Value)
		}

		ref.Release()
	}
}

func benchmarkOurs(b *testing.B, callers, size int) {
	constructor := func() []byte {
		return make([]byte, size)
	}
	resetter := func(v *[]byte) bool {
		return true
	}

	g := New[string](constructor, resetter)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		wg.Add(callers)
		for j := 0; j < callers; j++ {
			go func(i int) {
				defer wg.Done()
				ref, _, err := g.Do("a", func(w *[]byte) error {
					(*w)[0] = byte(i)
					return nil
				})
				if err != nil {
					b.Error(err)
				}
				if byte(i) != ref.Value[0] {
					b.Errorf("want %d, got %d", i, ref.Value[0])
				}
				ref.Release()
			}(i)
		}
		wg.Wait()
	}
}

func benchmarkSingleflight(b *testing.B, callers, size int) {
	var g singleflight.Group

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		wg.Add(callers)
		for j := 0; j < callers; j++ {
			go func(i int) {
				defer wg.Done()
				ref, err, _ := g.Do("a", func() (any, error) {
					w := make([]byte, size)
					w[0] = byte(i)
					return w, nil
				})
				if err != nil {
					b.Error(err)
				}
				got := ref.([]byte)
				if byte(i) != got[0] {
					b.Errorf("want %d, got %d", i, got[0])
				}
			}(i)
		}
		wg.Wait()
	}
}

func BenchmarkConcurrentOurs10b(b *testing.B)    { benchmarkOurs(b, 1000, 10) }
func BenchmarkConcurrentOurs100b(b *testing.B)   { benchmarkOurs(b, 1000, 100) }
func BenchmarkConcurrentOurs1000b(b *testing.B)  { benchmarkOurs(b, 1000, 1000) }
func BenchmarkConcurrentOurs10000b(b *testing.B) { benchmarkOurs(b, 1000, 10000) }

func BenchmarkConcurrentSingleflight10b(b *testing.B)    { benchmarkSingleflight(b, 1000, 10) }
func BenchmarkConcurrentSingleflight100b(b *testing.B)   { benchmarkSingleflight(b, 1000, 100) }
func BenchmarkConcurrentSingleflight1000b(b *testing.B)  { benchmarkSingleflight(b, 1000, 1000) }
func BenchmarkConcurrentSingleflight10000b(b *testing.B) { benchmarkSingleflight(b, 1000, 10000) }
