package pileup

import (
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
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

func bench(b *testing.B, callers int, scenario string) {
	var allocs, fetches int64
	constructor := func() []byte {
		atomic.AddInt64(&allocs, 1)
		return make([]byte, 4096)
	}
	destructor := func(v *[]byte) bool {
		return true
	}

	var workers sync.WaitGroup
	requests := make(chan int, callers)
	g := New[int](constructor, destructor)
	for j := 0; j < callers; j++ {
		go func() {
			for key := range requests {
				ref, _, _ := g.Do(key, func(w *[]byte) error {
					time.Sleep(10 * time.Millisecond)
					atomic.AddInt64(&fetches, 1)
					return nil
				})
				ref.Release()
				workers.Done()
			}
		}()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		workers.Add(callers)
		for j := 0; j < callers; j++ {
			switch scenario {
			case "best":
				requests <- 1
			case "mid":
				requests <- rand.Intn(callers / 2)
			case "worst":
				requests <- j
			}
		}
		workers.Wait()
	}
	b.ReportMetric(math.Round(float64(allocs)/float64(b.N)), "makes/op")
	b.ReportMetric(math.Round(float64(fetches)/float64(b.N)), "fetches/op")
}

func Benchmark_10_best(b *testing.B)     { bench(b, 10, "best") }
func Benchmark_10_mid(b *testing.B)      { bench(b, 10, "mid") }
func Benchmark_10_worst(b *testing.B)    { bench(b, 10, "worst") }
func Benchmark_100_best(b *testing.B)    { bench(b, 100, "best") }
func Benchmark_100_mid(b *testing.B)     { bench(b, 100, "mid") }
func Benchmark_100_worst(b *testing.B)   { bench(b, 100, "worst") }
func Benchmark_1000_best(b *testing.B)   { bench(b, 1000, "best") }
func Benchmark_1000_mid(b *testing.B)    { bench(b, 1000, "mid") }
func Benchmark_1000_worst(b *testing.B)  { bench(b, 1000, "worst") }
func Benchmark_10000_best(b *testing.B)  { bench(b, 10000, "best") }
func Benchmark_10000_mid(b *testing.B)   { bench(b, 10000, "mid") }
func Benchmark_10000_worst(b *testing.B) { bench(b, 10000, "worst") }
