// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
package pileup

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func newValue() int          { return 0 }
func resetValue(v *int) bool { *v = 0; return true }

func TestRace(t *testing.T) {
	g := New[string](newValue, resetValue)

	const n, m = 1000, 100
	var sum, calls uint64

	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			for i := 0; i < m; i++ {
				ref, _, _ := g.Do("asd", func(w *int) error {
					*w = 42
					atomic.AddUint64(&calls, 1)
					return nil
				})
				atomic.AddUint64(&sum, uint64(ref.Value))
				ref.Release()
			}
		}()
	}
	wg.Wait()
	if want := uint64(n * m * 42); sum != want {
		t.Errorf("sum = %d, want %d", sum, want)
	}
	if wantMax := uint64(m * n); calls > wantMax {
		t.Errorf("calls = %d, want at most %d", calls, wantMax)
	}
	if want := uint64(m * n); g.issued != want {
		t.Errorf("refs = %d, want %d", g.issued, want)
	}

	if g.issued != g.releases {
		t.Errorf("leaking issued refs, issued = %d, release calls = %d", g.issued, g.releases)
	}
	if g.created != g.freed {
		t.Errorf("leaking created refs, created = %d, freed = %d", g.created, g.freed)
	}
}

func TestDo(t *testing.T) {
	g := New[string](newValue, resetValue)
	got, _, err := g.Do("key", func(w *int) error {
		*w = 42
		return nil
	})
	if got.refs != 1 {
		t.Errorf("Do refs = %d, want %d", got.refs, 1)
	}
	if want := 42; got.Value != want {
		t.Errorf("Do = %d; want %d", got.Value, want)
	}
	if err != nil {
		t.Errorf("Do error = %v", err)
	}
}

func TestDoErr(t *testing.T) {
	g := New[string](newValue, resetValue)
	someErr := errors.New("Some error")
	got, _, err := g.Do("key", func(w *int) error {
		return someErr
	})
	if err != someErr {
		t.Errorf("Do error = %v; want someErr %v", err, someErr)
	}
	if got != nil {
		t.Errorf("unexpected non-nil value %#v", got)
	}
}

func TestDoDupSuppress(t *testing.T) {
	g := New[string](newValue, resetValue)
	var wg1, wg2 sync.WaitGroup
	c := make(chan int, 1)
	var calls int32
	fn := func(w *int) error {
		if atomic.AddInt32(&calls, 1) == 1 {
			// First invocation.
			wg1.Done()
		}
		v := <-c
		c <- v // pump; make available for any future calls
		*w = v

		time.Sleep(10 * time.Millisecond) // let more goroutines enter Do
		return nil
	}

	const n = 10
	wg1.Add(1)
	for i := 0; i < n; i++ {
		wg1.Add(1)
		wg2.Add(1)
		go func() {
			defer wg2.Done()
			wg1.Done()
			got, _, err := g.Do("key", fn)
			if got.refs != n {
				t.Errorf("Do refs = %d, want %d", got.refs, n)
			}
			if err != nil {
				t.Errorf("Do error: %v", err)
				return
			}
			if got.Value != 42 {
				t.Errorf("Do = %d; want %d", got.Value, 42)
			}
		}()
	}
	wg1.Wait()
	// At least one goroutine is in fn now and all of them have at
	// least reached the line before the Do.
	c <- 42
	wg2.Wait()
	if got := atomic.LoadInt32(&calls); got <= 0 || got >= n {
		t.Errorf("number of calls = %d; want over 0 and less than %d", got, n)
	}
}

// Test singleflight behaves correctly after Do panic.
// See https://github.com/golang/go/issues/41133
func TestPanicDo(t *testing.T) {
	g := New[string](newValue, resetValue)
	fn := func(w *int) error {
		panic("invalid memory address or nil pointer dereference")
	}

	const n = 5000
	var wg sync.WaitGroup
	wg.Add(n)
	panicCount := int32(0)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			got, _, err := g.Do("key", fn)
			if got != nil {
				panic("got should be nil")
			}
			if errors.As(err, new(*panicError)) {
				atomic.AddInt32(&panicCount, 1)
			}
		}()
	}

	wg.Wait()
	if panicCount != n {
		t.Errorf("Expect %d panic, but got %d", n, panicCount)
	}
}
