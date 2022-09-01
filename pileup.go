// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package pileup provides a duplicate function call suppression mechanism.
//
// It is a specialization of [golang.org/x/sync/singleflight] designed to enable
// zero-ish allocation mode on top of thundering herd protection by pooling and
// refcounting the values shared amongst peers.
package pileup

import (
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
)

// Ref wraps T and adds reference counting to it.
//
// A Ref[T] will be shared by any callers that got suppressed along with the
// original requester.
type Ref[T any] struct {
	// Handles to the group that created us, used for returning Ref back to the
	// pool and keeping track of debug metrics. Internal.
	// We don't use *Group itself to avoid inheriting the key parametric type.
	gPool     *sync.Pool
	gReleases *uint64
	gFreed    *uint64

	Value T             // RW, reset before returning the instance to the pool. User visible.
	reset func(*T) bool // RO

	// RW, reset after obtaining the instance from the pool. Internal.
	//
	// Written once before the wg is done and only read after the wg is done.
	err error

	// RW, reset after obtaining the instance from the pool. User visible.
	//
	// Read and written with the singleflight mutex held before the wg is done,
	// and read but not written by us after the wg is done. A caller may
	// indirectly write to it (atomically) by calling Release().
	refs int64

	// Keeps track of whether the call is complete.
	wg sync.WaitGroup
}

// Release decrements the ref count. Once it reaches 0 the value will be returned
// to the pool.
//
// It is safe to call Release concurrently. Every holder of a Ref should release
// it when done.
func (r *Ref[T]) Release() {
	// must not mutate flight state.
	if EnableMetrics {
		atomic.AddUint64(r.gReleases, 1)
	}

	if atomic.AddInt64(&r.refs, -1) > 0 {
		return
	}

	if EnableMetrics {
		atomic.AddUint64(r.gFreed, 1)
	}
	if r.reset(&r.Value) {
		r.gPool.Put(r)
	}
}

// Group represents a class of work and forms a namespace in
// which units of work can be executed with duplicate suppression.
//
// As opposed to the original implementation, the zero value is NOT ready to use.
type Group[K comparable, T any] struct {
	m    map[K]*Ref[T]
	pool *sync.Pool
	mu   sync.Mutex // protects m and [Ref.refs]

	// keep track of the lifetimes of the created values, for debugging purposes.
	releases uint64
	created  uint64
	issued   uint64
	freed    uint64
}

// New creates a new [Group].
//
// constructor is used to create a new T, it should not return a pointer.
// reset is used to clear any transient state of T, the return value indicates
// whether T is eligble to be put back in the pool.
//
// If you're allocating dynamically sized objects like buffers, you can drop
// the value by returning false.
func New[K comparable, T any](constructor func() T, reset func(*T) bool) *Group[K, T] {
	g := &Group[K, T]{
		m:    make(map[K]*Ref[T]),
		pool: &sync.Pool{},
	}
	g.pool.New = func() any {
		return &Ref[T]{
			gPool:     g.pool,
			gReleases: &g.releases,
			gFreed:    &g.freed,
			refs:      1,
			Value:     constructor(),
			reset:     reset,
		}
	}

	return g
}

// Do executes and returns the results of the given function, making
// sure that only one execution is in-flight for a given key at a
// time. If a duplicate comes in, the duplicate caller waits for the
// original to complete and receives the same results.
//
// The return value leader indicates whether this was the first call.
//
// If the inner function returns an error, it will be propagated.
// Do will return either a Ref[T] or an error, but never both.
func (g *Group[K, T]) Do(key K, fn func(*T) error) (ref *Ref[T], leader bool, err error) {
	g.mu.Lock()
	if ref, ok := g.m[key]; ok {
		ref.refs++
		g.mu.Unlock()
		ref.wg.Wait()

		// SAFETY:
		// Any caller in-flight is guaranteed to either not see the current call
		// or to have written to ref already. doCall will only mark the wg as done
		// after deleting it from the map (while holding the lock).
		//
		// This is guaranteed to happen after any current callers have written
		// to ref (because they released the lock) or before new callers acquire
		// the lock (in which case they will not see the current call since
		// doCall deleted it).
		//
		// This flow ensures that every call to Do that gets coalesced (including
		// the leader) sees the same ref count, it is not sufficient to rely on
		// the fact that they use the same instance of ref, we must ensure that
		// no new callers are allowed to see ref after it is complete.
		//
		// It is not safe to mutate ref at this point because the caller may
		// already hold a reference to it. It is safe to read but not write to
		// err (the caller cannot write to it).
		if ref.err != nil {
			// Calling release will eventually put ref back in the pool, if a new
			// caller comes in and takes this ref out of the pool it will cause
			// a race because we're still reading from it. So read first, release after.
			err = ref.err
			ref.Release()
			return nil, false, err
		}

		if EnableMetrics {
			atomic.AddUint64(&g.issued, 1)
		}
		return ref, false, nil
	}

	// Acquire a new Ref, reset flight state and store it so that new callers
	// see it. The actual operation will happen without holding the lock to
	// reduce contention. Special care must be taken to ensure that no invariant
	// is violated after releasing the lock.
	//
	// See the SAFETY notice above, as well as in doCall.
	ref = g.pool.Get().(*Ref[T])
	ref.refs = 1
	ref.err = nil
	ref.wg.Add(1)
	g.m[key] = ref
	g.mu.Unlock()

	if EnableMetrics {
		atomic.AddUint64(&g.created, 1)
	}

	g.doCall(ref, key, fn)
	// The call is complete. Read the SAFETY note above. From this point on
	// exactly the same circumstances apply since the two blocks are concurrent.

	if ref.err != nil {
		// Calling release will eventually put ref back in the pool, if a new
		// caller comes in and takes this ref out of the pool it will cause
		// a race because we're still reading from it. So read first, release after.
		err = ref.err
		ref.Release()
		return nil, true, err
	}

	if EnableMetrics {
		atomic.AddUint64(&g.issued, 1)
	}

	return ref, true, nil
}

// doCall handles the single call for a key.
func (g *Group[K, T]) doCall(ref *Ref[T], key K, fn func(*T) error) {
	defer func() {
		// It is guaranteed that we are the only writer to err, it is also
		// guaranteed that there are currently no readers because we've yet to
		// return (which will allow the leader to read) and have not yet notified
		// the waiters that the call is done.
		if r := recover(); r != nil {
			ref.err = newPanicError(r)
		}

		g.mu.Lock()
		// We hold the lock, so no new waiter is able to join the flight and
		// any waiters are guaranteed to have written to refs already.
		delete(g.m, key)
		g.mu.Unlock() // After this point, new callers will start a new flight.

		// We have ensured that no new waiter joined in so we can unblock
		// current waiters.
		ref.wg.Done()
	}()
	ref.err = fn(&ref.Value)
}

// A panicError is an arbitrary value recovered from a panic
// with the stack trace during the execution of given function.
type panicError struct {
	value any
	stack []byte
}

// Error implements error interface.
func (p *panicError) Error() string {
	return fmt.Sprintf("%v\n\n%s", p.value, p.stack)
}

func newPanicError(v any) error {
	return &panicError{value: v, stack: debug.Stack()}
}
