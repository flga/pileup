// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package zflight provides a duplicate function call suppression mechanism.
//
// It is a specialization of [golang.org/x/sync/singleflight] designed to enable
// zero allocation mode on top of thundering herd protection by pooling and
// refcounting the values shared amongst peers.
package zflight

import (
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
)

// Ref wraps T and adds reference counting to it.
//
// A Ref[T] will be shared by any callers that got suppressed (including the
// original requester).
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
	atomic.AddUint64(r.gReleases, 1)

	if atomic.AddInt64(&r.refs, -1) > 0 {
		return
	}

	atomic.AddUint64(r.gFreed, 1)
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
func (g *Group[K, T]) Do(key K, fn func(*T) error) (ref *Ref[T], leader bool, err error) {
	ref, leader, _, err = g.doInternal(key, fn)
	return ref, leader, err
}

func (g *Group[K, T]) doInternal(key K, fn func(*T) error) (*Ref[T], bool, int64, error) {
	g.mu.Lock()
	if w, ok := g.m[key]; ok {
		w.refs++
		g.mu.Unlock()
		w.wg.Wait()

		// SAFETY:
		// Any caller in-flight is guaranteed to either not see the current call
		// or to have written to w already. doCall will only mark the wg as done
		// after deleting it from the map (while holding the lock).
		//
		// This is guaranteed to happen after any current callers have written
		// to w (because they released the lock) or before new callers acquire
		// the lock (in which case they will not see the current call since
		// doCall deleted it).
		//
		// This flow ensures that every call to Do that gets coalesced (including
		// the leader) sees the same ref count, it is not sufficient to rely on
		// the fact that they use the same instance of w, we must ensure that
		// no new callers are allowed to see w after it is complete.
		//
		// It is not safe to mutate w at this point because the caller may
		// already hold a reference to it. It is safe to read but not write to
		// sfErr (the caller cannot write to it).
		if w.err != nil {
			// This call must always be deferred.
			// Calling release will eventually put w back in the pool, if a new
			// caller comes in and takes this w out of the pool it will cause
			// a race because we're still reading from it.
			defer w.Release()
			return nil, false, -1, w.err // only the leader can see refcount.
		}

		atomic.AddUint64(&g.issued, 1)
		return w, false, -1, nil // only the leader can see refcount.
	}

	// Acquire a new Writer, reset flight state and store it so that new callers
	// see it. The actual operation will happen without holding the lock to
	// reduce contention. Special care must be taken to ensure that no invariant
	// is violated after releasing the lock.
	//
	// See the SAFETY notice above, as well as in doCall.

	ref := g.pool.Get().(*Ref[T])
	ref.refs = 1
	ref.err = nil
	ref.wg.Add(1)
	g.m[key] = ref
	g.mu.Unlock()

	atomic.AddUint64(&g.created, 1)

	refs := g.doCall(ref, key, fn)
	// The call is complete. Read the SAFETY note above. From this point on
	// exactly the same circumstances apply since the two blocks are concurrent.

	if ref.err != nil {
		// This call must always be deferred.
		// Calling release will eventually put w back in the pool, if a new
		// caller comes in and takes this w out of the pool it will cause
		// a race because we're still reading from it.
		defer ref.Release()
		return nil, true, -1, ref.err
	}

	atomic.AddUint64(&g.issued, 1)
	return ref, true, refs, nil
}

// doCall handles the single call for a key.
func (g *Group[K, T]) doCall(ref *Ref[T], key K, fn func(*T) error) (refs int64) {
	defer func() {
		// It is guaranteed that we are the only writer to sfErr, it is also
		// guaranteed that there are currently no readers because we've yet to
		// return (which will allow the leader to read) and have not yet notified
		// the waiters that the call is done.
		if r := recover(); r != nil {
			ref.err = newPanicError(r)
		}

		g.mu.Lock()
		// We hold the lock, so no new waiter is able to join the flight and
		// any waiters are guaranteed to have written to flightRefs already.
		refs = ref.refs // This is for testing purposes only.
		delete(g.m, key)
		g.mu.Unlock() // After this point, new callers will start a new flight.

		// We have ensured that no new waiter joined in so we can unblock
		// current waiters.
		ref.wg.Done()
	}()
	ref.err = fn(&ref.Value)
	return 42 // meaningless, unconditionally overridden by defer.
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
