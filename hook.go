package sqlx

import (
	"context"
	"sync/atomic"
	"time"
)

// QueryOp identifies the kind of database operation a QueryEvent describes.
type QueryOp string

const (
	OpQuery    QueryOp = "query"
	OpQueryRow QueryOp = "query_row"
	OpExec     QueryOp = "exec"
	OpPrepare  QueryOp = "prepare"
	OpBegin    QueryOp = "begin"
	OpCommit   QueryOp = "commit"
	OpRollback QueryOp = "rollback"
)

// QueryEvent describes one database operation observed by a QueryHook.
//
// For OpQuery, AfterQuery is called when the returned *Rows is closed, so
// Duration includes row iteration and RowsScanned is the number of rows
// consumed.  For OpQueryRow it is called when Row.Scan finishes.  For OpExec,
// OpPrepare, OpBegin, OpCommit and OpRollback it is called when the underlying
// call returns.
type QueryEvent struct {
	DriverName string
	Op         QueryOp
	Query      string
	Args       []any

	Start    time.Time
	End      time.Time
	Duration time.Duration

	RowsScanned  int64
	RowsAffected int64
	Err          error

	// Data is hook-private state.  sqlx never reads or writes it.  A tracing
	// hook can use it to carry a span from BeforeQuery to AfterQuery.
	Data any
}

// QueryHook observes database operations.  Hooks run synchronously on the
// calling goroutine and must be safe for concurrent use.  A hook must not
// recursively use the same DB without a reentrancy guard.
//
// Hooks are observers: they cannot rewrite SQL, abort a query, retry, or
// change the result.  Use a separate interceptor or driver wrapper for those
// behaviors.
type QueryHook interface {
	BeforeQuery(ctx context.Context, event *QueryEvent)
	AfterQuery(ctx context.Context, event *QueryEvent)
}

// HookFuncs adapts functions to the QueryHook interface.  Either field may be
// nil.
type HookFuncs struct {
	Before func(context.Context, *QueryEvent)
	After  func(context.Context, *QueryEvent)
}

func (h HookFuncs) BeforeQuery(ctx context.Context, event *QueryEvent) {
	if h.Before != nil {
		h.Before(ctx, event)
	}
}

func (h HookFuncs) AfterQuery(ctx context.Context, event *QueryEvent) {
	if h.After != nil {
		h.After(ctx, event)
	}
}

// MultiHook runs a sequence of hooks in order.  A panic in any hook propagates
// to the caller.
type MultiHook []QueryHook

func (m MultiHook) BeforeQuery(ctx context.Context, event *QueryEvent) {
	for _, h := range m {
		if h != nil {
			h.BeforeQuery(ctx, event)
		}
	}
}

func (m MultiHook) AfterQuery(ctx context.Context, event *QueryEvent) {
	for _, h := range m {
		if h != nil {
			h.AfterQuery(ctx, event)
		}
	}
}

// hookHolder stores a QueryHook behind an atomic pointer.  A nil holder means
// "no hook"; DB, Tx, Conn and Stmt share the same holder so changing a DB hook
// also affects objects derived from it.
type hookHolder struct {
	p atomic.Pointer[QueryHook]
}

func newHookHolder() *hookHolder {
	return &hookHolder{}
}

func (h *hookHolder) load() QueryHook {
	if h == nil {
		return nil
	}
	p := h.p.Load()
	if p == nil {
		return nil
	}
	return *p
}

func (h *hookHolder) store(hook QueryHook) {
	if h == nil {
		return
	}
	if hook == nil {
		h.p.Store(nil)
		return
	}
	h.p.Store(&hook)
}

// observation is the per-operation hook state.  It is nil when there is no
// hook, which keeps the disabled path allocation-free.
type observation struct {
	hook QueryHook
	ctx  context.Context
	ev   *QueryEvent
	done bool
}

func (h *hookHolder) start(ctx context.Context, driverName string, op QueryOp, query string, args []any) *observation {
	hook := h.load()
	if hook == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ev := &QueryEvent{
		DriverName: driverName,
		Op:         op,
		Query:      query,
		Args:       args,
		Start:      time.Now(),
	}
	hook.BeforeQuery(ctx, ev)
	return &observation{hook: hook, ctx: ctx, ev: ev}
}

func (o *observation) finish(err error, scanned, affected int64) {
	if o == nil || o.done {
		return
	}
	o.done = true
	o.ev.End = time.Now()
	o.ev.Duration = o.ev.End.Sub(o.ev.Start)
	o.ev.Err = err
	o.ev.RowsScanned = scanned
	o.ev.RowsAffected = affected
	o.hook.AfterQuery(o.ctx, o.ev)
}

// SetQueryHook installs an observer for this DB and objects derived from it.
// Passing nil disables hooks.  SetQueryHook should be called during setup;
// it is safe to reload while other goroutines are using the DB.
func (db *DB) SetQueryHook(hook QueryHook) {
	if db.hooks == nil {
		db.hooks = newHookHolder()
	}
	db.hooks.store(hook)
}

// hooksFor returns the hook holder associated with a sqlx wrapper, if any.
func hooksFor(v any) *hookHolder {
	switch v := v.(type) {
	case *DB:
		return v.hooks
	case *Tx:
		return v.hooks
	case *Conn:
		return v.hooks
	case *Stmt:
		return v.hooks
	case *qStmt:
		return v.Stmt.hooks
	default:
		return nil
	}
}

// driverNameFor returns the driver name associated with a sqlx wrapper.
func driverNameFor(v any) string {
	switch v := v.(type) {
	case *DB:
		return v.driverName
	case *Tx:
		return v.driverName
	case *Conn:
		return v.driverName
	case *Stmt:
		return v.driverName
	case *qStmt:
		return v.Stmt.driverName
	default:
		return ""
	}
}
