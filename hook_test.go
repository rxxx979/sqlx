package sqlx

import (
	"context"
	"database/sql"
	"sync"
	"testing"
)

type hookRecorder struct {
	mu      sync.Mutex
	befores int
	afters  int
	events  []QueryEvent
}

func (h *hookRecorder) BeforeQuery(ctx context.Context, ev *QueryEvent) {
	h.mu.Lock()
	h.befores++
	h.mu.Unlock()
}

func (h *hookRecorder) AfterQuery(ctx context.Context, ev *QueryEvent) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.afters++
	cp := *ev
	cp.Args = append([]any(nil), ev.Args...)
	h.events = append(h.events, cp)
}

func (h *hookRecorder) reset() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.befores = 0
	h.afters = 0
	h.events = nil
}

func (h *hookRecorder) snapshot() (befores, afters int, events []QueryEvent) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.befores, h.afters, append([]QueryEvent(nil), h.events...)
}

func newHookDB(t *testing.T) (*DB, *hookRecorder) {
	t.Helper()
	db := newIterDB(t)
	rec := &hookRecorder{}
	db.SetQueryHook(rec)
	return db, rec
}

func TestQueryHookSelectAndGet(t *testing.T) {
	db, rec := newHookDB(t)

	var people []iterPerson
	if err := db.Select(&people, `SELECT id, name FROM people ORDER BY id`); err != nil {
		t.Fatal(err)
	}
	befores, afters, events := rec.snapshot()
	if befores != 1 || afters != 1 || len(events) != 1 {
		t.Fatalf("select hook counts: before=%d after=%d events=%d", befores, afters, len(events))
	}
	ev := events[0]
	if ev.Op != OpQuery || ev.DriverName != "sqlite" {
		t.Fatalf("unexpected event: %+v", ev)
	}
	if ev.RowsScanned != int64(len(people)) || len(people) != 5 {
		t.Fatalf("rows scanned = %d, want 5", ev.RowsScanned)
	}
	if ev.Err != nil || ev.Duration <= 0 || ev.Query == "" {
		t.Fatalf("incomplete event: %+v", ev)
	}

	rec.reset()
	p, err := GetAs[iterPerson](db, `SELECT id, name FROM people WHERE id = ?`, 3)
	if err != nil {
		t.Fatal(err)
	}
	if p.ID != 3 {
		t.Fatalf("unexpected person: %+v", p)
	}
	_, _, events = rec.snapshot()
	if len(events) != 1 {
		t.Fatalf("get events = %d", len(events))
	}
	if events[0].Op != OpQueryRow || events[0].RowsScanned != 1 {
		t.Fatalf("unexpected get event: %+v", events[0])
	}

	rec.reset()
	var missing iterPerson
	err = db.Get(&missing, `SELECT id, name FROM people WHERE id = ?`, 999)
	if err != sql.ErrNoRows {
		t.Fatalf("expected ErrNoRows, got %v", err)
	}
	_, _, events = rec.snapshot()
	if len(events) != 1 || events[0].Err != sql.ErrNoRows || events[0].RowsScanned != 0 {
		t.Fatalf("unexpected no-rows event: %+v", events)
	}
}

func TestQueryHookExecAndNamed(t *testing.T) {
	db, rec := newHookDB(t)

	res, err := db.Exec(`UPDATE people SET name = ? WHERE id = ?`, "updated", 2)
	if err != nil {
		t.Fatal(err)
	}
	n, _ := res.RowsAffected()
	_, _, events := rec.snapshot()
	if len(events) != 1 || events[0].Op != OpExec || events[0].RowsAffected != n {
		t.Fatalf("unexpected exec event: %+v", events)
	}

	rec.reset()
	if _, err := db.NamedExec(`UPDATE people SET name = :name WHERE id = :id`, map[string]any{
		"name": "named",
		"id":   2,
	}); err != nil {
		t.Fatal(err)
	}
	_, _, events = rec.snapshot()
	if len(events) != 1 || events[0].Op != OpExec {
		t.Fatalf("unexpected named exec event: %+v", events)
	}
	if events[0].Query == "" || len(events[0].Args) != 2 {
		t.Fatalf("named exec did not expose bound query/args: %+v", events[0])
	}
}

func TestQueryHookPrepareAndStmt(t *testing.T) {
	db, rec := newHookDB(t)

	stmt, err := db.Preparex(`SELECT id, name FROM people WHERE id = ?`)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()

	_, _, events := rec.snapshot()
	if len(events) != 1 || events[0].Op != OpPrepare || events[0].Query == "" {
		t.Fatalf("unexpected prepare event: %+v", events)
	}

	rec.reset()
	rows, err := stmt.Queryx(2)
	if err != nil {
		t.Fatal(err)
	}
	if !rows.Next() {
		t.Fatal("missing row")
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	_, _, events = rec.snapshot()
	if len(events) != 1 {
		t.Fatalf("stmt query events = %d", len(events))
	}
	if events[0].Op != OpQuery || events[0].Query == "" || events[0].RowsScanned != 1 {
		t.Fatalf("unexpected stmt query event: %+v", events[0])
	}

	rec.reset()
	if _, err := stmt.Exec(2); err != nil {
		t.Fatal(err)
	}
	_, _, events = rec.snapshot()
	if len(events) != 1 || events[0].Op != OpExec {
		t.Fatalf("unexpected stmt exec event: %+v", events)
	}
}

func TestQueryHookNamedStmtSingleEvent(t *testing.T) {
	db, rec := newHookDB(t)

	stmt, err := db.PrepareNamed(`SELECT id, name FROM people WHERE id = :id`)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	rec.reset()

	rows, err := stmt.Queryx(map[string]any{"id": 2})
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	_, _, events := rec.snapshot()
	if len(events) != 1 {
		t.Fatalf("named stmt should emit exactly one query event, got %d: %+v", len(events), events)
	}
	if events[0].Op != OpQuery || events[0].RowsScanned != 1 {
		t.Fatalf("unexpected named stmt event: %+v", events[0])
	}
}

func TestQueryHookTxAndConn(t *testing.T) {
	db, rec := newHookDB(t)

	tx, err := db.Beginx()
	if err != nil {
		t.Fatal(err)
	}
	rows, err := tx.Queryx(`SELECT id, name FROM people ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`UPDATE people SET name = ? WHERE id = ?`, "tx", 1); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	_, _, events := rec.snapshot()
	if len(events) != 4 || events[0].Op != OpBegin || events[1].Op != OpQuery || events[2].Op != OpExec || events[3].Op != OpCommit {
		t.Fatalf("unexpected tx events: %+v", events)
	}

	rec.reset()
	conn, err := db.Connx(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	rows, err = conn.QueryxContext(context.Background(), `SELECT id, name FROM people ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(context.Background(), `UPDATE people SET name = ? WHERE id = ?`, "conn", 1); err != nil {
		t.Fatal(err)
	}
	_, _, events = rec.snapshot()
	if len(events) != 2 || events[0].Op != OpQuery || events[1].Op != OpExec {
		t.Fatalf("unexpected conn events: %+v", events)
	}
}

func TestQueryHookTransactionLifecycle(t *testing.T) {
	db, rec := newHookDB(t)

	tx, err := db.BeginTxx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	_, _, events := rec.snapshot()
	if len(events) != 2 || events[0].Op != OpBegin || events[1].Op != OpRollback {
		t.Fatalf("unexpected rollback events: %+v", events)
	}

	rec.reset()
	tx, err = db.Beginx()
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	_, _, events = rec.snapshot()
	if len(events) != 2 || events[0].Op != OpBegin || events[1].Op != OpCommit {
		t.Fatalf("unexpected commit events: %+v", events)
	}

	rec.reset()
	conn, err := db.Connx(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	tx, err = conn.BeginTxx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	_, _, events = rec.snapshot()
	if len(events) != 2 || events[0].Op != OpBegin || events[1].Op != OpRollback {
		t.Fatalf("unexpected conn begin/rollback events: %+v", events)
	}
}

func TestQueryHookErrorAndCloseOnce(t *testing.T) {
	db, rec := newHookDB(t)

	if _, err := db.Queryx(`SELECT * FROM does_not_exist`); err == nil {
		t.Fatal("expected query error")
	}
	_, _, events := rec.snapshot()
	if len(events) != 1 || events[0].Err == nil {
		t.Fatalf("unexpected query error event: %+v", events)
	}

	rec.reset()
	rows, err := db.Queryx(`SELECT id, name FROM people ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	if !rows.Next() {
		t.Fatal("missing row")
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	_, afters, events := rec.snapshot()
	if afters != 1 || len(events) != 1 || events[0].RowsScanned != 1 {
		t.Fatalf("close should finish exactly once: after=%d events=%+v", afters, events)
	}

	rec.reset()
	db.SetQueryHook(nil)
	if _, err := db.Exec(`UPDATE people SET name = ? WHERE id = ?`, "x", 1); err != nil {
		t.Fatal(err)
	}
	befores, afters, events := rec.snapshot()
	if befores != 0 || afters != 0 || len(events) != 0 {
		t.Fatalf("hook still active after nil: %d/%d/%d", befores, afters, len(events))
	}
}

func TestQueryHookPanicDoesNotSkipAfter(t *testing.T) {
	db, rec := newHookDB(t)
	rec.reset()

	// A panic in the query path should still let Rows.Close finish the event if
	// the caller closes rows in a defer.
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("expected panic")
			}
		}()
		rows, err := db.Queryx(`SELECT id, name FROM people`)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		panic("boom")
	}()

	_, afters, events := rec.snapshot()
	if afters != 1 || len(events) != 1 {
		t.Fatalf("panic path should still finish: after=%d events=%+v", afters, events)
	}
}

func TestQueryHookEventData(t *testing.T) {
	db := newIterDB(t)
	rec := &hookRecorder{}
	db.SetQueryHook(HookFuncs{
		Before: func(ctx context.Context, ev *QueryEvent) {
			ev.Data = "state"
		},
		After: rec.AfterQuery,
	})
	if _, err := db.Exec(`UPDATE people SET name = ? WHERE id = ?`, "x", 1); err != nil {
		t.Fatal(err)
	}
	_, _, events := rec.snapshot()
	if len(events) != 1 || events[0].Data != "state" {
		t.Fatalf("event Data was not preserved: %+v", events)
	}
}

var _ QueryHook = (*hookRecorder)(nil)

func TestQueryHookGenericPaths(t *testing.T) {
	db, rec := newHookDB(t)

	if _, err := SelectAsInto[iterPerson](nil, db, `SELECT id, name FROM people ORDER BY id`); err != nil {
		t.Fatal(err)
	}
	_, _, events := rec.snapshot()
	if len(events) != 1 || events[0].RowsScanned != 5 {
		t.Fatalf("SelectAsInto event: %+v", events)
	}

	rec.reset()
	n := 0
	for _, err := range SelectIter[iterPerson](db, `SELECT id, name FROM people ORDER BY id`) {
		if err != nil {
			t.Fatal(err)
		}
		n++
		if n == 2 {
			break
		}
	}
	_, _, events = rec.snapshot()
	if len(events) != 1 || events[0].RowsScanned != 2 {
		t.Fatalf("SelectIter early-break event: %+v", events)
	}

	rec.reset()
	rows, err := db.Queryx(`SELECT id, name FROM people ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if !rows.Next() {
			t.Fatal("missing row")
		}
		if _, err := rows.ScanAs[iterPerson](); err != nil {
			t.Fatal(err)
		}
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	_, _, events = rec.snapshot()
	if len(events) != 1 || events[0].RowsScanned != 2 {
		t.Fatalf("Rows.ScanAs event: %+v", events)
	}
}

type orderHook struct {
	name string
	log  *[]string
}

func (h orderHook) BeforeQuery(ctx context.Context, ev *QueryEvent) {
	*h.log = append(*h.log, "before:"+h.name)
}

func (h orderHook) AfterQuery(ctx context.Context, ev *QueryEvent) {
	*h.log = append(*h.log, "after:"+h.name)
}

func TestQueryHookMultiHookOrder(t *testing.T) {
	db := newIterDB(t)
	var log []string
	db.SetQueryHook(MultiHook{orderHook{"a", &log}, orderHook{"b", &log}})
	if _, err := db.Exec(`UPDATE people SET name = ? WHERE id = ?`, "x", 1); err != nil {
		t.Fatal(err)
	}
	want := []string{"before:a", "before:b", "after:a", "after:b"}
	if len(log) != len(want) {
		t.Fatalf("order log = %v, want %v", log, want)
	}
	for i := range want {
		if log[i] != want[i] {
			t.Fatalf("order log = %v, want %v", log, want)
		}
	}
}

func TestQueryHookReplace(t *testing.T) {
	db := newIterDB(t)
	first, second := &hookRecorder{}, &hookRecorder{}
	db.SetQueryHook(first)
	if _, err := db.Exec(`UPDATE people SET name = ? WHERE id = ?`, "first", 1); err != nil {
		t.Fatal(err)
	}
	db.SetQueryHook(second)
	if _, err := db.Exec(`UPDATE people SET name = ? WHERE id = ?`, "second", 1); err != nil {
		t.Fatal(err)
	}
	_, _, firstEvents := first.snapshot()
	_, _, secondEvents := second.snapshot()
	if len(firstEvents) != 1 || len(secondEvents) != 1 {
		t.Fatalf("hook replace: first=%d second=%d", len(firstEvents), len(secondEvents))
	}
}

func TestQueryHookStmtRawQuery(t *testing.T) {
	db, rec := newHookDB(t)
	stmt, err := db.Preparex(`SELECT id, name FROM people WHERE id = ?`)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	rec.reset()

	rows, err := stmt.Query(1)
	if err != nil {
		t.Fatal(err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	_, _, events := rec.snapshot()
	if len(events) != 1 || events[0].Op != OpQuery || events[0].RowsScanned != 0 {
		t.Fatalf("unexpected raw stmt query event: %+v", events)
	}
}
