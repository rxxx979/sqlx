package sqlx

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"
)

type iterPerson struct {
	ID   int    `db:"id"`
	Name string `db:"name"`
}

func newIterDB(t *testing.T) *DB {
	t.Helper()
	db, err := Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })

	db.MustExec(`CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)`)
	tx := db.MustBegin()
	for i := 1; i <= 5; i++ {
		tx.MustExec(`INSERT INTO people (id, name) VALUES (?, ?)`, i, fmt.Sprintf("p%d", i))
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return db
}

func TestGetAs(t *testing.T) {
	db := newIterDB(t)

	p, err := GetAs[iterPerson](db, `SELECT id, name FROM people WHERE id = ?`, 3)
	if err != nil {
		t.Fatal(err)
	}
	if p.ID != 3 || p.Name != "p3" {
		t.Fatalf("unexpected person: %+v", p)
	}

	pp, err := GetAs[*iterPerson](db, `SELECT id, name FROM people WHERE id = ?`, 4)
	if err != nil {
		t.Fatal(err)
	}
	if pp == nil || pp.ID != 4 || pp.Name != "p4" {
		t.Fatalf("unexpected person pointer: %+v", pp)
	}

	if _, err := GetAs[iterPerson](db, `SELECT id, name FROM people WHERE id = ?`, 99); err != sql.ErrNoRows {
		t.Fatalf("expected ErrNoRows, got %v", err)
	}
}

func TestGetAsContext(t *testing.T) {
	db := newIterDB(t)
	p, err := GetAsContext[iterPerson](context.Background(), db, `SELECT id, name FROM people WHERE id = ?`, 2)
	if err != nil {
		t.Fatal(err)
	}
	if p.ID != 2 || p.Name != "p2" {
		t.Fatalf("unexpected person: %+v", p)
	}
}

func TestSelectAs(t *testing.T) {
	db := newIterDB(t)

	ps, err := SelectAs[iterPerson](db, `SELECT id, name FROM people ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	if len(ps) != 5 || ps[0].Name != "p1" || ps[4].ID != 5 {
		t.Fatalf("unexpected people: %+v", ps)
	}

	ptrs, err := SelectAs[*iterPerson](db, `SELECT id, name FROM people ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	if len(ptrs) != 5 || ptrs[2] == nil || ptrs[2].Name != "p3" {
		t.Fatalf("unexpected person pointers: %+v", ptrs)
	}

	ids, err := SelectAs[int](db, `SELECT id FROM people ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 5 || ids[0] != 1 || ids[4] != 5 {
		t.Fatalf("unexpected ids: %v", ids)
	}
}

func TestSelectAsContext(t *testing.T) {
	db := newIterDB(t)
	ps, err := SelectAsContext[iterPerson](context.Background(), db, `SELECT id, name FROM people ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	if len(ps) != 5 {
		t.Fatalf("got %d rows", len(ps))
	}
}

func TestSelectIter(t *testing.T) {
	db := newIterDB(t)

	var got []iterPerson
	for p, err := range SelectIter[iterPerson](db, `SELECT id, name FROM people ORDER BY id`) {
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, p)
	}
	if len(got) != 5 || got[0].ID != 1 || got[4].Name != "p5" {
		t.Fatalf("unexpected rows: %+v", got)
	}
}

func TestSelectIterEarlyBreak(t *testing.T) {
	db := newIterDB(t)

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
	if n != 2 {
		t.Fatalf("expected 2 rows, got %d", n)
	}

	// The iterator must have closed its rows when the loop broke; with a
	// single-connection pool a leaked cursor would block this query.
	deadline := time.Now().Add(2 * time.Second)
	for db.Stats().InUse != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if inUse := db.Stats().InUse; inUse != 0 {
		t.Fatalf("%d connections still in use after early break", inUse)
	}
	var count int
	if err := db.Get(&count, `SELECT count(*) FROM people`); err != nil {
		t.Fatal(err)
	}
	if count != 5 {
		t.Fatalf("expected 5 people, got %d", count)
	}
}

func TestSelectIterError(t *testing.T) {
	db := newIterDB(t)

	type onlyName struct {
		Name string `db:"name"`
	}
	var gotErr error
	for _, err := range SelectIter[onlyName](db, `SELECT id, name FROM people`) {
		if err != nil {
			gotErr = err
			break
		}
	}
	if gotErr == nil || !strings.Contains(gotErr.Error(), "missing destination name id") {
		t.Fatalf("expected missing destination error, got %v", gotErr)
	}
}

func TestSelectIterContext(t *testing.T) {
	db := newIterDB(t)

	n := 0
	for _, err := range SelectIterContext[iterPerson](context.Background(), db, `SELECT id, name FROM people`) {
		if err != nil {
			t.Fatal(err)
		}
		n++
	}
	if n != 5 {
		t.Fatalf("expected 5 rows, got %d", n)
	}
}

// TestSelectReusedSliceZeroesFields verifies that scanning into a slice that
// already holds rows does not leak values for columns absent from the new
// query.  The previous implementation allocated a fresh struct per row.
func TestSelectReusedSliceZeroesFields(t *testing.T) {
	db := newIterDB(t)

	var out []iterPerson
	if err := db.Select(&out, `SELECT id, name FROM people ORDER BY id`); err != nil {
		t.Fatal(err)
	}
	if len(out) != 5 || out[0].Name != "p1" {
		t.Fatalf("unexpected first result: %+v", out)
	}

	if err := db.Select(&out, `SELECT id FROM people ORDER BY id`); err != nil {
		t.Fatal(err)
	}
	if len(out) != 5 {
		t.Fatalf("expected 5 rows, got %d", len(out))
	}
	if out[0].Name != "" {
		t.Fatalf("stale field leaked into reused slice: %+v", out[0])
	}
}

// TestStructScanDifferentDestTypes ensures the per-Rows plan is rebuilt when
// the destination type changes, instead of reusing the previous type's field
// indexes.
func TestStructScanDifferentDestTypes(t *testing.T) {
	db := newIterDB(t)

	rows, err := db.Queryx(`SELECT id, name FROM people ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	type idFirst struct {
		ID   int    `db:"id"`
		Name string `db:"name"`
	}
	type nameFirst struct {
		Name string `db:"name"`
		ID   int    `db:"id"`
	}

	var a idFirst
	if !rows.Next() {
		t.Fatal("missing first row")
	}
	if err := rows.StructScan(&a); err != nil {
		t.Fatal(err)
	}
	if a.ID != 1 || a.Name != "p1" {
		t.Fatalf("unexpected first row: %+v", a)
	}

	var b nameFirst
	if !rows.Next() {
		t.Fatal("missing second row")
	}
	if err := rows.StructScan(&b); err != nil {
		t.Fatal(err)
	}
	if b.ID != 2 || b.Name != "p2" {
		t.Fatalf("unexpected second row: %+v", b)
	}
}

func TestSelectIterPointerAndScalar(t *testing.T) {
	db := newIterDB(t)

	var ptrs []*iterPerson
	for p, err := range SelectIter[*iterPerson](db, `SELECT id, name FROM people ORDER BY id`) {
		if err != nil {
			t.Fatal(err)
		}
		if p == nil {
			t.Fatal("nil pointer yielded")
		}
		ptrs = append(ptrs, p)
	}
	if len(ptrs) != 5 || ptrs[1].Name != "p2" {
		t.Fatalf("unexpected pointers: %+v", ptrs)
	}

	var ids []int
	for id, err := range SelectIter[int](db, `SELECT id FROM people ORDER BY id`) {
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if len(ids) != 5 || ids[0] != 1 || ids[4] != 5 {
		t.Fatalf("unexpected ids: %v", ids)
	}

	names, err := SelectAs[string](db, `SELECT name FROM people ORDER BY id`)
	if err != nil || len(names) != 5 || names[0] != "p1" {
		t.Fatalf("unexpected names: %v, %v", names, err)
	}

	var ptrIDs []*int
	for id, err := range SelectIter[*int](db, `SELECT id FROM people ORDER BY id`) {
		if err != nil {
			t.Fatal(err)
		}
		if id == nil {
			t.Fatal("nil pointer yielded")
		}
		ptrIDs = append(ptrIDs, id)
	}
	if len(ptrIDs) != 5 || *ptrIDs[0] != 1 {
		t.Fatalf("unexpected pointer ids")
	}
}

func TestSelectIterNestedPointerError(t *testing.T) {
	db := newIterDB(t)
	var gotErr error
	for _, err := range SelectIter[**iterPerson](db, `SELECT id, name FROM people`) {
		if err != nil {
			gotErr = err
			break
		}
	}
	if gotErr == nil || !strings.Contains(gotErr.Error(), "nested pointer") {
		t.Fatalf("expected nested pointer error, got %v", gotErr)
	}
}
