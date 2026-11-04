package sqlx

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
)

// scannerPerson implements RowScanner by hand.  In real code this method would
// be generated; it is the reflection-free escape hatch for hot structs.
type scannerPerson struct {
	ID   int    `db:"id"`
	Name string `db:"name"`
}

func (p *scannerPerson) ScanRow(rows *sql.Rows) error {
	return rows.Scan(&p.ID, &p.Name)
}

// binderPerson implements StructBinder by hand.
type binderPerson struct {
	ID   int
	Name string
}

func (p binderPerson) BindArgs(names []string) ([]any, error) {
	out := make([]any, len(names))
	for i, name := range names {
		switch name {
		case "id":
			out[i] = p.ID
		case "name":
			out[i] = p.Name
		default:
			return nil, fmt.Errorf("unknown bind name %q", name)
		}
	}
	return out, nil
}

func TestRowScanner(t *testing.T) {
	db := newIterDB(t)

	ps, err := SelectAs[scannerPerson](db, `SELECT id, name FROM people ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	if len(ps) != 5 || ps[0].ID != 1 || ps[4].Name != "p5" {
		t.Fatalf("SelectAs scanner: %+v", ps)
	}

	// The non-generic API must use the same fast path.
	var legacy []scannerPerson
	if err := db.Select(&legacy, `SELECT id, name FROM people ORDER BY id`); err != nil {
		t.Fatal(err)
	}
	if len(legacy) != 5 || legacy[2].Name != "p3" {
		t.Fatalf("Select scanner: %+v", legacy)
	}

	ptrs, err := SelectAs[*scannerPerson](db, `SELECT id, name FROM people ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	if len(ptrs) != 5 || ptrs[1] == nil || ptrs[1].Name != "p2" {
		t.Fatalf("SelectAs scanner pointers: %+v", ptrs)
	}

	p, err := GetAs[scannerPerson](db, `SELECT id, name FROM people WHERE id = ?`, 3)
	if err != nil {
		t.Fatal(err)
	}
	if p.ID != 3 || p.Name != "p3" {
		t.Fatalf("GetAs scanner: %+v", p)
	}

	pp, err := GetAs[*scannerPerson](db, `SELECT id, name FROM people WHERE id = ?`, 4)
	if err != nil {
		t.Fatal(err)
	}
	if pp == nil || pp.ID != 4 {
		t.Fatalf("GetAs scanner pointer: %+v", pp)
	}
}

func TestScanAs(t *testing.T) {
	db := newIterDB(t)

	rows, err := db.Queryx(`SELECT id, name FROM people ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var got []scannerPerson
	for rows.Next() {
		v, err := rows.ScanAs[scannerPerson]()
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, v)
	}
	if len(got) != 5 || got[0].ID != 1 || got[4].Name != "p5" {
		t.Fatalf("Rows.ScanAs: %+v", got)
	}

	row := db.QueryRowx(`SELECT id, name FROM people WHERE id = ?`, 2)
	v, err := row.ScanAs[scannerPerson]()
	if err != nil {
		t.Fatal(err)
	}
	if v.ID != 2 || v.Name != "p2" {
		t.Fatalf("Row.ScanAs: %+v", v)
	}

	id, err := db.QueryRowx(`SELECT count(*) FROM people`).ScanAs[int]()
	if err != nil {
		t.Fatal(err)
	}
	if id != 5 {
		t.Fatalf("Row.ScanAs[int] = %d", id)
	}
}

func TestGenericScalarPaths(t *testing.T) {
	db := newIterDB(t)

	ids, err := SelectAs[int](db, `SELECT id FROM people ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 5 || ids[0] != 1 || ids[4] != 5 {
		t.Fatalf("SelectAs[int]: %v", ids)
	}

	name, err := GetAs[string](db, `SELECT name FROM people WHERE id = ?`, 3)
	if err != nil {
		t.Fatal(err)
	}
	if name != "p3" {
		t.Fatalf("GetAs[string] = %q", name)
	}

	count, err := GetAs[int](db, `SELECT count(*) FROM people`)
	if err != nil {
		t.Fatal(err)
	}
	if count != 5 {
		t.Fatalf("GetAs[int] = %d", count)
	}

	if _, err := SelectAs[int](db, `SELECT id, name FROM people`); err == nil {
		t.Fatal("expected >1 columns error")
	}
}

func TestStructBinder(t *testing.T) {
	db := newIterDB(t)

	if _, err := db.NamedExec(`UPDATE people SET name = :name WHERE id = :id`, binderPerson{ID: 2, Name: "bound"}); err != nil {
		t.Fatal(err)
	}
	name, err := GetAs[string](db, `SELECT name FROM people WHERE id = ?`, 2)
	if err != nil {
		t.Fatal(err)
	}
	if name != "bound" {
		t.Fatalf("binder did not update row: %q", name)
	}

	// Pointer arguments must also use the binder without dereferencing through
	// reflection.
	if _, err := db.NamedExec(`UPDATE people SET name = :name WHERE id = :id`, &binderPerson{ID: 3, Name: "ptr"}); err != nil {
		t.Fatal(err)
	}

	q, args, err := bindStruct(QUESTION, `UPDATE people SET name = :name WHERE id = :id`, binderPerson{ID: 7, Name: "x"}, mapper())
	if err != nil {
		t.Fatal(err)
	}
	if q != `UPDATE people SET name = ? WHERE id = ?` || len(args) != 2 || args[0] != "x" || args[1] != 7 {
		t.Fatalf("unexpected bind: %q %#v", q, args)
	}
}

type EmbeddedInner struct {
	ID   int    `db:"id"`
	Name string `db:"name"`
}

type EmbeddedPtr struct {
	*EmbeddedInner
}

func TestSelectAsEmbeddedPointer(t *testing.T) {
	db := newIterDB(t)

	out, err := SelectAs[EmbeddedPtr](db, `SELECT id, name FROM people ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 5 || out[0].EmbeddedInner == nil || out[4].Name != "p5" {
		t.Fatalf("unexpected embedded pointer rows: %+v", out)
	}

	in, err := SelectAs[EmbeddedInner](db, `SELECT id, name FROM people ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	if len(in) != 5 || in[2].ID != 3 {
		t.Fatalf("unexpected embedded rows: %+v", in)
	}
}

func TestSelectAsInto(t *testing.T) {
	db := newIterDB(t)
	dest := make([]iterPerson, 0, 10)

	out, err := SelectAsInto(dest, db, `SELECT id, name FROM people ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 5 || cap(out) < 10 || out[0].ID != 1 {
		t.Fatalf("unexpected result: %+v", out)
	}

	// Reusing the returned slice must reset its length and keep capacity.
	out, err = SelectAsInto(out, db, `SELECT id FROM people ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 5 || out[0].Name != "" {
		t.Fatalf("stale value or wrong length: %+v", out)
	}

	ids, err := SelectAsInto[int](nil, db, `SELECT id FROM people ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 5 || ids[4] != 5 {
		t.Fatalf("unexpected ids: %v", ids)
	}

	ctxOut, err := SelectAsIntoContext(context.Background(), dest[:0], db, `SELECT id, name FROM people ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	if len(ctxOut) != 5 || ctxOut[0].Name != "p1" {
		t.Fatalf("unexpected context result: %+v", ctxOut)
	}
}

func TestSelectAsEmbeddedMap(t *testing.T) {
	db := newIterDB(t)
	db.MustExec(`CREATE TABLE message (string TEXT, properties TEXT)`)
	msg := Message{Text: "hello", Properties: PropertyMap{"a": "1"}}
	if _, err := db.NamedExec(`INSERT INTO message (string, properties) VALUES (:string, :properties)`, msg); err != nil {
		t.Fatal(err)
	}

	out, err := SelectAs[Message](db, `SELECT * FROM message`)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 || out[0].Properties == nil || out[0].Properties["a"] != "1" {
		t.Fatalf("unexpected message: %+v", out)
	}

	var legacy []Message
	if err := db.Select(&legacy, `SELECT * FROM message`); err != nil {
		t.Fatal(err)
	}
	if len(legacy) != 1 || legacy[0].Properties == nil {
		t.Fatalf("legacy Select lost map: %+v", legacy)
	}
}

func TestRowsScanAsIndirect(t *testing.T) {
	db := newIterDB(t)

	rows, err := db.Queryx(`SELECT id, name FROM people ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	var embedded []EmbeddedPtr
	for rows.Next() {
		v, err := rows.ScanAs[EmbeddedPtr]()
		if err != nil {
			t.Fatal(err)
		}
		embedded = append(embedded, v)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if len(embedded) != 5 || embedded[0].EmbeddedInner == nil || embedded[4].Name != "p5" {
		t.Fatalf("unexpected embedded rows: %+v", embedded)
	}

	// Map fields also need their per-row allocation refreshed.
	db.MustExec(`CREATE TABLE message (string TEXT, properties TEXT)`)
	if _, err := db.NamedExec(`INSERT INTO message (string, properties) VALUES (:string, :properties)`,
		Message{Text: "hello", Properties: PropertyMap{"k": "v"}}); err != nil {
		t.Fatal(err)
	}
	rows, err = db.Queryx(`SELECT * FROM message`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("missing row")
	}
	msg, err := rows.ScanAs[Message]()
	if err != nil {
		t.Fatal(err)
	}
	if msg.Properties == nil || msg.Properties["k"] != "v" {
		t.Fatalf("map field lost: %+v", msg)
	}
}

func TestSelectPtrBlocks(t *testing.T) {
	db := newIterDB(t)
	tx := db.MustBegin()
	for i := 6; i <= 200; i++ {
		tx.MustExec(`INSERT INTO people (id, name) VALUES (?, ?)`, i, fmt.Sprintf("p%d", i))
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	var legacy []*iterPerson
	if err := db.Select(&legacy, `SELECT id, name FROM people ORDER BY id`); err != nil {
		t.Fatal(err)
	}
	if len(legacy) != 200 || legacy[0].ID != 1 || legacy[63].ID != 64 || legacy[64].ID != 65 || legacy[199].Name != "p200" {
		t.Fatalf("unexpected legacy pointers at block boundaries")
	}
	if legacy[0] == legacy[1] {
		t.Fatal("duplicate pointer")
	}

	generic, err := SelectAs[*iterPerson](db, `SELECT id, name FROM people ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	if len(generic) != 200 || generic[64].ID != 65 || generic[199].Name != "p200" {
		t.Fatalf("unexpected generic pointers at block boundaries")
	}

	rows, err := db.Queryx(`SELECT id, name FROM people ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var scanned []*iterPerson
	for rows.Next() {
		p, err := rows.ScanAs[*iterPerson]()
		if err != nil {
			t.Fatal(err)
		}
		scanned = append(scanned, p)
	}
	if len(scanned) != 200 || scanned[64].ID != 65 || scanned[199].Name != "p200" {
		t.Fatalf("unexpected ScanAs pointers at block boundaries")
	}
}
