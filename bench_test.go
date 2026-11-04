package sqlx

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"testing"
)

// benchPerson is deliberately a plain, flat struct: it represents the common
// case that sqlx's scanning fast path should make essentially free.
type benchPerson struct {
	ID        int64   `db:"id"`
	FirstName string  `db:"first_name"`
	LastName  string  `db:"last_name"`
	Email     string  `db:"email"`
	Score     float64 `db:"score"`
}

const benchRows = 1000

// benchDB creates an in-memory SQLite database with benchRows rows.  A single
// connection is pinned so the schema survives across queries.
func benchDB(b *testing.B) *DB {
	b.Helper()
	db, err := Open("sqlite", ":memory:")
	if err != nil {
		b.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	b.Cleanup(func() { db.Close() })

	db.MustExec(`CREATE TABLE bench_person (
		id INTEGER PRIMARY KEY,
		first_name TEXT,
		last_name TEXT,
		email TEXT,
		score REAL
	)`)

	tx := db.MustBegin()
	for i := range benchRows {
		tx.MustExec(`INSERT INTO bench_person (id, first_name, last_name, email, score)
			VALUES (?, ?, ?, ?, ?)`, i, "First", "Last", fmt.Sprintf("user%d@example.com", i), float64(i)/10)
	}
	if err := tx.Commit(); err != nil {
		b.Fatal(err)
	}
	return db
}

// fakeRows is a rowsi implementation whose Scan is a no-op.  It isolates the
// reflection and allocation cost of sqlx's scanning code from the database
// driver and from value conversion.
type fakeRows struct {
	cols []string
	n    int
	i    int
}

func (r *fakeRows) Close() error               { return nil }
func (r *fakeRows) Columns() ([]string, error) { return r.cols, nil }
func (r *fakeRows) Err() error                 { return nil }
func (r *fakeRows) Next() bool                 { r.i++; return r.i <= r.n }
func (r *fakeRows) Scan(dest ...any) error     { return nil }

var benchColumns = []string{"id", "first_name", "last_name", "email", "score"}

// BenchmarkStructScanAll measures scanAll's reflect work for 1000 rows with no
// driver overhead.
func BenchmarkStructScanAll(b *testing.B) {
	rows := &fakeRows{cols: benchColumns, n: benchRows}
	var dest []benchPerson

	b.ReportAllocs()
	for b.Loop() {
		rows.i = 0
		if err := StructScan(rows, &dest); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkStructScanAllPtr is the same, but for a slice of pointers, which
// cannot use the in-place slice fill.
func BenchmarkStructScanAllPtr(b *testing.B) {
	rows := &fakeRows{cols: benchColumns, n: benchRows}
	var dest []*benchPerson

	b.ReportAllocs()
	for b.Loop() {
		rows.i = 0
		if err := StructScan(rows, &dest); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkSelect measures the full public Select path over 1000 rows.
func BenchmarkSelect(b *testing.B) {
	db := benchDB(b)
	const q = `SELECT id, first_name, last_name, email, score FROM bench_person`
	var dest []benchPerson

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		dest = dest[:0]
		if err := db.Select(&dest, q); err != nil {
			b.Fatal(err)
		}
		if len(dest) != benchRows {
			b.Fatalf("got %d rows", len(dest))
		}
	}
}

// BenchmarkSelectPtr measures Select into a slice of pointers.
func BenchmarkSelectPtr(b *testing.B) {
	db := benchDB(b)
	const q = `SELECT id, first_name, last_name, email, score FROM bench_person`
	var dest []*benchPerson

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		dest = dest[:0]
		if err := db.Select(&dest, q); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkRowsStructScan measures the manual Queryx/StructScan loop with a
// destination struct that is reused across rows.
func BenchmarkRowsStructScan(b *testing.B) {
	db := benchDB(b)
	const q = `SELECT id, first_name, last_name, email, score FROM bench_person`

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		rows, err := db.Queryx(q)
		if err != nil {
			b.Fatal(err)
		}
		var p benchPerson
		for rows.Next() {
			if err := rows.StructScan(&p); err != nil {
				b.Fatal(err)
			}
		}
		if err := rows.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkRowsStructScanAlloc measures StructScan with a fresh destination for
// every row, so the cached field pointers cannot be reused.
func BenchmarkRowsStructScanAlloc(b *testing.B) {
	db := benchDB(b)
	const q = `SELECT id, first_name, last_name, email, score FROM bench_person`

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		rows, err := db.Queryx(q)
		if err != nil {
			b.Fatal(err)
		}
		for rows.Next() {
			var p benchPerson
			if err := rows.StructScan(&p); err != nil {
				b.Fatal(err)
			}
		}
		if err := rows.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkGet measures a single-row lookup.
func BenchmarkGet(b *testing.B) {
	db := benchDB(b)
	const q = `SELECT id, first_name, last_name, email, score FROM bench_person WHERE id = ?`

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		var p benchPerson
		if err := db.Get(&p, q, 500); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkNamedExec measures ad-hoc named parameter binding plus execution.
func BenchmarkNamedExec(b *testing.B) {
	db := benchDB(b)
	const q = `UPDATE bench_person SET first_name = :first_name, last_name = :last_name,
		email = :email, score = :score WHERE id = :id`
	arg := benchPerson{ID: 1, FirstName: "First", LastName: "Last", Email: "user@example.com", Score: 1.5}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := db.NamedExec(q, arg); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkNamedExecStmt measures the prepared named statement path, which
// exercises binding without recompiling the query.
func BenchmarkNamedExecStmt(b *testing.B) {
	db := benchDB(b)
	const q = `UPDATE bench_person SET first_name = :first_name, last_name = :last_name,
		email = :email, score = :score WHERE id = :id`
	arg := benchPerson{ID: 1, FirstName: "First", LastName: "Last", Email: "user@example.com", Score: 1.5}
	stmt, err := db.PrepareNamed(q)
	if err != nil {
		b.Fatal(err)
	}
	defer stmt.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := stmt.Exec(arg); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkMapScan measures the map-based scan path.
func BenchmarkMapScan(b *testing.B) {
	db := benchDB(b)
	const q = `SELECT id, first_name, last_name, email, score FROM bench_person WHERE id = ?`
	dest := make(map[string]any, 5)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := db.QueryRowx(q, 500).MapScan(dest); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkSliceScan measures the slice-based scan path.
func BenchmarkSliceScan(b *testing.B) {
	db := benchDB(b)
	const q = `SELECT id, first_name, last_name, email, score FROM bench_person WHERE id = ?`

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := db.QueryRowx(q, 500).SliceScan(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkBindArray measures multi-row named binding, which exercises
// fixBound's VALUES-clause detection.
func BenchmarkBindArray(b *testing.B) {
	type row struct {
		A int
		B string
		C int
	}
	rows := make([]row, 100)
	for i := range rows {
		rows[i] = row{A: i, B: "value", C: i * 2}
	}
	const q = `INSERT INTO foo (a, b, c) VALUES (:a, :b, :c)`

	b.ReportAllocs()
	for b.Loop() {
		if _, _, err := bindArray(QUESTION, q, rows, mapper()); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkSelectIter measures the streaming iterator path over 1000 rows.
func BenchmarkSelectIter(b *testing.B) {
	db := benchDB(b)
	const q = `SELECT id, first_name, last_name, email, score FROM bench_person`

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		n := 0
		for _, err := range SelectIter[benchPerson](db, q) {
			if err != nil {
				b.Fatal(err)
			}
			n++
		}
		if n != benchRows {
			b.Fatalf("got %d rows", n)
		}
	}
}

// benchScanner is the hand-written (normally generated) RowScanner form of
// benchPerson.  Comparing it with benchPerson shows the ceiling of the
// reflection-free path.
type benchScanner benchPerson

func (p *benchScanner) ScanRow(rows *sql.Rows) error {
	return rows.Scan(&p.ID, &p.FirstName, &p.LastName, &p.Email, &p.Score)
}

// BenchmarkSelectAsStruct measures the generic fast path for a struct slice.
func BenchmarkSelectAsStruct(b *testing.B) {
	db := benchDB(b)
	const q = `SELECT id, first_name, last_name, email, score FROM bench_person`

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		out, err := SelectAs[benchPerson](db, q)
		if err != nil {
			b.Fatal(err)
		}
		if len(out) != benchRows {
			b.Fatalf("got %d rows", len(out))
		}
	}
}

// BenchmarkSelectAsStructPtr measures the generic path for a slice of pointers.
func BenchmarkSelectAsStructPtr(b *testing.B) {
	db := benchDB(b)
	const q = `SELECT id, first_name, last_name, email, score FROM bench_person`

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		out, err := SelectAs[*benchPerson](db, q)
		if err != nil {
			b.Fatal(err)
		}
		if len(out) != benchRows {
			b.Fatalf("got %d rows", len(out))
		}
	}
}

// BenchmarkSelectScanner measures SelectAs with a hand-written RowScanner,
// which performs no column-to-field reflection at all.
func BenchmarkSelectScanner(b *testing.B) {
	db := benchDB(b)
	const q = `SELECT id, first_name, last_name, email, score FROM bench_person`

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		out, err := SelectAs[benchScanner](db, q)
		if err != nil {
			b.Fatal(err)
		}
		if len(out) != benchRows {
			b.Fatalf("got %d rows", len(out))
		}
	}
}

// BenchmarkSelectAsScalar measures a scalar slice, which needs no field
// reflection at all.
func BenchmarkSelectAsScalar(b *testing.B) {
	db := benchDB(b)
	const q = `SELECT id FROM bench_person`

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		out, err := SelectAs[int64](db, q)
		if err != nil {
			b.Fatal(err)
		}
		if len(out) != benchRows {
			b.Fatalf("got %d rows", len(out))
		}
	}
}

// BenchmarkGetAsStruct measures the generic single-row struct path.
func BenchmarkGetAsStruct(b *testing.B) {
	db := benchDB(b)
	const q = `SELECT id, first_name, last_name, email, score FROM bench_person WHERE id = ?`

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		p, err := GetAs[benchPerson](db, q, 500)
		if err != nil {
			b.Fatal(err)
		}
		if p.ID != 500 {
			b.Fatalf("got id %d", p.ID)
		}
	}
}

// BenchmarkGetAsScalar measures the generic single-row scalar path, which
// avoids sqlx's column/field planning.
func BenchmarkGetAsScalar(b *testing.B) {
	db := benchDB(b)
	const q = `SELECT count(*) FROM bench_person`

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		n, err := GetAs[int64](db, q)
		if err != nil {
			b.Fatal(err)
		}
		if n != benchRows {
			b.Fatalf("got %d", n)
		}
	}
}

// BenchmarkRowsScanAs measures the generic per-row scanner with a reused Rows.
func BenchmarkRowsScanAs(b *testing.B) {
	db := benchDB(b)
	const q = `SELECT id, first_name, last_name, email, score FROM bench_person`

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		rows, err := db.Queryx(q)
		if err != nil {
			b.Fatal(err)
		}
		n := 0
		for rows.Next() {
			if _, err := rows.ScanAs[benchPerson](); err != nil {
				b.Fatal(err)
			}
			n++
		}
		if err := rows.Close(); err != nil {
			b.Fatal(err)
		}
		if n != benchRows {
			b.Fatalf("got %d rows", n)
		}
	}
}

// BenchmarkScanSliceGeneric isolates the generic slice scanner from the driver:
// fakeRows.Scan is a no-op, so this is the pure reflection/append cost.
func BenchmarkScanSliceGeneric(b *testing.B) {
	rows := &fakeRows{cols: benchColumns, n: benchRows}
	fields := mapper().FieldInfosByName(reflect.TypeFor[benchPerson](), benchColumns)
	values := make([]any, len(fields))

	b.ReportAllocs()
	for b.Loop() {
		rows.i = 0
		if _, err := scanSlice[benchPerson](rows, reflect.TypeFor[benchPerson](), false, fields, values); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkBindStructBinder measures named binding through a hand-written
// StructBinder, bypassing reflection.
func BenchmarkBindStructBinder(b *testing.B) {
	const q = `INSERT INTO foo (id, name) VALUES (:id, :name)`
	arg := binderPerson{ID: 30, Name: "Jason Moiron"}

	b.ReportAllocs()
	for b.Loop() {
		if _, _, err := bindStruct(DOLLAR, q, arg, mapper()); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkScanSliceIntoGeneric isolates the generic scanner while reusing the
// destination backing array, so it measures only the per-row work.
func BenchmarkScanSliceIntoGeneric(b *testing.B) {
	rows := &fakeRows{cols: benchColumns, n: benchRows}
	fields := mapper().FieldInfosByName(reflect.TypeFor[benchPerson](), benchColumns)
	values := make([]any, len(fields))
	out := make([]benchPerson, 0, benchRows)

	b.ReportAllocs()
	for b.Loop() {
		rows.i = 0
		var err error
		out, err = scanSliceInto(out[:0], rows, reflect.TypeFor[benchPerson](), false, fields, values)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkSelectAsIntoStruct reuses the destination backing array in addition
// to the generic scratch path, so neither the per-row reflection nor the result
// slice growth remains.
func BenchmarkSelectAsIntoStruct(b *testing.B) {
	db := benchDB(b)
	const q = `SELECT id, first_name, last_name, email, score FROM bench_person`
	out := make([]benchPerson, 0, benchRows)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		var err error
		out, err = SelectAsInto(out, db, q)
		if err != nil {
			b.Fatal(err)
		}
		if len(out) != benchRows {
			b.Fatalf("got %d rows", len(out))
		}
	}
}

// BenchmarkSelectScalar is the reflection-based scalar slice path, used as the
// baseline for SelectAs[int64].
func BenchmarkSelectScalar(b *testing.B) {
	db := benchDB(b)
	const q = `SELECT id FROM bench_person`
	var out []int64

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		out = out[:0]
		if err := db.Select(&out, q); err != nil {
			b.Fatal(err)
		}
		if len(out) != benchRows {
			b.Fatalf("got %d rows", len(out))
		}
	}
}

// BenchmarkGetScalar is the reflection-based single scalar path, used as the
// baseline for GetAs[int64].
func BenchmarkGetScalar(b *testing.B) {
	db := benchDB(b)
	const q = `SELECT count(*) FROM bench_person`

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		var n int64
		if err := db.Get(&n, q); err != nil {
			b.Fatal(err)
		}
		if n != benchRows {
			b.Fatalf("got %d", n)
		}
	}
}

// BenchmarkDirectSQLiteScan is the database/sql floor for the same query and
// row shape, with a preallocated destination slice.  Comparing it with
// BenchmarkSelect shows how much of the allocation is sqlx's responsibility.
func BenchmarkDirectSQLiteScan(b *testing.B) {
	db := benchDB(b)
	const q = `SELECT id, first_name, last_name, email, score FROM bench_person`
	var id int64
	var first, last, email string
	var score float64
	dest := []any{&id, &first, &last, &email, &score}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		rows, err := db.DB.Query(q)
		if err != nil {
			b.Fatal(err)
		}
		n := 0
		for rows.Next() {
			if err := rows.Scan(dest...); err != nil {
				b.Fatal(err)
			}
			n++
		}
		rows.Close()
		if n != benchRows {
			b.Fatalf("got %d rows", n)
		}
	}
}

// BenchmarkSelectAsIntoStructPtr is the pointer-slice variant with a reused
// destination, isolating the block allocation from result-slice growth.
func BenchmarkSelectAsIntoStructPtr(b *testing.B) {
	db := benchDB(b)
	const q = `SELECT id, first_name, last_name, email, score FROM bench_person`
	out := make([]*benchPerson, 0, benchRows)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		var err error
		out, err = SelectAsInto(out, db, q)
		if err != nil {
			b.Fatal(err)
		}
		if len(out) != benchRows {
			b.Fatalf("got %d rows", len(out))
		}
	}
}

// BenchmarkRowsScanAsPtr measures the generic per-row pointer scanner, which
// uses the same 64-element blocks as SelectPtr.
func BenchmarkRowsScanAsPtr(b *testing.B) {
	db := benchDB(b)
	const q = `SELECT id, first_name, last_name, email, score FROM bench_person`

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		rows, err := db.Queryx(q)
		if err != nil {
			b.Fatal(err)
		}
		n := 0
		for rows.Next() {
			p, err := rows.ScanAs[*benchPerson]()
			if err != nil {
				b.Fatal(err)
			}
			if p == nil {
				b.Fatal("nil pointer")
			}
			n++
		}
		if err := rows.Close(); err != nil {
			b.Fatal(err)
		}
		if n != benchRows {
			b.Fatalf("got %d rows", n)
		}
	}
}

// BenchmarkSelectAsIntoScalar shows the result-slice-free scalar path.
func BenchmarkSelectAsIntoScalar(b *testing.B) {
	db := benchDB(b)
	const q = `SELECT id FROM bench_person`
	out := make([]int64, 0, benchRows)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		var err error
		out, err = SelectAsInto(out, db, q)
		if err != nil {
			b.Fatal(err)
		}
		if len(out) != benchRows {
			b.Fatalf("got %d rows", len(out))
		}
	}
}

type noopQueryHook struct{}

func (noopQueryHook) BeforeQuery(context.Context, *QueryEvent) {}
func (noopQueryHook) AfterQuery(context.Context, *QueryEvent)  {}

// BenchmarkHookDisabled and BenchmarkHookEnabled measure the cost of the hook
// plumbing itself (the hook bodies are empty).
func BenchmarkHookDisabled(b *testing.B) {
	db := benchDB(b)
	const q = `SELECT id, first_name, last_name, email, score FROM bench_person`
	var dest []benchPerson

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		dest = dest[:0]
		if err := db.Select(&dest, q); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkHookEnabled(b *testing.B) {
	db := benchDB(b)
	db.SetQueryHook(noopQueryHook{})
	const q = `SELECT id, first_name, last_name, email, score FROM bench_person`
	var dest []benchPerson

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		dest = dest[:0]
		if err := db.Select(&dest, q); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkHookEnabledGet(b *testing.B) {
	db := benchDB(b)
	db.SetQueryHook(noopQueryHook{})
	const q = `SELECT id, first_name, last_name, email, score FROM bench_person WHERE id = ?`

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		var p benchPerson
		if err := db.Get(&p, q, 500); err != nil {
			b.Fatal(err)
		}
	}
}
