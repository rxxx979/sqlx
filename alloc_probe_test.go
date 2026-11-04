package sqlx

import (
	"database/sql"
	"database/sql/driver"
	"io"
	"testing"
)

// allocProbeDriver returns 1000 rows of the benchPerson shape without
// allocating in Next, so benchmarks isolate database/sql + sqlx overhead.
type allocProbeDriver struct{}

func (allocProbeDriver) Open(string) (driver.Conn, error) { return allocProbeConn{}, nil }

type allocProbeConn struct{}

func (allocProbeConn) Prepare(string) (driver.Stmt, error) { return allocProbeStmt{}, nil }
func (allocProbeConn) Close() error                        { return nil }
func (allocProbeConn) Begin() (driver.Tx, error)           { return nil, io.EOF }

type allocProbeStmt struct{}

func (allocProbeStmt) Close() error                               { return nil }
func (allocProbeStmt) NumInput() int                              { return -1 }
func (allocProbeStmt) Exec([]driver.Value) (driver.Result, error) { return driver.RowsAffected(0), nil }
func (allocProbeStmt) Query([]driver.Value) (driver.Rows, error) {
	return &allocProbeRows{}, nil
}

type allocProbeRows struct{ i int }

func (*allocProbeRows) Columns() []string {
	return []string{"id", "first_name", "last_name", "email", "score"}
}
func (*allocProbeRows) Close() error { return nil }
func (r *allocProbeRows) Next(dest []driver.Value) error {
	if r.i >= 1000 {
		return io.EOF
	}
	dest[0] = int64(r.i)
	dest[1] = "First"
	dest[2] = "Last"
	dest[3] = "user@example.com"
	dest[4] = 1.5
	r.i++
	return nil
}

func init() {
	sql.Register("sqlx-alloc-probe", allocProbeDriver{})
}

func allocProbeDB(b *testing.B) *DB {
	b.Helper()
	db, err := Open("sqlx-alloc-probe", "")
	if err != nil {
		b.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	b.Cleanup(func() { db.Close() })
	return db
}

func BenchmarkAllocProbeDirect(b *testing.B) {
	db := allocProbeDB(b)
	const q = `SELECT id, first_name, last_name, email, score FROM bench_person`
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		rows, err := db.DB.Query(q)
		if err != nil {
			b.Fatal(err)
		}
		n := 0
		for rows.Next() {
			var id int64
			var first, last, email string
			var score float64
			if err := rows.Scan(&id, &first, &last, &email, &score); err != nil {
				b.Fatal(err)
			}
			n++
		}
		rows.Close()
		if n != 1000 {
			b.Fatalf("got %d", n)
		}
	}
}

func BenchmarkAllocProbeSelect(b *testing.B) {
	db := allocProbeDB(b)
	const q = `SELECT id, first_name, last_name, email, score FROM bench_person`
	var out []benchPerson
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		out = out[:0]
		if err := db.Select(&out, q); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAllocProbeSelectAsInto(b *testing.B) {
	db := allocProbeDB(b)
	const q = `SELECT id, first_name, last_name, email, score FROM bench_person`
	out := make([]benchPerson, 0, 1000)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		var err error
		out, err = SelectAsInto(out, db, q)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAllocProbeRowsStructScan(b *testing.B) {
	db := allocProbeDB(b)
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
		rows.Close()
	}
}

func BenchmarkAllocProbeDirectReuse(b *testing.B) {
	db := allocProbeDB(b)
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
		if n != 1000 {
			b.Fatalf("got %d", n)
		}
	}
}

func benchmarkDirectCols(b *testing.B, q string, dest []any) {
	db := benchDB(b)
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
			b.Fatalf("got %d", n)
		}
	}
}

func BenchmarkDirectSQLiteByColumnType(b *testing.B) {
	var id int64
	var score float64
	var s0, s1, s2 string

	b.Run("int", func(b *testing.B) {
		benchmarkDirectCols(b, `SELECT id FROM bench_person`, []any{&id})
	})
	b.Run("float", func(b *testing.B) {
		benchmarkDirectCols(b, `SELECT score FROM bench_person`, []any{&score})
	})
	b.Run("1text", func(b *testing.B) {
		benchmarkDirectCols(b, `SELECT first_name FROM bench_person`, []any{&s0})
	})
	b.Run("2text", func(b *testing.B) {
		benchmarkDirectCols(b, `SELECT first_name, last_name FROM bench_person`, []any{&s0, &s1})
	})
	b.Run("3text", func(b *testing.B) {
		benchmarkDirectCols(b, `SELECT first_name, last_name, email FROM bench_person`, []any{&s0, &s1, &s2})
	})
	b.Run("int+score", func(b *testing.B) {
		benchmarkDirectCols(b, `SELECT id, score FROM bench_person`, []any{&id, &score})
	})
	b.Run("int+3text+score", func(b *testing.B) {
		benchmarkDirectCols(b, `SELECT id, first_name, last_name, email, score FROM bench_person`, []any{&id, &s0, &s1, &s2, &score})
	})
}
