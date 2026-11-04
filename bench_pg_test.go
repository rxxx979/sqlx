package sqlx

import (
	"fmt"
	"os"
	"testing"
)

// pgBenchDSN returns the DSN used for Postgres benchmarks.  Benchmarks are
// skipped when it is unset or set to "skip", matching the functional tests.
func pgBenchDSN(b *testing.B) string {
	b.Helper()
	dsn := os.Getenv("SQLX_POSTGRES_DSN")
	if dsn == "" || dsn == "skip" {
		b.Skip("SQLX_POSTGRES_DSN is not set")
	}
	return dsn
}

const pgBenchTable = "sqlx_bench_person"

// pgBenchDB creates a dedicated Postgres table with benchRows rows.  The
// connection is opened independently of the global pgdb used by the tests.
func pgBenchDB(b *testing.B) *DB {
	b.Helper()
	db, err := Open("pgx", pgBenchDSN(b))
	if err != nil {
		b.Fatal(err)
	}
	if err := db.Ping(); err != nil {
		db.Close()
		b.Fatal(err)
	}
	b.Cleanup(func() {
		db.MustExec("DROP TABLE IF EXISTS " + pgBenchTable)
		db.Close()
	})

	db.MustExec("DROP TABLE IF EXISTS " + pgBenchTable)
	db.MustExec(`CREATE TABLE ` + pgBenchTable + ` (
		id          bigint PRIMARY KEY,
		first_name  text,
		last_name   text,
		email       text,
		score       double precision
	)`)

	tx := db.MustBegin()
	stmt, err := tx.Preparex(`INSERT INTO ` + pgBenchTable + `
		(id, first_name, last_name, email, score) VALUES ($1, $2, $3, $4, $5)`)
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < benchRows; i++ {
		if _, err := stmt.Exec(int64(i), "First", "Last", fmt.Sprintf("user%d@example.com", i), float64(i)/10); err != nil {
			b.Fatal(err)
		}
	}
	if err := stmt.Close(); err != nil {
		b.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		b.Fatal(err)
	}
	return db
}

const pgBenchQuery = `SELECT id, first_name, last_name, email, score FROM ` + pgBenchTable

func BenchmarkPGSelect(b *testing.B) {
	db := pgBenchDB(b)
	var dest []benchPerson

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		dest = dest[:0]
		if err := db.Select(&dest, pgBenchQuery); err != nil {
			b.Fatal(err)
		}
		if len(dest) != benchRows {
			b.Fatalf("got %d rows", len(dest))
		}
	}
}

func BenchmarkPGSelectAsInto(b *testing.B) {
	db := pgBenchDB(b)
	out := make([]benchPerson, 0, benchRows)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		var err error
		out, err = SelectAsInto(out, db, pgBenchQuery)
		if err != nil {
			b.Fatal(err)
		}
		if len(out) != benchRows {
			b.Fatalf("got %d rows", len(out))
		}
	}
}

func BenchmarkPGSelectIter(b *testing.B) {
	db := pgBenchDB(b)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		n := 0
		for _, err := range SelectIter[benchPerson](db, pgBenchQuery) {
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

func BenchmarkPGRowsStructScan(b *testing.B) {
	db := pgBenchDB(b)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		rows, err := db.Queryx(pgBenchQuery)
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

func BenchmarkPGRowsScanAs(b *testing.B) {
	db := pgBenchDB(b)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		rows, err := db.Queryx(pgBenchQuery)
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

func BenchmarkPGSelectPtr(b *testing.B) {
	db := pgBenchDB(b)
	var dest []*benchPerson

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		dest = dest[:0]
		if err := db.Select(&dest, pgBenchQuery); err != nil {
			b.Fatal(err)
		}
		if len(dest) != benchRows {
			b.Fatalf("got %d rows", len(dest))
		}
	}
}

func BenchmarkPGGet(b *testing.B) {
	db := pgBenchDB(b)
	q := db.Rebind(`SELECT id, first_name, last_name, email, score FROM ` + pgBenchTable + ` WHERE id = ?`)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		var p benchPerson
		if err := db.Get(&p, q, 500); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPGNamedExec(b *testing.B) {
	db := pgBenchDB(b)
	q := `UPDATE ` + pgBenchTable + ` SET first_name = :first_name, last_name = :last_name,
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

func BenchmarkPGDirectScan(b *testing.B) {
	db := pgBenchDB(b)
	var id int64
	var first, last, email string
	var score float64
	dest := []any{&id, &first, &last, &email, &score}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		rows, err := db.DB.Query(pgBenchQuery)
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
		if err := rows.Close(); err != nil {
			b.Fatal(err)
		}
		if n != benchRows {
			b.Fatalf("got %d rows", n)
		}
	}
}

// BenchmarkPGSelectAsIntoScalar exercises the generic scalar path on Postgres.
func BenchmarkPGSelectAsIntoScalar(b *testing.B) {
	db := pgBenchDB(b)
	q := `SELECT id FROM ` + pgBenchTable
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
