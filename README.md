# sqlx

[![license](http://img.shields.io/badge/license-MIT-red.svg?style=flat)](https://raw.githubusercontent.com/jmoiron/sqlx/master/LICENSE)

Personal fork of [jmoiron/sqlx](https://github.com/jmoiron/sqlx), targeting Go 1.27.3+.
Not intended for upstream PRs or as a `sqlc` replacement.

sqlx is a library which provides a set of extensions on go's standard
`database/sql` library.  The sqlx versions of `sql.DB`, `sql.TX`, `sql.Stmt`,
et al. all leave the underlying interfaces untouched, so that their interfaces
are a superset on the standard ones.  This makes it relatively painless to
integrate existing codebases using database/sql with sqlx.

Major additional concepts are:

* Marshal rows into structs (with embedded struct support), maps, and slices
* Named parameter support including prepared statements
* `Get` and `Select` to go quickly from query to struct/slice

In addition to the [godoc API documentation](http://godoc.org/github.com/rxxx979/sqlx),
there is also some [user documentation](http://jmoiron.github.io/sqlx/) that
explains how to use `database/sql` along with sqlx.

> **This fork** targets **Go 1.27.3+**, rewrites sqlx's reflection hot paths,
> and adds generic/iterator fast paths, query hooks and `WithTx`.

## Recent Changes

1.2.0 (unreleased):

* New `WithTx(ctx, db, opts, fn)` helper: commits on success, rolls back on
  error, rolls back and re-panics on panic. Rollback errors are joined with the
  original error instead of being discarded.
* Transaction begin/commit/rollback are reported through `QueryHook`.

1.1.0 (query hooks):

* New `sqlx.QueryHook` with `BeforeQuery` / `AfterQuery`, plus `HookFuncs` and
  `MultiHook` adapters.
* `DB.SetQueryHook` observes `Queryx`, `QueryRowx`, `Exec`, `Preparex` and
  their Context variants; `Tx`, `Conn` and `Stmt` inherit the same hook.
* Hook events include SQL, args, duration, `RowsScanned`, `RowsAffected`,
  `Err` and hook-private `Data`.
* `Queryx` events finish on `Rows.Close`; `QueryRowx` events finish on
  `Row.Scan`; transaction events cover `Beginx`/`BeginTxx` and
  `Commit`/`Rollback`.
* Disabled hooks cost an atomic load; enabled hooks add roughly 2 allocations
  per query.

1.0.0 (first tagged fork release):

* Requires Go 1.27.3; module and dependencies updated
* Test/benchmark drivers: pure-Go `modernc.org/sqlite` and `pgx/v5`
  (`mattn/go-sqlite3` and `lib/pq` removed)
* **Faster scanning**: cached column plans, byte-offset field access, scratch-
  value scanning, and a lock-free mapper cache. `Select` allocates within 3
  allocations / 1000 rows of hand-written `database/sql`; non-generic
  `StructScan` is ~6x faster and generic scanning with a reused destination is
  ~20x faster than the original implementation (2002 → 1–2 allocations).
* **Faster named queries**: compiled-query cache and cached bind plans make
  `bindStruct` ~4.7x faster (11 → 1 allocs) and `bindArray` ~1.9x faster.
* New: `GetAs[T]`, `SelectAs[T]`, `GetAsContext[T]`, `SelectAsContext[T]`,
  `SelectAsInto[T]`, `SelectAsIntoContext[T]`, `SelectIter[T]`,
  `SelectIterContext[T]`, `(*Rows).ScanAs[T]`, `(*Row).ScanAs[T]`.
* Generic fast paths: scalar scans skip reflection entirely; struct `SelectAs`
  resolves field addresses once per query and appends directly. With a reused
  destination, pure scanning is ~20x faster than the original implementation.
* `[]*T` scans now allocate one block per 64 rows instead of one object per row
  (pure scan: 2001 → 33 allocations for 1000 rows).
* New opt-in zero-reflection interfaces: `RowScanner` (scan a row without
  planning) and `StructBinder` (bind named args without reflection).
* Allocation note: with SQLite, `Select` is within 3 allocations per 1000 rows
  of a hand-written `database/sql` scan; most remaining allocations come from
  the driver. Use `SelectAsInto` to avoid the result-slice allocation.
* Named parameters now support non-ASCII names (`:名前`).
* `Rows.StructScan` supports changing destination types and returns an error
  (instead of panicking) for nil/non-pointer destinations.
* Benchmark suite added in [bench_test.go](bench_test.go).

1.3.0:

* `sqlx.DB.Connx(context.Context) *sqlx.Conn`
* `sqlx.BindDriver(driverName, bindType)`
* support for `[]map[string]interface{}` to do "batch" insertions
* allocation & perf improvements for `sqlx.In`

DB.Connx returns an `sqlx.Conn`, which is an `sql.Conn`-alike consistent with
sqlx's wrapping of other types.

`BindDriver` allows users to control the bindvars that sqlx will use for drivers,
and add new drivers at runtime.  This results in a very slight performance hit
when resolving the driver into a bind type (~40ns per call), but it allows users
to specify what bindtype their driver uses even when sqlx has not been updated
to know about it by default.

### Backwards Compatibility

This fork requires **Go 1.27.3 or newer**.  API changes are additive where
possible.  Notable behavioral fixes: `Rows.StructScan` returns an error instead
of panicking for nil/non-pointer destinations, `Select` clears reused elements,
named parameters support non-ASCII names, and `GzippedText.Value` reports gzip
write errors.

Versioning is done with Go modules.  Breaking changes (eg. removing deprecated API)
will get major version number bumps.

## install

    go get github.com/rxxx979/sqlx

```go
import "github.com/rxxx979/sqlx"
```

## benchmarks

SQLite benchmarks use `modernc.org/sqlite` and run without external services:

    make bench
    # or
    go test -run '^$' -bench . -benchmem ./...

PostgreSQL benchmarks use `pgx` and are skipped unless `SQLX_POSTGRES_DSN` is set:

    export SQLX_POSTGRES_DSN='postgres://user:pass@localhost:5432/db?sslmode=disable'
    make bench-pg

The PG benchmark suite creates and drops its own `sqlx_bench_person` table.

## issues

Row headers can be ambiguous (`SELECT 1 AS a, 2 AS a`), and the result of
`Columns()` does not fully qualify column names in queries like:

```sql
SELECT a.id, a.name, b.id, b.name FROM foos AS a JOIN foos AS b ON a.parent = b.id;
```

making a struct or map destination ambiguous.  Use `AS` in your queries
to give columns distinct names, `rows.Scan` to scan them manually, or 
`SliceScan` to get a slice of results.

## usage

Below is an example which shows some common use cases for sqlx.  Check 
[sqlx_test.go](sqlx_test.go) for more
usage.


```go
package main

import (
    "database/sql"
    "fmt"
    "log"
    
    _ "github.com/jackc/pgx/v5/stdlib"
    "github.com/rxxx979/sqlx"
)

var schema = []string{
    `CREATE TABLE person (
        first_name text,
        last_name text,
        email text
    )`,
    `CREATE TABLE place (
        country text,
        city text NULL,
        telcode integer
    )`,
}

type Person struct {
    FirstName string `db:"first_name"`
    LastName  string `db:"last_name"`
    Email     string
}

type Place struct {
    Country string
    City    sql.NullString
    TelCode int
}

func main() {
    // this Pings the database trying to connect
    // use sqlx.Open() for sql.Open() semantics
    db, err := sqlx.Connect("pgx", "postgres://user:pass@localhost:5432/dbname?sslmode=disable")
    if err != nil {
        log.Fatalln(err)
    }

    // pgx uses the extended protocol by default and does not accept multiple
    // statements in one Exec; run each statement separately.
    for _, stmt := range schema {
        db.MustExec(stmt)
    }
    
    tx := db.MustBegin()
    tx.MustExec("INSERT INTO person (first_name, last_name, email) VALUES ($1, $2, $3)", "Jason", "Moiron", "jmoiron@jmoiron.net")
    tx.MustExec("INSERT INTO person (first_name, last_name, email) VALUES ($1, $2, $3)", "John", "Doe", "johndoeDNE@gmail.net")
    tx.MustExec("INSERT INTO place (country, city, telcode) VALUES ($1, $2, $3)", "United States", "New York", "1")
    tx.MustExec("INSERT INTO place (country, telcode) VALUES ($1, $2)", "Hong Kong", "852")
    tx.MustExec("INSERT INTO place (country, telcode) VALUES ($1, $2)", "Singapore", "65")
    // Named queries can use structs, so if you have an existing struct (i.e. person := &Person{}) that you have populated, you can pass it in as &person
    tx.NamedExec("INSERT INTO person (first_name, last_name, email) VALUES (:first_name, :last_name, :email)", &Person{"Jane", "Citizen", "jane.citzen@example.com"})
    tx.Commit()

    // Query the database, storing results in a []Person (wrapped in []interface{})
    people := []Person{}
    db.Select(&people, "SELECT * FROM person ORDER BY first_name ASC")
    jason, john := people[0], people[1]

    fmt.Printf("%#v\n%#v", jason, john)
    // Person{FirstName:"Jason", LastName:"Moiron", Email:"jmoiron@jmoiron.net"}
    // Person{FirstName:"John", LastName:"Doe", Email:"johndoeDNE@gmail.net"}

    // You can also get a single result, a la QueryRow
    jason = Person{}
    err = db.Get(&jason, "SELECT * FROM person WHERE first_name=$1", "Jason")
    fmt.Printf("%#v\n", jason)
    // Person{FirstName:"Jason", LastName:"Moiron", Email:"jmoiron@jmoiron.net"}

    // if you have null fields and use SELECT *, you must use sql.Null* in your struct
    places := []Place{}
    err = db.Select(&places, "SELECT * FROM place ORDER BY telcode ASC")
    if err != nil {
        fmt.Println(err)
        return
    }
    usa, singsing, honkers := places[0], places[1], places[2]
    
    fmt.Printf("%#v\n%#v\n%#v\n", usa, singsing, honkers)
    // Place{Country:"United States", City:sql.NullString{String:"New York", Valid:true}, TelCode:1}
    // Place{Country:"Singapore", City:sql.NullString{String:"", Valid:false}, TelCode:65}
    // Place{Country:"Hong Kong", City:sql.NullString{String:"", Valid:false}, TelCode:852}

    // Loop through rows using only one struct
    place := Place{}
    rows, err := db.Queryx("SELECT * FROM place")
    for rows.Next() {
        err := rows.StructScan(&place)
        if err != nil {
            log.Fatalln(err)
        } 
        fmt.Printf("%#v\n", place)
    }
    // Place{Country:"United States", City:sql.NullString{String:"New York", Valid:true}, TelCode:1}
    // Place{Country:"Hong Kong", City:sql.NullString{String:"", Valid:false}, TelCode:852}
    // Place{Country:"Singapore", City:sql.NullString{String:"", Valid:false}, TelCode:65}

    // Named queries, using `:name` as the bindvar.  Automatic bindvar support
    // which takes into account the dbtype based on the driverName on sqlx.Open/Connect
    _, err = db.NamedExec(`INSERT INTO person (first_name,last_name,email) VALUES (:first,:last,:email)`, 
        map[string]interface{}{
            "first": "Bin",
            "last": "Smuth",
            "email": "bensmith@allblacks.nz",
    })

    // Selects Mr. Smith from the database
    rows, err = db.NamedQuery(`SELECT * FROM person WHERE first_name=:fn`, map[string]interface{}{"fn": "Bin"})

    // Named queries can also use structs.  Their bind names follow the same rules
    // as the name -> db mapping, so struct fields are lowercased and the `db` tag
    // is taken into consideration.
    rows, err = db.NamedQuery(`SELECT * FROM person WHERE first_name=:first_name`, jason)
    
    
    // batch insert
    
    // batch insert with structs
    personStructs := []Person{
        {FirstName: "Ardie", LastName: "Savea", Email: "asavea@ab.co.nz"},
        {FirstName: "Sonny Bill", LastName: "Williams", Email: "sbw@ab.co.nz"},
        {FirstName: "Ngani", LastName: "Laumape", Email: "nlaumape@ab.co.nz"},
    }

    _, err = db.NamedExec(`INSERT INTO person (first_name, last_name, email)
        VALUES (:first_name, :last_name, :email)`, personStructs)

    // batch insert with maps
    personMaps := []map[string]interface{}{
        {"first_name": "Ardie", "last_name": "Savea", "email": "asavea@ab.co.nz"},
        {"first_name": "Sonny Bill", "last_name": "Williams", "email": "sbw@ab.co.nz"},
        {"first_name": "Ngani", "last_name": "Laumape", "email": "nlaumape@ab.co.nz"},
    }

    _, err = db.NamedExec(`INSERT INTO person (first_name, last_name, email)
        VALUES (:first_name, :last_name, :email)`, personMaps)
}
```
