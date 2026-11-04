package sqlx

import (
	"context"
	"database/sql"
	"errors"
	"testing"
)

func personName(t *testing.T, db *DB, id int) string {
	t.Helper()
	var name string
	if err := db.Get(&name, `SELECT name FROM people WHERE id = ?`, id); err != nil {
		t.Fatal(err)
	}
	return name
}

func TestWithTxCommit(t *testing.T) {
	db := newIterDB(t)

	err := WithTx(context.Background(), db, nil, func(tx *Tx) error {
		_, err := tx.Exec(`UPDATE people SET name = ? WHERE id = ?`, "committed", 1)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := personName(t, db, 1); got != "committed" {
		t.Fatalf("name = %q, want committed", got)
	}
}

func TestWithTxRollback(t *testing.T) {
	db := newIterDB(t)
	sentinel := errors.New("business error")

	err := WithTx(context.Background(), db, nil, func(tx *Tx) error {
		if _, err := tx.Exec(`UPDATE people SET name = ? WHERE id = ?`, "rolled back", 1); err != nil {
			return err
		}
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("err = %v, want sentinel", err)
	}
	if got := personName(t, db, 1); got != "p1" {
		t.Fatalf("name = %q, want p1 after rollback", got)
	}
}

func TestWithTxPanicRollsBack(t *testing.T) {
	db := newIterDB(t)

	func() {
		defer func() {
			if p := recover(); p != "boom" {
				t.Fatalf("recovered %v, want boom", p)
			}
		}()
		_ = WithTx(context.Background(), db, nil, func(tx *Tx) error {
			if _, err := tx.Exec(`UPDATE people SET name = ? WHERE id = ?`, "panic", 1); err != nil {
				t.Fatal(err)
			}
			panic("boom")
		})
	}()

	if got := personName(t, db, 1); got != "p1" {
		t.Fatalf("name = %q, want p1 after panic rollback", got)
	}
}

func TestWithTxNilArgs(t *testing.T) {
	db := newIterDB(t)
	ctx := context.Background()

	if err := WithTx(ctx, nil, nil, func(*Tx) error { return nil }); !errors.Is(err, ErrNilDB) {
		t.Fatalf("nil db err = %v", err)
	}
	if err := WithTx(ctx, db, nil, nil); !errors.Is(err, ErrNilTxFunc) {
		t.Fatalf("nil fn err = %v", err)
	}
}

func TestWithTxCanceledContext(t *testing.T) {
	db := newIterDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	called := false
	err := WithTx(ctx, db, nil, func(tx *Tx) error {
		called = true
		return nil
	})
	if err == nil {
		t.Fatal("expected canceled context error")
	}
	if called {
		t.Fatal("fn should not be called when Begin fails")
	}
}

func TestWithTxRollbackErrorJoined(t *testing.T) {
	db := newIterDB(t)
	sentinel := errors.New("sentinel")

	// Committing inside fn makes WithTx's rollback return sql.ErrTxDone.
	// Both errors should be visible through errors.Is.
	err := WithTx(context.Background(), db, nil, func(tx *Tx) error {
		if _, err := tx.Exec(`UPDATE people SET name = ? WHERE id = ?`, "joined", 1); err != nil {
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("err = %v, want sentinel", err)
	}
	if !errors.Is(err, sql.ErrTxDone) {
		t.Fatalf("err = %v, want joined sql.ErrTxDone", err)
	}
}

func TestWithTxHookEvents(t *testing.T) {
	db, rec := newHookDB(t)

	if err := WithTx(context.Background(), db, nil, func(tx *Tx) error {
		_, err := tx.Exec(`UPDATE people SET name = ? WHERE id = ?`, "hook", 1)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	_, _, events := rec.snapshot()
	if len(events) != 3 || events[0].Op != OpBegin || events[1].Op != OpExec || events[2].Op != OpCommit {
		t.Fatalf("commit hook events: %+v", events)
	}

	rec.reset()
	sentinel := errors.New("hook rollback")
	if err := WithTx(context.Background(), db, nil, func(tx *Tx) error {
		if _, err := tx.Exec(`UPDATE people SET name = ? WHERE id = ?`, "hook", 1); err != nil {
			return err
		}
		return sentinel
	}); !errors.Is(err, sentinel) {
		t.Fatalf("err = %v", err)
	}
	_, _, events = rec.snapshot()
	if len(events) != 3 || events[0].Op != OpBegin || events[1].Op != OpExec || events[2].Op != OpRollback {
		t.Fatalf("rollback hook events: %+v", events)
	}
}
