package sqlx

import (
	"context"
	"database/sql"
	"errors"
)

var (
	// ErrNilDB is returned by WithTx when db is nil.
	ErrNilDB = errors.New("sqlx: nil DB")
	// ErrNilTxFunc is returned by WithTx when fn is nil.
	ErrNilTxFunc = errors.New("sqlx: nil transaction function")
)

// WithTx runs fn inside a transaction.
//
//   - fn returns nil: the transaction is committed;
//   - fn returns an error: the transaction is rolled back and the error is
//     returned; a rollback error is joined with the original error;
//   - fn panics: the transaction is rolled back and the panic is re-raised.
//
// fn must not call Commit or Rollback itself.  Use a *Tx returned by BeginTxx
// directly if you need manual transaction control.
func WithTx(ctx context.Context, db *DB, opts *sql.TxOptions, fn func(*Tx) error) error {
	if db == nil {
		return ErrNilDB
	}
	if fn == nil {
		return ErrNilTxFunc
	}

	tx, err := db.BeginTxx(ctx, opts)
	if err != nil {
		return err
	}

	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback()
			panic(p)
		}
	}()

	if err := fn(tx); err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			return errors.Join(err, rbErr)
		}
		return err
	}
	return tx.Commit()
}
