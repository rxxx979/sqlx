package sqlx

import (
	"context"
	"fmt"
	"iter"
	"reflect"

	"github.com/rxxx979/sqlx/reflectx"
)

// GetAs is the generic form of Get: it queries a single row and scans it into
// a new T, returning it by value.
//
// T may be a struct, a single-column scannable type, or a pointer to either.
// Scalar and sql.Scanner destinations go straight to Rows.Scan, without any
// field reflection.  GetAs returns the zero value of T together with any error,
// including sql.ErrNoRows.
//
//	p, err := sqlx.GetAs[Person](db, "SELECT * FROM person WHERE id = ?", 42)
func GetAs[T any](q Queryer, query string, args ...any) (T, error) {
	return getAsRow[T](q.QueryRowx(query, args...))
}

// GetAsContext is the context-aware form of GetAs.
func GetAsContext[T any](ctx context.Context, q QueryerContext, query string, args ...any) (T, error) {
	return getAsRow[T](q.QueryRowxContext(ctx, query, args...))
}

func getAsRow[T any](r *Row) (T, error) {
	var v T
	t := reflect.TypeFor[T]()
	base := reflectx.Deref(t)

	// Scalar and sql.Scanner destinations can go straight to Scan.  The type
	// check happens once; no reflection is involved in the row itself.
	if isScannable(base) {
		if err := r.Scan(&v); err != nil {
			var zero T
			return zero, err
		}
		return v, nil
	}

	if t.Kind() == reflect.Pointer {
		p := reflect.New(base)
		if err := r.StructScan(p.Interface()); err != nil {
			var zero T
			return zero, err
		}
		return p.Interface().(T), nil
	}

	if err := r.StructScan(&v); err != nil {
		var zero T
		return zero, err
	}
	return v, nil
}

// SelectAs is the generic form of Select: it queries rows and scans each one
// into a new []T.  T may be a struct, a single-column scannable type, or a
// pointer to either.
//
// For a non-pointer struct T, SelectAs uses a single scratch T per query: the
// field addresses are resolved once, then every row is a Scan plus a direct
// append.  That removes all per-row reflection for the slice bookkeeping and
// the column-to-field work.
//
//	people, err := sqlx.SelectAs[Person](db, "SELECT * FROM person")
func SelectAs[T any](q Queryer, query string, args ...any) ([]T, error) {
	rows, err := q.Queryx(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return selectAsRows[T](rows)
}

// SelectAsContext is the context-aware form of SelectAs.
func SelectAsContext[T any](ctx context.Context, q QueryerContext, query string, args ...any) ([]T, error) {
	rows, err := q.QueryxContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return selectAsRows[T](rows)
}

// SelectAsInto is like SelectAs, but reuses the backing array of dest instead
// of allocating a new result slice.  The returned slice has its length reset to
// the number of rows; pass a slice with spare capacity (for example, a previous
// result) to make repeated queries allocation-free.
func SelectAsInto[T any](dest []T, q Queryer, query string, args ...any) ([]T, error) {
	rows, err := q.Queryx(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return selectAsRowsInto(dest, rows)
}

// SelectAsIntoContext is the context-aware form of SelectAsInto.
func SelectAsIntoContext[T any](ctx context.Context, dest []T, q QueryerContext, query string, args ...any) ([]T, error) {
	rows, err := q.QueryxContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return selectAsRowsInto(dest, rows)
}

func selectAsRows[T any](rows *Rows) ([]T, error) {
	return selectAsRowsInto(make([]T, 0, 16), rows)
}

func selectAsRowsInto[T any](dest []T, rows *Rows) ([]T, error) {
	t := reflect.TypeFor[T]()
	base := reflectx.Deref(t)

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	// Scalars, sql.Scanner implementations and other non-struct destinations
	// only need Scan; append is direct because T is known at compile time.
	if isScannable(base) {
		if len(columns) > 1 {
			return nil, fmt.Errorf("non-struct dest type %s with >1 columns (%d)", base.Kind(), len(columns))
		}
		out := dest[:0]
		sp := reflect.New(reflect.TypeFor[T]()).Interface().(*T)
		var zero T
		for rows.Next() {
			*sp = zero
			if err := rows.Scan(sp); err != nil {
				return nil, err
			}
			out = append(out, *sp)
		}
		return out, rows.Err()
	}

	isPtr := t.Kind() == reflect.Pointer

	// A generated or hand-written RowScanner bypasses field planning.
	if reflect.PointerTo(base).Implements(_rowScannerType) {
		return scanSliceScannerInto(dest[:0], rows, base, isPtr)
	}

	fields, values, err := rows.structPlan(base, columns)
	if err != nil {
		return nil, err
	}
	return scanSliceInto(dest[:0], rows, base, isPtr, fields, values)
}

// scanSlice scans rows into []T.  For non-pointer T it scans into one reusable
// scratch value and appends a copy per row, so field pointers are built once per
// query instead of once per row.
func scanSlice[T any](rows rowsi, base reflect.Type, isPtr bool, fields []*reflectx.FieldInfo, values []any) ([]T, error) {
	return scanSliceInto(make([]T, 0, 16), rows, base, isPtr, fields, values)
}

// scanSliceInto is scanSlice with a caller-supplied destination, allowing the
// backing array to be reused across queries.
func scanSliceInto[T any](out []T, rows rowsi, base reflect.Type, isPtr bool, fields []*reflectx.FieldInfo, values []any) ([]T, error) {
	if isPtr {
		sliceType := reflect.SliceOf(base)
		var block reflect.Value
		blockIdx := ptrBlockSize
		for rows.Next() {
			if blockIdx == ptrBlockSize {
				block = reflect.MakeSlice(sliceType, ptrBlockSize, ptrBlockSize)
				blockIdx = 0
			}
			elem := block.Index(blockIdx)
			blockIdx++
			if err := fieldPointers(elem, fields, values); err != nil {
				return nil, err
			}
			if err := rows.Scan(values...); err != nil {
				return nil, err
			}
			out = append(out, elem.Addr().Interface().(T))
		}
		return out, rows.Err()
	}

	sp := reflect.New(reflect.TypeFor[T]()).Interface().(*T)
	indirect := needsPerRowPointers(fields)
	if !indirect {
		if err := fieldPointers(reflect.ValueOf(sp).Elem(), fields, values); err != nil {
			return nil, err
		}
	}
	var zero T
	for rows.Next() {
		*sp = zero
		if indirect {
			if err := fieldPointers(reflect.ValueOf(sp).Elem(), fields, values); err != nil {
				return nil, err
			}
		}
		if err := rows.Scan(values...); err != nil {
			return nil, err
		}
		out = append(out, *sp)
	}
	return out, rows.Err()
}

func scanSliceScannerInto[T any](out []T, rows *Rows, base reflect.Type, isPtr bool) ([]T, error) {
	raw := rows.Rows

	if isPtr {
		sliceType := reflect.SliceOf(base)
		var block reflect.Value
		blockIdx := ptrBlockSize
		for rows.Next() {
			if blockIdx == ptrBlockSize {
				block = reflect.MakeSlice(sliceType, ptrBlockSize, ptrBlockSize)
				blockIdx = 0
			}
			ptr := block.Index(blockIdx).Addr()
			blockIdx++
			if err := ptr.Interface().(RowScanner).ScanRow(raw); err != nil {
				return nil, err
			}
			out = append(out, ptr.Interface().(T))
		}
		return out, rows.Err()
	}

	var v T
	scanner := any(&v).(RowScanner)
	var zero T
	for rows.Next() {
		v = zero
		if err := scanner.ScanRow(raw); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// structPlan resolves the column-to-field plan and scratch pointer slice for a
// struct destination type.
func (r *Rows) structPlan(base reflect.Type, columns []string) ([]*reflectx.FieldInfo, []any, error) {
	m := r.Mapper
	if m == nil {
		m = mapper()
	}
	fields := m.FieldInfosByName(base, columns)
	if f, err := missingFields(fields); err != nil && !r.unsafe {
		return nil, nil, fmt.Errorf("missing destination name %s in %s", columns[f], base)
	}
	return fields, make([]any, len(columns)), nil
}

type scanAsMode uint8

const (
	scanAsScalar scanAsMode = iota
	scanAsStruct
	scanAsScanner
	scanAsPointer
)

// scanAsState caches the scratch value, field plan and scan mode used by
// Rows.ScanAs, so repeated calls for the same T do no type analysis at all.
type scanAsState[T any] struct {
	typ         reflect.Type
	mode        scanAsMode
	sp          *T
	fields      []*reflectx.FieldInfo
	values      []any
	scanner     RowScanner
	scannerType bool
	indirect    bool
	sliceType   reflect.Type
	block       reflect.Value
	blockIdx    int
}

func newScanAsState[T any](r *Rows, t reflect.Type) (*scanAsState[T], error) {
	base := reflectx.Deref(t)
	st := &scanAsState[T]{typ: t}

	// Scalars, sql.Scanner implementations and non-struct values: Scan only.
	if base.Kind() != reflect.Struct || isScannable(base) {
		st.mode = scanAsScalar
		st.sp = reflect.New(t).Interface().(*T)
		return st, nil
	}

	scannerType := reflect.PointerTo(base).Implements(_rowScannerType)

	if t.Kind() == reflect.Pointer {
		st.mode = scanAsPointer
		st.scannerType = scannerType
		st.sliceType = reflect.SliceOf(base)
		if !scannerType {
			columns, err := r.Columns()
			if err != nil {
				return nil, err
			}
			fields, values, err := r.structPlan(base, columns)
			if err != nil {
				return nil, err
			}
			st.fields, st.values = fields, values
		}
		return st, nil
	}

	st.sp = reflect.New(t).Interface().(*T)
	if scannerType {
		st.mode = scanAsScanner
		st.scanner = any(st.sp).(RowScanner)
		return st, nil
	}

	columns, err := r.Columns()
	if err != nil {
		return nil, err
	}
	fields, values, err := r.structPlan(base, columns)
	if err != nil {
		return nil, err
	}
	st.mode = scanAsStruct
	st.fields, st.values = fields, values
	st.indirect = needsPerRowPointers(fields)
	if !st.indirect {
		if err := fieldPointers(reflect.ValueOf(st.sp).Elem(), fields, values); err != nil {
			return nil, err
		}
	}
	return st, nil
}

// ScanAs scans the current row into a T, choosing the direct Scan path for
// scalar/scanner types and the structural path for structs.
//
// For a non-pointer struct T the scratch value, field pointers and scan mode are
// cached on Rows, so a loop over ScanAs does no per-row type analysis or field
// reflection.
func (r *Rows) ScanAs[T any]() (T, error) {
	t := reflect.TypeFor[T]()
	st, err := r.scanAsState[T](t)
	if err != nil {
		var zero T
		return zero, err
	}

	switch st.mode {
	case scanAsScalar:
		var zero T
		*st.sp = zero
		if err := r.Scan(st.sp); err != nil {
			var zero T
			return zero, err
		}
		return *st.sp, nil

	case scanAsPointer:
		if st.blockIdx == 0 || st.blockIdx == ptrBlockSize {
			st.block = reflect.MakeSlice(st.sliceType, ptrBlockSize, ptrBlockSize)
			st.blockIdx = 0
		}
		elem := st.block.Index(st.blockIdx)
		st.blockIdx++
		ptr := elem.Addr()
		if st.scannerType {
			if err := ptr.Interface().(RowScanner).ScanRow(r.Rows); err != nil {
				var zero T
				return zero, err
			}
		} else {
			if err := fieldPointers(elem, st.fields, st.values); err != nil {
				var zero T
				return zero, err
			}
			if err := r.Scan(st.values...); err != nil {
				var zero T
				return zero, err
			}
		}
		return ptr.Interface().(T), nil

	case scanAsScanner:
		var zero T
		*st.sp = zero
		if err := st.scanner.ScanRow(r.Rows); err != nil {
			var zero T
			return zero, err
		}
		return *st.sp, nil

	default: // scanAsStruct
		var zero T
		*st.sp = zero
		if st.indirect {
			if err := fieldPointers(reflect.ValueOf(st.sp).Elem(), st.fields, st.values); err != nil {
				var zero T
				return zero, err
			}
		}
		if err := r.Scan(st.values...); err != nil {
			var zero T
			return zero, err
		}
		return *st.sp, nil
	}
}

func (r *Rows) scanAsState[T any](t reflect.Type) (*scanAsState[T], error) {
	st, _ := r.scanAs.(*scanAsState[T])
	if st != nil && st.typ == t {
		return st, nil
	}
	st, err := newScanAsState[T](r, t)
	if err != nil {
		return nil, err
	}
	r.scanAs = st
	return st, nil
}

// ScanAs scans the row into a T, choosing the direct Scan path for
// scalar/scanner types and the structural path for structs.
func (r *Row) ScanAs[T any]() (T, error) {
	var v T
	t := reflect.TypeFor[T]()
	base := reflectx.Deref(t)

	if t.Kind() == reflect.Pointer && base.Kind() == reflect.Struct && !isScannable(base) {
		p := reflect.New(base)
		if err := r.StructScan(p.Interface()); err != nil {
			var zero T
			return zero, err
		}
		return p.Interface().(T), nil
	}
	if base.Kind() != reflect.Struct || isScannable(base) {
		if err := r.Scan(&v); err != nil {
			var zero T
			return zero, err
		}
		return v, nil
	}
	if err := r.StructScan(&v); err != nil {
		var zero T
		return zero, err
	}
	return v, nil
}

// SelectIter streams the results of a query as an iterator of T values.  It is
// the allocation-friendly, early-termination-friendly alternative to SelectAs
// for large result sets:
//
//	for p, err := range sqlx.SelectIter[Person](db, "SELECT * FROM person") {
//		if err != nil {
//			return err
//		}
//		process(p)
//	}
//
// The underlying rows are closed when iteration stops, including on an early
// break.  Scan and query errors are delivered as the second value and
// terminate the sequence.  For struct T, field pointers are resolved once and
// reused for all rows.
func SelectIter[T any](q Queryer, query string, args ...any) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		if err := checkIterType[T](); err != nil {
			var zero T
			yield(zero, err)
			return
		}
		rows, err := q.Queryx(query, args...)
		if err != nil {
			var zero T
			yield(zero, err)
			return
		}
		defer rows.Close()

		scan, err := newRowScanner[T](rows)
		if err != nil {
			var zero T
			yield(zero, err)
			return
		}
		for rows.Next() {
			v, err := scan()
			if err != nil {
				var zero T
				yield(zero, err)
				return
			}
			if !yield(v, nil) {
				return
			}
		}
		if err := rows.Err(); err != nil {
			var zero T
			yield(zero, err)
		}
	}
}

// SelectIterContext is the context-aware form of SelectIter.
func SelectIterContext[T any](ctx context.Context, q QueryerContext, query string, args ...any) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		if err := checkIterType[T](); err != nil {
			var zero T
			yield(zero, err)
			return
		}
		rows, err := q.QueryxContext(ctx, query, args...)
		if err != nil {
			var zero T
			yield(zero, err)
			return
		}
		defer rows.Close()

		scan, err := newRowScanner[T](rows)
		if err != nil {
			var zero T
			yield(zero, err)
			return
		}
		for rows.Next() {
			v, err := scan()
			if err != nil {
				var zero T
				yield(zero, err)
				return
			}
			if !yield(v, nil) {
				return
			}
		}
		if err := rows.Err(); err != nil {
			var zero T
			yield(zero, err)
		}
	}
}

func checkIterType[T any]() error {
	t := reflect.TypeFor[T]()
	if t.Kind() == reflect.Pointer && t.Elem().Kind() == reflect.Pointer {
		return fmt.Errorf("sqlx: nested pointer type %s cannot be scanned by SelectIter", t)
	}
	return nil
}

// newRowScanner chooses the cheapest scan strategy for T once per query.
func newRowScanner[T any](rows *Rows) (func() (T, error), error) {
	t := reflect.TypeFor[T]()
	base := reflectx.Deref(t)

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	if isScannable(base) {
		if len(columns) > 1 {
			return nil, fmt.Errorf("non-struct dest type %s with >1 columns (%d)", base.Kind(), len(columns))
		}
		sp := reflect.New(t).Interface().(*T)
		var zero T
		return func() (T, error) {
			*sp = zero
			err := rows.Scan(sp)
			return *sp, err
		}, nil
	}

	if reflect.PointerTo(base).Implements(_rowScannerType) {
		raw := rows.Rows
		if t.Kind() == reflect.Pointer {
			return func() (T, error) {
				p := reflect.New(base)
				err := p.Interface().(RowScanner).ScanRow(raw)
				return p.Interface().(T), err
			}, nil
		}
		var v T
		scanner := any(&v).(RowScanner)
		var zero T
		return func() (T, error) {
			v = zero
			err := scanner.ScanRow(raw)
			return v, err
		}, nil
	}

	fields, values, err := rows.structPlan(base, columns)
	if err != nil {
		return nil, err
	}

	if t.Kind() == reflect.Pointer {
		return func() (T, error) {
			p := reflect.New(base)
			if err := fieldPointers(p, fields, values); err != nil {
				var zero T
				return zero, err
			}
			if err := rows.Scan(values...); err != nil {
				var zero T
				return zero, err
			}
			return p.Interface().(T), nil
		}, nil
	}

	sp := reflect.New(t).Interface().(*T)
	indirect := needsPerRowPointers(fields)
	if !indirect {
		if err := fieldPointers(reflect.ValueOf(sp).Elem(), fields, values); err != nil {
			return nil, err
		}
	}
	var zero T
	return func() (T, error) {
		*sp = zero
		if indirect {
			if err := fieldPointers(reflect.ValueOf(sp).Elem(), fields, values); err != nil {
				var zero T
				return zero, err
			}
		}
		if err := rows.Scan(values...); err != nil {
			var zero T
			return zero, err
		}
		return *sp, nil
	}, nil
}
