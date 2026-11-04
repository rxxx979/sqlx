package sqlx

// Named Query Support
//
//  * BindMap - bind query bindvars to map/struct args
//	* NamedExec, NamedQuery - named query w/ struct or map
//  * NamedStmt - a pre-compiled named query which is a prepared statement
//
// Internal Interfaces:
//
//  * compileNamedQuery - rebind a named query, returning a query and list of names
//  * bindArgs, bindMapArgs, bindAnyArgs - given a list of names, return an arglist
//
import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"unicode"
	"unicode/utf8"

	"github.com/rxxx979/sqlx/reflectx"
)

// NamedStmt is a prepared statement that executes named queries.  Prepare it
// how you would execute a NamedQuery, but pass in a struct or map when executing.
type NamedStmt struct {
	Params      []string
	QueryString string
	Stmt        *Stmt
}

// Close closes the named statement.
func (n *NamedStmt) Close() error {
	return n.Stmt.Close()
}

// Exec executes a named statement using the struct passed.
// Any named placeholder parameters are replaced with fields from arg.
func (n *NamedStmt) Exec(arg any) (sql.Result, error) {
	args, err := bindAnyArgs(n.Params, arg, n.Stmt.Mapper)
	if err != nil {
		return *new(sql.Result), err
	}
	return n.Stmt.Exec(args...)
}

// Query executes a named statement using the struct argument, returning rows.
// Any named placeholder parameters are replaced with fields from arg.
func (n *NamedStmt) Query(arg any) (*sql.Rows, error) {
	args, err := bindAnyArgs(n.Params, arg, n.Stmt.Mapper)
	if err != nil {
		return nil, err
	}
	return n.Stmt.Query(args...)
}

// QueryRow executes a named statement against the database.  Because sqlx cannot
// create a *sql.Row with an error condition pre-set for binding errors, sqlx
// returns a *sqlx.Row instead.
// Any named placeholder parameters are replaced with fields from arg.
func (n *NamedStmt) QueryRow(arg any) *Row {
	args, err := bindAnyArgs(n.Params, arg, n.Stmt.Mapper)
	if err != nil {
		return &Row{err: err}
	}
	return n.Stmt.QueryRowx(args...)
}

// MustExec execs a NamedStmt, panicing on error
// Any named placeholder parameters are replaced with fields from arg.
func (n *NamedStmt) MustExec(arg any) sql.Result {
	res, err := n.Exec(arg)
	if err != nil {
		panic(err)
	}
	return res
}

// Queryx using this NamedStmt
// Any named placeholder parameters are replaced with fields from arg.
func (n *NamedStmt) Queryx(arg any) (*Rows, error) {
	args, err := bindAnyArgs(n.Params, arg, n.Stmt.Mapper)
	if err != nil {
		return nil, err
	}
	return n.Stmt.Queryx(args...)
}

// QueryRowx this NamedStmt.  Because of limitations with QueryRow, this is
// an alias for QueryRow.
// Any named placeholder parameters are replaced with fields from arg.
func (n *NamedStmt) QueryRowx(arg any) *Row {
	return n.QueryRow(arg)
}

// Select using this NamedStmt
// Any named placeholder parameters are replaced with fields from arg.
func (n *NamedStmt) Select(dest any, arg any) error {
	rows, err := n.Queryx(arg)
	if err != nil {
		return err
	}
	// if something happens here, we want to make sure the rows are Closed
	defer rows.Close()
	return scanAll(rows, dest, false)
}

// Get using this NamedStmt
// Any named placeholder parameters are replaced with fields from arg.
func (n *NamedStmt) Get(dest any, arg any) error {
	r := n.QueryRowx(arg)
	return r.scanAny(dest, false)
}

// Unsafe creates an unsafe version of the NamedStmt
func (n *NamedStmt) Unsafe() *NamedStmt {
	r := &NamedStmt{Params: n.Params, Stmt: n.Stmt, QueryString: n.QueryString}
	r.Stmt.unsafe = true
	return r
}

// A union interface of preparer and binder, required to be able to prepare
// named statements (as the bindtype must be determined).
type namedPreparer interface {
	Preparer
	binder
}

func prepareNamed(p namedPreparer, query string) (*NamedStmt, error) {
	bindType := BindType(p.DriverName())
	q, args, err := compileNamedQueryCached(query, bindType)
	if err != nil {
		return nil, err
	}
	stmt, err := Preparex(p, q)
	if err != nil {
		return nil, err
	}
	return &NamedStmt{
		QueryString: q,
		Params:      slices.Clone(args),
		Stmt:        stmt,
	}, nil
}

// mapStringInterfaceType is the canonical type used to normalize named map
// arguments.  Caching it avoids a reflect.TypeFor lookup on the hot path.
var mapStringInterfaceType = reflect.TypeFor[map[string]any]()

// convertMapStringInterface attempts to convert v to map[string]any.
// Unlike v.(map[string]any), this function works on named types that
// are convertible to map[string]any as well.
func convertMapStringInterface(v any) (map[string]any, bool) {
	if m, ok := v.(map[string]any); ok {
		return m, true
	}
	if v == nil {
		return nil, false
	}
	t := reflect.TypeOf(v)
	if !t.ConvertibleTo(mapStringInterfaceType) {
		return nil, false
	}
	return reflect.ValueOf(v).Convert(mapStringInterfaceType).Interface().(map[string]any), true
}

func bindAnyArgs(names []string, arg any, m *reflectx.Mapper) ([]any, error) {
	// A generated or hand-written StructBinder bypasses reflection entirely.
	if b, ok := arg.(StructBinder); ok {
		if rv := reflect.ValueOf(arg); rv.Kind() == reflect.Pointer && rv.IsNil() {
			return nil, fmt.Errorf("nil pointer passed as named query argument: %#v", arg)
		}
		return b.BindArgs(names)
	}
	switch arg := arg.(type) {
	case map[string]any:
		return bindMapArgs(names, arg)
	case nil:
		return nil, errors.New("sqlx: nil argument passed to named query")
	}
	// Only map arguments need the convertibility check; structs (by far the
	// most common case) go straight to the field plan.
	if t := reflect.TypeOf(arg); t.Kind() == reflect.Map {
		if maparg, ok := convertMapStringInterface(arg); ok {
			return bindMapArgs(names, maparg)
		}
		return nil, fmt.Errorf("sqlx: unsupported map type: %T", arg)
	}
	return bindArgs(names, arg, m)
}

// bindArgs generates a list of interfaces from a struct argument, given a list
// of names to pull out of the struct.  Used by the public BindStruct interface.
//
// Field lookups use the mapper's cached column-to-field plans, so repeated
// executions against the same type do not rebuild traversals.
func bindArgs(names []string, arg any, m *reflectx.Mapper) ([]any, error) {
	// grab the indirected value of arg
	v := reflect.ValueOf(arg)
	for v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return nil, fmt.Errorf("nil pointer passed as named query argument: %#v", arg)
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return nil, fmt.Errorf("expected a struct or map argument, got %T", arg)
	}

	fields := m.FieldInfosByName(v.Type(), names)
	return appendStructArgs(make([]any, 0, len(names)), v, fields, names, arg)
}

// appendStructArgs appends the values of fields to dst.  v must be a struct;
// fields must have been resolved from v's type and names.  When the struct is
// addressable (slice elements, pointers) the cached field offsets are used
// instead of replaying the reflect traversal.
func appendStructArgs(dst []any, v reflect.Value, fields []*reflectx.FieldInfo, names []string, arg any) ([]any, error) {
	for i, fi := range fields {
		if fi == nil {
			return dst, fmt.Errorf("could not find name %s in %#v", names[i], arg)
		}
		// FieldByIndex is the fastest way to read a value out of an
		// addressable struct with a precomputed index path; NewAt+Elem is
		// measurably slower here because reading does not need an address.
		dst = append(dst, v.FieldByIndex(fi.Index).Interface())
	}
	return dst, nil
}

// like bindArgs, but for maps.
func bindMapArgs(names []string, arg map[string]any) ([]any, error) {
	arglist := make([]any, len(names))

	for i, name := range names {
		val, ok := arg[name]
		if !ok {
			return arglist, fmt.Errorf("could not find name %s in %#v", name, arg)
		}
		arglist[i] = val
	}
	return arglist, nil
}

// bindStruct binds a named parameter query with fields from a struct argument.
// The rules for binding field names to parameter names follow the same
// conventions as for StructScan, including obeying the `db` struct tags.
func bindStruct(bindType int, query string, arg any, m *reflectx.Mapper) (string, []any, error) {
	bound, names, err := compileNamedQueryCached(query, bindType)
	if err != nil {
		return "", []any{}, err
	}

	arglist, err := bindAnyArgs(names, arg, m)
	if err != nil {
		return "", []any{}, err
	}

	return bound, arglist, nil
}

func findMatchingClosingBracketIndex(s string) int {
	count := 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '(':
			count++
		case ')':
			count--
			if count == 0 {
				return i
			}
		}
	}
	return 0
}

// findValuesClause locates the "(" in the first `)\s*VALUES\s*(` sequence,
// case-insensitively.  It replaces a regular expression that used to be run for
// every bindArray call.
func findValuesClause(s string) (int, bool) {
	for i := 0; i < len(s); i++ {
		if s[i] != ')' {
			continue
		}
		j := i + 1
		for j < len(s) && isSpaceByte(s[j]) {
			j++
		}
		if j+len("values") > len(s) || !strings.EqualFold(s[j:j+len("values")], "values") {
			continue
		}
		j += len("values")
		for j < len(s) && isSpaceByte(s[j]) {
			j++
		}
		if j < len(s) && s[j] == '(' {
			return j, true
		}
	}
	return 0, false
}

func isSpaceByte(b byte) bool {
	switch b {
	case ' ', '\t', '\n', '\r', '\f', '\v':
		return true
	}
	return false
}

func fixBound(bound string, loop int) string {
	openingBracketIndex, ok := findValuesClause(bound)
	// defensive guard when "VALUES (...)" not found
	if !ok {
		return bound
	}

	index := findMatchingClosingBracketIndex(bound[openingBracketIndex:])
	// defensive guard. must have closing bracket
	if index == 0 {
		return bound
	}
	closingBracketIndex := openingBracketIndex + index + 1

	var buffer strings.Builder
	buffer.Grow(len(bound) + (loop-1)*(closingBracketIndex-openingBracketIndex))

	buffer.WriteString(bound[0:closingBracketIndex])
	for i := 0; i < loop-1; i++ {
		buffer.WriteByte(',')
		buffer.WriteString(bound[openingBracketIndex:closingBracketIndex])
	}
	buffer.WriteString(bound[closingBracketIndex:])
	return buffer.String()
}

// bindArray binds a named parameter query with fields from an array or slice of
// structs argument.
func bindArray(bindType int, query string, arg any, m *reflectx.Mapper) (string, []any, error) {
	// do the initial binding with QUESTION;  if bindType is not question,
	// we can rebind it at the end.
	bound, names, err := compileNamedQueryCached(query, QUESTION)
	if err != nil {
		return "", []any{}, err
	}
	arrayValue := reflect.ValueOf(arg)
	arrayLen := arrayValue.Len()
	if arrayLen == 0 {
		return "", []any{}, fmt.Errorf("length of array is 0: %#v", arg)
	}
	arglist := make([]any, 0, len(names)*arrayLen)

	// Fast path for slices and arrays of structs (or pointers to structs):
	// resolve the column plan once and reuse it for every element.
	if elemType := reflectx.Deref(arrayValue.Type().Elem()); elemType.Kind() == reflect.Struct {
		fields := m.FieldInfosByName(elemType, names)
		for i, fi := range fields {
			if fi == nil {
				return "", []any{}, fmt.Errorf("could not find name %s in %#v", names[i], arg)
			}
		}
		for i := range arrayLen {
			ev := arrayValue.Index(i)
			for ev.Kind() == reflect.Pointer {
				if ev.IsNil() {
					return "", []any{}, fmt.Errorf("nil element at index %d in %#v", i, arg)
				}
				ev = ev.Elem()
			}
			var err error
			arglist, err = appendStructArgs(arglist, ev, fields, names, arg)
			if err != nil {
				return "", []any{}, err
			}
		}
	} else {
		for i := range arrayLen {
			elemArglist, err := bindAnyArgs(names, arrayValue.Index(i).Interface(), m)
			if err != nil {
				return "", []any{}, err
			}
			arglist = append(arglist, elemArglist...)
		}
	}
	if arrayLen > 1 {
		bound = fixBound(bound, arrayLen)
	}
	// adjust binding type if we weren't on question
	if bindType != QUESTION {
		bound = Rebind(bindType, bound)
	}
	return bound, arglist, nil
}

// bindMap binds a named parameter query with a map of arguments.
func bindMap(bindType int, query string, args map[string]any) (string, []any, error) {
	bound, names, err := compileNamedQueryCached(query, bindType)
	if err != nil {
		return "", []any{}, err
	}

	arglist, err := bindMapArgs(names, args)
	return bound, arglist, err
}

// -- Compilation of Named Queries

// Allow digits and letters in bind params;  additionally runes are
// checked against underscores, meaning that bind params can have be
// alphanumeric with underscores.  Mind the difference between unicode
// digits and numbers, where '5' is a digit but '五' is not.
var allowedBindRunes = []*unicode.RangeTable{unicode.Letter, unicode.Digit}

// compiledNamedQuery is an immutable compilation result, safe to share between
// goroutines.
type compiledNamedQuery struct {
	query string
	names []string
}

type compiledNamedQueryKey struct {
	query    string
	bindType int
}

var (
	// namedQueryCache memoizes the (pure) named-query compiler.  Ad-hoc
	// NamedExec/NamedQuery calls with the same SQL text (the overwhelmingly
	// common case) then pay only the binding cost.  Entries are immutable and
	// therefore safe to share; Params of a NamedStmt are cloned by
	// prepareNamed before being exposed to callers.
	namedQueryCache     sync.Map // compiledNamedQueryKey -> *compiledNamedQuery
	namedQueryCacheSize atomic.Int64
)

// maxNamedQueryCache bounds the compiler cache so dynamically generated SQL
// cannot grow it without limit.  Once full, new queries are simply compiled
// without being cached.
const maxNamedQueryCache = 4096

func compileNamedQueryCached(query string, bindType int) (string, []string, error) {
	key := compiledNamedQueryKey{query: query, bindType: bindType}
	if v, ok := namedQueryCache.Load(key); ok {
		c := v.(*compiledNamedQuery)
		return c.query, c.names, nil
	}

	q, names, err := compileNamedQuery(query, bindType)
	if err != nil {
		return q, names, err
	}
	if namedQueryCacheSize.Load() < maxNamedQueryCache {
		c := &compiledNamedQuery{query: q, names: names}
		if _, loaded := namedQueryCache.LoadOrStore(key, c); !loaded {
			namedQueryCacheSize.Add(1)
		}
	}
	return q, names, nil
}

// bindNameRune reports whether the rune starting at s[i] can appear in a
// named parameter, and how many bytes it occupies.  ASCII, which is what
// practically every query uses, is checked without decoding.
func bindNameRune(s string, i int) (ok bool, size int) {
	b := s[i]
	if b < utf8.RuneSelf {
		return b == '_' || b == '.' ||
			('a' <= b && b <= 'z') ||
			('A' <= b && b <= 'Z') ||
			('0' <= b && b <= '9'), 1
	}
	r, size := utf8.DecodeRuneInString(s[i:])
	return unicode.IsOneOf(allowedBindRunes, r), size
}

// compile a NamedQuery into an unbound query (using the '?' bindvar) and
// a list of names.
//
// Names are byte-range slices of the input instead of byte-by-byte copies, and
// the scanner advances by rune so non-ASCII parameter names work correctly.
func compileNamedQuery(qs string, bindType int) (query string, names []string, err error) {
	names = make([]string, 0, 8)
	rebound := make([]byte, 0, len(qs)+8)

	inName := false
	currentVar := 1
	nameStart := 0

	for i := 0; i < len(qs); {
		b := qs[i]

		// a ':' while we're in a name is an error
		if b == ':' {
			// if this is the second ':' in a '::' escape sequence, append a ':'
			if inName && i > 0 && qs[i-1] == ':' {
				rebound = append(rebound, ':')
				inName = false
				i++
				continue
			}
			if inName {
				return "", names, errors.New("unexpected `:` while reading named param at " + strconv.Itoa(i))
			}
			inName = true
			nameStart = i + 1
			i++
			continue
		}

		// a ':=' is an assignment operator, not a parameter
		if inName && i == nameStart && b == '=' {
			rebound = append(rebound, ':', '=')
			inName = false
			i++
			continue
		}

		if inName {
			isName, size := bindNameRune(qs, i)
			if isName && i+size < len(qs) {
				// still inside the name
				i += size
				continue
			}

			end := i
			if isName {
				// the final rune of the query is part of the name
				end = i + size
			}
			name := qs[nameStart:end]
			names = append(names, name)

			// add a proper bindvar for the bindType
			switch bindType {
			// oracle only supports named type bind vars even for positional
			case NAMED:
				rebound = append(rebound, ':')
				rebound = append(rebound, name...)
			case QUESTION, UNKNOWN:
				rebound = append(rebound, '?')
			case DOLLAR:
				rebound = append(rebound, '$')
				rebound = strconv.AppendInt(rebound, int64(currentVar), 10)
				currentVar++
			case AT:
				rebound = append(rebound, '@', 'p')
				rebound = strconv.AppendInt(rebound, int64(currentVar), 10)
				currentVar++
			}

			inName = false
			// add this rune to the string unless it was part of the name
			if end == i {
				rebound = append(rebound, qs[i:i+size]...)
			}
			i += size
			continue
		}

		// this is a normal byte and should just go onto the rebound query
		rebound = append(rebound, b)
		i++
	}

	return string(rebound), names, nil
}

// BindNamed binds a struct or a map to a query with named parameters.
// DEPRECATED: use sqlx.Named` instead of this, it may be removed in future.
func BindNamed(bindType int, query string, arg any) (string, []any, error) {
	return bindNamedMapper(bindType, query, arg, mapper())
}

// Named takes a query using named parameters and an argument and
// returns a new query with a list of args that can be executed by
// a database.  The return value uses the `?` bindvar.
func Named(query string, arg any) (string, []any, error) {
	return bindNamedMapper(QUESTION, query, arg, mapper())
}

func bindNamedMapper(bindType int, query string, arg any, m *reflectx.Mapper) (string, []any, error) {
	if arg == nil {
		return "", nil, errors.New("sqlx: nil argument passed to named query")
	}
	t := reflect.TypeOf(arg)
	k := t.Kind()
	switch {
	case k == reflect.Map && t.Key().Kind() == reflect.String:
		m, ok := convertMapStringInterface(arg)
		if !ok {
			return "", nil, fmt.Errorf("sqlx.bindNamedMapper: unsupported map type: %T", arg)
		}
		return bindMap(bindType, query, m)
	case k == reflect.Array || k == reflect.Slice:
		return bindArray(bindType, query, arg, m)
	default:
		return bindStruct(bindType, query, arg, m)
	}
}

// NamedQuery binds a named query and then runs Query on the result using the
// provided Ext (sqlx.Tx, sqlx.Db).  It works with both structs and with
// map[string]any types.
func NamedQuery(e Ext, query string, arg any) (*Rows, error) {
	q, args, err := bindNamedMapper(BindType(e.DriverName()), query, arg, mapperFor(e))
	if err != nil {
		return nil, err
	}
	return e.Queryx(q, args...)
}

// NamedExec uses BindStruct to get a query executable by the driver and
// then runs Exec on the result.  Returns an error from the binding
// or the query execution itself.
func NamedExec(e Ext, query string, arg any) (sql.Result, error) {
	q, args, err := bindNamedMapper(BindType(e.DriverName()), query, arg, mapperFor(e))
	if err != nil {
		return nil, err
	}
	return e.Exec(q, args...)
}
