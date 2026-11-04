// Package reflectx implements extensions to the standard reflect lib suitable
// for implementing marshalling and unmarshalling packages.  The main Mapper type
// allows for Go-compatible named attribute access, including accessing embedded
// struct attributes and the ability to use  functions and struct tags to
// customize field names.
package reflectx

import (
	"reflect"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
)

// A FieldInfo is metadata for a struct field.
type FieldInfo struct {
	Index    []int
	Path     string
	Field    reflect.StructField
	Zero     reflect.Value
	Name     string
	Options  map[string]string
	Embedded bool
	Children []*FieldInfo
	Parent   *FieldInfo

	// Offset is the byte offset of the field from the base address of the
	// struct at the root of the traversal.  It is only meaningful when Direct
	// is true; when Direct is false the traversal passes through an embedded
	// pointer and Offset must not be used.
	//
	// Offset lets callers that can prove a destination is addressable avoid
	// replaying the reflect field traversal for every row.  See the sqlx
	// package for an example.
	Offset uintptr

	// Direct reports whether Offset is valid, that is whether the field can be
	// addressed by adding Offset to the address of the root struct.  Embedded
	// structs accessed by value are direct; traversals that go through an
	// embedded pointer (which may be nil) are not.
	Direct bool
}

// A StructMap is an index of field metadata for a struct.
type StructMap struct {
	Tree  *FieldInfo
	Index []*FieldInfo
	Paths map[string]*FieldInfo
	Names map[string]*FieldInfo
}

// GetByPath returns a *FieldInfo for a given string path.
func (f StructMap) GetByPath(path string) *FieldInfo {
	return f.Paths[path]
}

// GetByTraversal returns a *FieldInfo for a given integer path.  It is
// analogous to reflect.FieldByIndex, but using the cached traversal
// rather than re-executing the reflect machinery each time.
func (f StructMap) GetByTraversal(index []int) *FieldInfo {
	if len(index) == 0 {
		return nil
	}

	tree := f.Tree
	for _, i := range index {
		if i >= len(tree.Children) || tree.Children[i] == nil {
			return nil
		}
		tree = tree.Children[i]
	}
	return tree
}

// planKey identifies a cached column-to-field traversal plan.  The hash is
// stored alongside the type so that the common case (a hot query with a stable
// column list) can be served with a single map load and a cheap comparison,
// without allocating a composite string key.
type planKey struct {
	t reflect.Type
	h uint64
}

type planEntry struct {
	names  []string
	fields []*FieldInfo
}

// maxPlanCache bounds the number of cached traversal plans per Mapper.  Plans
// are keyed by column names, which usually come from a finite set of queries;
// the bound protects Mapper instances that are used with dynamically generated
// SQL from growing without limit.
const maxPlanCache = 4096

// Mapper is a general purpose mapper of names to struct fields.  A Mapper
// behaves like most marshallers in the standard library, obeying a field tag
// for name mapping but also providing a basic transform function.
//
// A Mapper is safe for concurrent use by multiple goroutines.  The zero value
// is ready to use and maps fields by their Go names with no tag support, but
// NewMapper and friends should normally be preferred.
type Mapper struct {
	cache      sync.Map // reflect.Type -> *StructMap
	plans      sync.Map // planKey -> *planEntry
	planCount  atomic.Int64
	tagName    string
	tagMapFunc func(string) string
	mapFunc    func(string) string
}

// NewMapper returns a new mapper using the tagName as its struct field tag.
// If tagName is the empty string, it is ignored.
func NewMapper(tagName string) *Mapper {
	return &Mapper{
		tagName: tagName,
	}
}

// NewMapperTagFunc returns a new mapper which contains a mapper for field names
// AND a mapper for tag values.  This is useful for tags like json which can
// have values like "name,omitempty".
func NewMapperTagFunc(tagName string, mapFunc, tagMapFunc func(string) string) *Mapper {
	return &Mapper{
		tagName:    tagName,
		mapFunc:    mapFunc,
		tagMapFunc: tagMapFunc,
	}
}

// NewMapperFunc returns a new mapper which optionally obeys a field tag and
// a struct field name mapper func given by f.  Tags will take precedence, but
// for any other field, the mapped name will be f(field.Name)
func NewMapperFunc(tagName string, f func(string) string) *Mapper {
	return &Mapper{
		tagName: tagName,
		mapFunc: f,
	}
}

// TypeMap returns a mapping of field strings to int slices representing
// the traversal down the struct to reach the field.
//
// The result is cached for the lifetime of the Mapper, so callers must treat
// the returned StructMap as immutable.
func (m *Mapper) TypeMap(t reflect.Type) *StructMap {
	if v, ok := m.cache.Load(t); ok {
		return v.(*StructMap)
	}
	mapping := getMapping(t, m.tagName, m.mapFunc, m.tagMapFunc)
	v, _ := m.cache.LoadOrStore(t, mapping)
	return v.(*StructMap)
}

// FieldMap returns the mapper's mapping of field names to reflect values.  Panics
// if v's Kind is not Struct, or v is not Indirectable to a struct kind.
func (m *Mapper) FieldMap(v reflect.Value) map[string]reflect.Value {
	v = reflect.Indirect(v)
	mustBe(v, reflect.Struct)

	r := map[string]reflect.Value{}
	tm := m.TypeMap(v.Type())
	for tagName, fi := range tm.Names {
		r[tagName] = FieldByIndexes(v, fi.Index)
	}
	return r
}

// FieldByName returns a field by its mapped name as a reflect.Value.
// Panics if v's Kind is not Struct or v is not Indirectable to a struct Kind.
// Returns zero Value if the name is not found.
func (m *Mapper) FieldByName(v reflect.Value, name string) reflect.Value {
	v = reflect.Indirect(v)
	mustBe(v, reflect.Struct)

	tm := m.TypeMap(v.Type())
	fi, ok := tm.Names[name]
	if !ok {
		return v
	}
	return FieldByIndexes(v, fi.Index)
}

// FieldsByName returns a slice of values corresponding to the slice of names
// for the value.  Panics if v's Kind is not Struct or v is not Indirectable
// to a struct Kind.  Returns zero Value for each name not found.
func (m *Mapper) FieldsByName(v reflect.Value, names []string) []reflect.Value {
	v = reflect.Indirect(v)
	mustBe(v, reflect.Struct)

	tm := m.TypeMap(v.Type())
	vals := make([]reflect.Value, len(names))
	for i, name := range names {
		fi, ok := tm.Names[name]
		if !ok {
			vals[i] = reflect.Value{}
		} else {
			vals[i] = FieldByIndexes(v, fi.Index)
		}
	}
	return vals
}

// TraversalsByName returns a slice of int slices which represent the struct
// traversals for each mapped name.  Panics if t is not a struct or Indirectable
// to a struct.  Returns empty int slice for each name not found.
func (m *Mapper) TraversalsByName(t reflect.Type, names []string) [][]int {
	t = Deref(t)
	mustBe(t, reflect.Struct)
	tm := m.TypeMap(t)

	r := make([][]int, len(names))
	for i, name := range names {
		if fi, ok := tm.Names[name]; ok {
			r[i] = fi.Index
		} else {
			r[i] = []int{}
		}
	}
	return r
}

// TraversalsByNameFunc traverses the mapped names and calls fn with the index of
// each name and the struct traversal represented by that name. Panics if t is not
// a struct or Indirectable to a struct. Returns the first error returned by fn or nil.
func (m *Mapper) TraversalsByNameFunc(t reflect.Type, names []string, fn func(int, []int) error) error {
	t = Deref(t)
	mustBe(t, reflect.Struct)
	tm := m.TypeMap(t)
	for i, name := range names {
		if fi, ok := tm.Names[name]; ok {
			if err := fn(i, fi.Index); err != nil {
				return err
			}
		} else {
			if err := fn(i, nil); err != nil {
				return err
			}
		}
	}
	return nil
}

// FieldInfosByName returns the FieldInfo for each mapped name in names, with a
// nil entry for names that do not map to a field.  It is the FieldInfo
// equivalent of TraversalsByName and is the form used by sqlx's row scanning
// fast path.
//
// The returned slice is shared and must be treated as read-only.  Plans are
// cached per Mapper keyed by the destination type and column names, which makes
// repeated queries against the same type essentially allocation-free.
func (m *Mapper) FieldInfosByName(t reflect.Type, names []string) []*FieldInfo {
	t = Deref(t)
	mustBe(t, reflect.Struct)

	key := planKey{t: t, h: hashNames(names)}
	if v, ok := m.plans.Load(key); ok {
		e := v.(*planEntry)
		if slices.Equal(e.names, names) {
			return e.fields
		}
		// Extremely unlikely hash collision: skip the cache rather than
		// returning the wrong fields.
		return m.fieldInfos(t, names)
	}

	fields := m.fieldInfos(t, names)
	if m.planCount.Load() >= maxPlanCache {
		return fields
	}
	e := &planEntry{names: slices.Clone(names), fields: fields}
	if _, loaded := m.plans.LoadOrStore(key, e); !loaded {
		m.planCount.Add(1)
	}
	return fields
}

func (m *Mapper) fieldInfos(t reflect.Type, names []string) []*FieldInfo {
	tm := m.TypeMap(t)
	fields := make([]*FieldInfo, len(names))
	for i, name := range names {
		fields[i] = tm.Names[name]
	}
	return fields
}

// hashNames computes a 64 bit FNV-1a hash over a list of names, including a
// separator between names so that ["ab","c"] and ["a","bc"] differ.
func hashNames(names []string) uint64 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	h := uint64(offset64)
	for _, name := range names {
		for i := 0; i < len(name); i++ {
			h ^= uint64(name[i])
			h *= prime64
		}
		h ^= 0xff
		h *= prime64
	}
	return h
}

// FieldByIndexes returns a value for the field given by the struct traversal
// for the given value.
func FieldByIndexes(v reflect.Value, indexes []int) reflect.Value {
	for _, i := range indexes {
		v = reflect.Indirect(v).Field(i)
		// if this is a pointer and it's nil, allocate a new value and set it
		switch v.Kind() {
		case reflect.Pointer:
			if v.IsNil() {
				v.Set(reflect.New(Deref(v.Type())))
			}
		case reflect.Map:
			if v.IsNil() {
				v.Set(reflect.MakeMap(v.Type()))
			}
		}
	}
	return v
}

// FieldByIndexesReadOnly returns a value for a particular struct traversal,
// but is not concerned with allocating nil pointers because the value is
// going to be used for reading and not setting.
func FieldByIndexesReadOnly(v reflect.Value, indexes []int) reflect.Value {
	for _, i := range indexes {
		v = reflect.Indirect(v).Field(i)
	}
	return v
}

// Deref is Indirect for reflect.Types
func Deref(t reflect.Type) reflect.Type {
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	return t
}

// -- helpers & utilities --

type kinder interface {
	Kind() reflect.Kind
}

// mustBe checks a value against a kind, panicing with a reflect.ValueError
// if the kind isn't that which is required.
func mustBe(v kinder, expected reflect.Kind) {
	if k := v.Kind(); k != expected {
		panic(&reflect.ValueError{Method: methodName(), Kind: k})
	}
}

// methodName returns the caller of the function calling methodName
func methodName() string {
	pc, _, _, _ := runtime.Caller(2)
	f := runtime.FuncForPC(pc)
	if f == nil {
		return "unknown method"
	}
	return f.Name()
}

type typeQueue struct {
	t  reflect.Type
	fi *FieldInfo
	pp string // Parent path
}

// A copying append that creates a new slice each time.
func apnd(is []int, i int) []int {
	x := make([]int, len(is)+1)
	copy(x, is)
	x[len(x)-1] = i
	return x
}

type mapf func(string) string

// parseName parses the tag and the target name for the given field using
// the tagName (eg 'json' for `json:"foo"` tags), mapFunc for mapping the
// field's name to a target name, and tagMapFunc for mapping the tag to
// a target name.
func parseName(field reflect.StructField, tagName string, mapFunc, tagMapFunc mapf) (tag, fieldName string) {
	// first, set the fieldName to the field's name
	fieldName = field.Name
	// if a mapFunc is set, use that to override the fieldName
	if mapFunc != nil {
		fieldName = mapFunc(fieldName)
	}

	// if there's no tag to look for, return the field name
	if tagName == "" {
		return "", fieldName
	}

	// StructTag.Lookup handles malformed tags the same way Get does (returning
	// nothing) while avoiding the extra allocation and substring search that
	// strings.Contains(string(field.Tag), tagName+":") used to perform.
	tag, ok := field.Tag.Lookup(tagName)
	if !ok {
		return "", fieldName
	}

	// if we have a mapper function, call it on the whole tag
	// XXX: this is a change from the old version, which pulled out the name
	// before the tagMapFunc could be run, but I think this is the right way
	if tagMapFunc != nil {
		tag = tagMapFunc(tag)
	}

	// finally, split the options from the name
	fieldName, _, _ = strings.Cut(tag, ",")

	return tag, fieldName
}

// parseOptions parses options out of a tag string, skipping the name.
func parseOptions(tag string) map[string]string {
	options := make(map[string]string)
	_, rest, ok := strings.Cut(tag, ",")
	if !ok {
		return options
	}
	for rest != "" {
		var opt string
		opt, rest, _ = strings.Cut(rest, ",")
		if opt == "" {
			continue
		}
		if k, v, ok := strings.Cut(opt, "="); ok {
			options[k] = v
		} else {
			options[opt] = ""
		}
	}
	return options
}

// getMapping returns a mapping for the t type, using the tagName, mapFunc and
// tagMapFunc to determine the canonical names of fields.
func getMapping(t reflect.Type, tagName string, mapFunc, tagMapFunc mapf) *StructMap {
	m := []*FieldInfo{}

	root := &FieldInfo{Direct: true}
	queue := []typeQueue{{Deref(t), root, ""}}

QueueLoop:
	for len(queue) != 0 {
		// pop the first item off of the queue
		tq := queue[0]
		queue = queue[1:]

		// ignore recursive field
		for p := tq.fi.Parent; p != nil; p = p.Parent {
			if tq.fi.Field.Type == p.Field.Type {
				continue QueueLoop
			}
		}

		nChildren := 0
		if tq.t.Kind() == reflect.Struct {
			nChildren = tq.t.NumField()
		}
		tq.fi.Children = make([]*FieldInfo, nChildren)

		// The direct parent can be addressed by adding an offset to the root
		// pointer unless it is (or is reached through) an embedded pointer.
		parentDirect := tq.fi.Direct
		if tq.fi.Field.Type != nil && tq.fi.Field.Type.Kind() == reflect.Pointer {
			parentDirect = false
		}

		if nChildren == 0 {
			continue
		}

		// iterate through all of its fields
		fieldPos := 0
		for f := range tq.t.Fields() {
			// parse the tag and the target name using the mapping options for this field
			tag, name := parseName(f, tagName, mapFunc, tagMapFunc)

			// if the name is "-", disabled via a tag, skip it
			if name == "-" {
				fieldPos++
				continue
			}

			fi := FieldInfo{
				Field:   f,
				Name:    name,
				Zero:    reflect.New(f.Type).Elem(),
				Options: parseOptions(tag),
				Direct:  parentDirect,
			}
			if parentDirect {
				fi.Offset = tq.fi.Offset + f.Offset
			}

			// if the path is empty this path is just the name
			if tq.pp == "" {
				fi.Path = fi.Name
			} else {
				fi.Path = tq.pp + "." + fi.Name
			}

			// skip unexported fields
			if len(f.PkgPath) != 0 && !f.Anonymous {
				fieldPos++
				continue
			}

			// bfs search of anonymous embedded structs
			if f.Anonymous {
				pp := tq.pp
				if tag != "" {
					pp = fi.Path
				}

				fi.Embedded = true
				fi.Index = apnd(tq.fi.Index, fieldPos)
				nChildren := 0
				ft := Deref(f.Type)
				if ft.Kind() == reflect.Struct {
					nChildren = ft.NumField()
				}
				fi.Children = make([]*FieldInfo, nChildren)
				queue = append(queue, typeQueue{Deref(f.Type), &fi, pp})
			} else if fi.Zero.Kind() == reflect.Struct || (fi.Zero.Kind() == reflect.Pointer && fi.Zero.Type().Elem().Kind() == reflect.Struct) {
				fi.Index = apnd(tq.fi.Index, fieldPos)
				fi.Children = make([]*FieldInfo, Deref(f.Type).NumField())
				queue = append(queue, typeQueue{Deref(f.Type), &fi, fi.Path})
			}

			fi.Index = apnd(tq.fi.Index, fieldPos)
			fi.Parent = tq.fi
			tq.fi.Children[fieldPos] = &fi
			m = append(m, &fi)
			fieldPos++
		}
	}

	flds := &StructMap{Index: m, Tree: root, Paths: map[string]*FieldInfo{}, Names: map[string]*FieldInfo{}}
	for _, fi := range flds.Index {
		// check if nothing has already been pushed with the same path
		// sometimes you can choose to override a type using embedded struct
		fld, ok := flds.Paths[fi.Path]
		if !ok || fld.Embedded {
			flds.Paths[fi.Path] = fi
			if fi.Name != "" && !fi.Embedded {
				flds.Names[fi.Path] = fi
			}
		}
	}

	return flds
}
