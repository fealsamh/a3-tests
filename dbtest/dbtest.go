package dbtest

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"

	_ "github.com/lib/pq"
	"gopkg.in/yaml.v3"
)

type (
	// TestSet ...
	TestSet struct {
		Tests []DBTest `yaml:"tests"`
	}

	// DBTest ...
	DBTest struct {
		Name    string    `yaml:"name"`
		Arrange []Arrange `yaml:"arrange"`
		Act     Act       `yaml:"act"`
		Assert  []Assert  `yaml:"assert"`
	}

	// Arrange ...
	Arrange struct {
		Statement string `yaml:"statement"`
	}

	// Act ...
	Act struct {
		Method    string   `yaml:"method"`
		Arguments []Object `yaml:"arguments"`
	}

	// Assert ...
	Assert struct {
		Value Object `yaml:"value"`
		Query string `yaml:"query"`
		Rows  []Row  `yaml:"rows"`
		Error string `yaml:"error"`
	}

	// Row ...
	Row struct {
		Columns []Object `yaml:"columns"`
	}

	// Object ...
	Object struct {
		Type  string      `yaml:"type"`
		Value interface{} `yaml:"value"`
	}
)

// Load ...
func Load(r io.Reader) (*TestSet, error) {
	var ts *TestSet
	if err := yaml.NewDecoder(r).Decode(&ts); err != nil {
		return nil, err
	}
	return ts, nil
}

var (
	customTypes = make(map[string]reflect.Type)
)

func registerServiceTypes(srv interface{}) error {
	t := reflect.TypeOf(srv)
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return errors.New("service object not a structure")
	}
	t = reflect.PtrTo(t)
	types := make(map[reflect.Type]struct{})
	for i := 0; i < t.NumMethod(); i++ {
		m := t.Method(i)
		for j := 1; j < m.Type.NumIn(); j++ {
			t := m.Type.In(j)
			for t.Kind() == reflect.Pointer || t.Kind() == reflect.Slice {
				t = t.Elem()
			}
			if t.Kind() == reflect.Struct {
				types[t] = struct{}{}
			}
		}
		for j := 0; j < m.Type.NumOut(); j++ {
			t := m.Type.Out(j)
			for t.Kind() == reflect.Pointer || t.Kind() == reflect.Slice {
				t = t.Elem()
			}
			if t.Kind() == reflect.Struct {
				types[t] = struct{}{}
			}
		}
	}
	for t := range types {
		registerType(t)
	}
	return nil
}

func registerType(t reflect.Type) {
	comps := strings.Split(t.PkgPath(), "/")
	key := comps[len(comps)-1] + "." + t.Name()
	if _, ok := customTypes[key]; ok {
		return
	}
	customTypes[key] = t
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		t := f.Type
		for t.Kind() == reflect.Pointer || t.Kind() == reflect.Slice {
			t = t.Elem()
		}
		if t.Kind() == reflect.Struct {
			registerType(t)
		}
	}
}

func buildObjectFromMap(m map[string]interface{}) (interface{}, error) {
	t, ok := m["type"]
	if !ok {
		return nil, errors.New("expected 'type' in map")
	}
	tn, ok := t.(string)
	if !ok {
		return nil, errors.New("'type' in map must be a string")
	}
	val, ok := m["value"]
	if !ok {
		return nil, errors.New("expected 'value' in map")
	}
	return BuildObject(&Object{
		Type:  tn,
		Value: val,
	})
}

// BuildObject builds a native object.
func BuildObject(obj *Object) (interface{}, error) {
	switch obj.Type {
	case "context":
		val, ok := obj.Value.(string)
		if !ok {
			return nil, fmt.Errorf("type '%s' expects value of type 'string'", obj.Type)
		}
		if val == "background" {
			return context.Background(), nil
		}
		return nil, fmt.Errorf("type '%s' expects value 'background'", obj.Type)
	case "string":
		val, ok := obj.Value.(string)
		if !ok {
			return nil, fmt.Errorf("type '%s' expects value of type 'string'", obj.Type)
		}
		return val, nil
	case "int":
		val, ok := obj.Value.(int)
		if !ok {
			return nil, fmt.Errorf("type '%s' expects value of type 'int'", obj.Type)
		}
		return val, nil
	case "array":
		val, ok := obj.Value.([]interface{})
		if !ok || len(val) == 0 {
			return nil, fmt.Errorf("type '%s' expects value of type 'array of maps' which mustn't be empty", obj.Type)
		}
		objs := make([]interface{}, len(val))
		var eltype reflect.Type
		for i, el := range val {
			el, ok := el.(map[string]interface{})
			if !ok {
				return nil, fmt.Errorf("type '%s' expects value of type 'array of maps' which mustn't be empty", obj.Type)
			}
			obj, err := buildObjectFromMap(el)
			if err != nil {
				return nil, err
			}
			t := reflect.TypeOf(obj)
			if i == 0 {
				eltype = t
			} else {
				if t != eltype {
					return nil, errors.New("type mismatch in array")
				}
			}
			objs[i] = obj
		}
		s := reflect.MakeSlice(reflect.SliceOf(eltype), len(objs), len(objs))
		for i, el := range objs {
			s.Index(i).Set(reflect.ValueOf(el))
		}
		return s.Interface(), nil
	default:
		if obj.Type == "" {
			return nil, errors.New("object type mustn't be empty")
		}
		comps := strings.Split(obj.Type, ".")
		tname := comps[len(comps)-1]
		if tname[:1] != strings.ToUpper(tname[:1]) {
			return nil, fmt.Errorf("custom type '%s' doesn't begin with an uppercase letter", obj.Type)
		}
		t, ok := customTypes[obj.Type]
		if !ok {
			return nil, fmt.Errorf("unknown custom type '%s'", obj.Type)
		}
		val, ok := obj.Value.(map[string]interface{})
		if ok {
			inst := reflect.New(t).Elem()
			for k, v := range val {
				f := inst.FieldByName(strings.ToUpper(k[:1]) + k[1:])
				if !f.IsValid() {
					return nil, fmt.Errorf("field '%s' not found in type '%s'", k, obj.Type)
				}
				m, ok := v.(map[string]interface{})
				if !ok {
					return nil, fmt.Errorf("invalid value of field '%s', must be a map (is %T)", k, v)
				}
				v, err := buildObjectFromMap(m)
				if err != nil {
					return nil, err
				}
				val := reflect.ValueOf(v)
				if !val.Type().AssignableTo(f.Type()) {
					if val.Type().ConvertibleTo(f.Type()) {
						val = val.Convert(f.Type())
					} else {
						return nil, fmt.Errorf("field '%s' can't be assigned the provided value, type mismatch", k)
					}
				}
				f.Set(val)
			}
			return inst.Addr().Interface(), nil
		}
		{
			val, ok := obj.Value.(string)
			if !ok {
				return nil, fmt.Errorf("type '%s' expects value of type 'map' or 'JSON string'", obj.Type)
			}
			var m map[string]interface{}
			if err := json.Unmarshal([]byte(val), &m); err != nil {
				return nil, fmt.Errorf("type '%s', failed to unmarshal JSON (%s)", obj.Type, err)
			}
			return buildObjectFromJSON(t, m)
		}
	}
}

func buildObjectFromJSON(typ reflect.Type, m map[string]interface{}) (interface{}, error) {
	inst := reflect.New(typ).Elem()
	for k, v := range m {
		f := inst.FieldByName(strings.ToUpper(k[:1]) + k[1:])
		if !f.IsValid() {
			return nil, fmt.Errorf("field '%s' not found in type '%s'", k, typ)
		}
		v, err := buildValueFromJSON(f.Type(), v)
		if err != nil {
			return nil, err
		}
		f.Set(reflect.ValueOf(v))
	}
	return inst.Addr().Interface(), nil
}

func buildValueFromJSON(typ reflect.Type, v interface{}) (interface{}, error) {
	switch v := v.(type) {
	case map[string]interface{}:
		return buildObjectFromJSON(typ.Elem(), v)
	case []interface{}:
		s := reflect.MakeSlice(typ, len(v), len(v))
		for i, v := range v {
			v, err := buildValueFromJSON(typ.Elem(), v)
			if err != nil {
				return nil, err
			}
			s.Index(i).Set(reflect.ValueOf(v))
		}
		return s.Interface(), nil
	default:
		if !reflect.TypeOf(v).ConvertibleTo(typ) {
			return nil, fmt.Errorf("expected value of/convertible to type '%s'", typ)
		}
		return reflect.ValueOf(v).Convert(typ).Interface(), nil
	}
}

// Run runs a test set.
func (ts *TestSet) Run(ctx context.Context, dbDsn string, service interface{}) error {
	db, err := sql.Open("postgres", dbDsn)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	if err := registerServiceTypes(service); err != nil {
		return err
	}

	for _, t := range ts.Tests {
		if err := t.Run(ctx, db, service); err != nil {
			return err
		}
	}

	return nil
}

// Run runs a test.
func (t *DBTest) Run(ctx context.Context, db *sql.DB, service interface{}) error {
	for _, arr := range t.Arrange {
		if _, err := db.ExecContext(ctx, arr.Statement); err != nil {
			return err
		}
	}

	s := reflect.ValueOf(service)
	m, ok := s.Type().MethodByName(t.Act.Method)
	if !ok {
		return fmt.Errorf("method '%s' not found in service", t.Act.Method)
	}

	if m.Type.NumIn() != len(t.Act.Arguments)+1 {
		return fmt.Errorf("invalid number of arguments to method '%s'", t.Act.Method)
	}

	args := make([]reflect.Value, len(t.Act.Arguments)+1)
	args[0] = s
	for i, obj := range t.Act.Arguments {
		obj, err := BuildObject(&obj)
		if err != nil {
			return err
		}
		args[i+1] = reflect.ValueOf(obj)
	}
	r := m.Func.Call(args)
	err := r[m.Type.NumOut()-1].Interface()

	for _, ass := range t.Assert {
		if err != nil && ass.Error == "" {
			err, ok := err.(error)
			if !ok {
				return fmt.Errorf("last return value of '%s' isn't an error", m.Name)
			}
			return &TestError{name: t.Name, message: "unexpected error: " + err.Error()}
		}

		switch {
		case ass.Error != "":
			if err == nil {
				return &TestError{name: t.Name, message: "expected error"}
			}
			err, ok := err.(error)
			if !ok {
				return fmt.Errorf("last return value of '%s' isn't an error", m.Name)
			}
			if err.Error() != ass.Error {
				return &TestError{name: t.Name, message: fmt.Sprintf("different error: '%s' /= '%s'", ass.Error, err.Error())}
			}

		case ass.Query != "":
			rows, err := db.QueryContext(ctx, ass.Query)
			if err != nil {
				return err
			}
			defer rows.Close()
			var i int
			for rows.Next() {
				if i >= len(ass.Rows) {
					return &TestError{name: t.Name, message: "less rows expected"}
				}
				row := ass.Rows[i]
				i++
				cols, err := rows.ColumnTypes()
				if err != nil {
					return err
				}
				if len(row.Columns) != len(cols) {
					return &TestError{name: t.Name, message: "invalid number of columns"}
				}
				expected := make([]interface{}, len(row.Columns))
				actual := make([]interface{}, len(row.Columns))
				for i, col := range cols {
					expected[i], err = BuildObject(&row.Columns[i])
					if err != nil {
						return err
					}
					actual[i] = reflect.New(col.ScanType()).Interface()
				}
				if err := rows.Scan(actual...); err != nil {
					return err
				}
				for i, val := range expected {
					a := actual[i]
					v1 := reflect.ValueOf(a)
					v2 := reflect.ValueOf(val)
					if v1.Kind() == reflect.Pointer && v2.Kind() != reflect.Pointer {
						v1 = v1.Elem()
						a = v1.Interface()
					}
					if v1.Type() != v2.Type() {
						if v1.Type().ConvertibleTo(v2.Type()) {
							a = v1.Convert(v2.Type()).Interface()
						} else {
							return &TestError{name: t.Name, message: fmt.Sprintf("incompatible types of field '%s'", cols[i].Name())}
						}
					}
					if !reflect.DeepEqual(val, a) {
						return &TestError{name: t.Name, message: fmt.Sprintf("values of field '%s' not equal: '%v' /= '%v'", cols[i].Name(), val, a)}
					}
				}
			}
			if err := rows.Err(); err != nil {
				return err
			}
			if i < len(ass.Rows) {
				return &TestError{name: t.Name, message: "more rows expected"}
			}
		default:
			if len(r) != 2 {
				return &TestError{name: t.Name, message: fmt.Sprintf("invalid number of return values of method '%s'", t.Act.Method)}
			}
			expected, err := BuildObject(&ass.Value)
			if err != nil {
				return err
			}
			actual := r[0].Interface()
			if !reflect.DeepEqual(expected, actual) {
				return &TestError{
					name:    t.Name,
					message: fmt.Sprintf("return values not equal: '%v' /= '%v'", expected, actual),
					data: map[string]interface{}{
						"expected": expected,
						"actual":   actual,
					},
				}
			}
		}
	}

	return nil
}

// TestError is a test error.
type TestError struct {
	name    string
	message string
	data    map[string]interface{}
}

func (te *TestError) Error() string {
	return fmt.Sprintf("%s: %s", te.name, te.message)
}

// Data returns a value for the provided key.
func (te *TestError) Data(key string) (interface{}, bool) {
	v, ok := te.data[key]
	return v, ok
}
