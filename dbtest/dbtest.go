package dbtest

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"reflect"
	"strings"

	_ "github.com/lib/pq" // Postgres driver
	"gopkg.in/yaml.v3"
)

// TestSet holds an array of DBTest. This maps to the top-level 'tests' array in the YAML.
type TestSet struct {
	// Tests is an array of DBTest objects, each representing a database test to run.
	Tests []DBTest `yaml:"tests"`
}

// DBTest encapsulates information about a single database test.
type DBTest struct {
	// Name is the name of the test, usually for logging and tracking.
	Name string `yaml:"name"`
	// Arrange contains SQL statements to be run before the test.
	Arrange []Arrange `yaml:"arrange"`
	// Act describes the method to call and its arguments during the test.
	Act Act `yaml:"act"`
	// Assert holds the information needed to verify the state of the database after the test.
	Assert []Assert `yaml:"assert"`
}

// Arrange encapsulates a single SQL statement used to prepare the database for a test.
type Arrange struct {
	// Statement is the SQL command to be executed.
	Statement string `yaml:"statement"`
}

// Act describes the method to call and its arguments.
type Act struct {
	// Method is the name of the method to call on the service under test.
	Method string `yaml:"method"`
	// Arguments is an array of objects, representing the arguments to pass to the method.
	Arguments []Object `yaml:"arguments"`
}

// Assert contains information for making an assertion on the database state.
type Assert struct {
	// Value is used in some cases to assert that a method returns this value.
	Value Object `yaml:"value"`
	// Query is the SQL query used to fetch data for assertion.
	Query string `yaml:"query"`
	// Rows describe the expected rows returned by the Query.
	Rows []Row `yaml:"rows"`
	// Error holds an expected error message, if applicable.
	Error string `yaml:"error"`
}

// Row encapsulates the expected columns for a row returned by a SQL query in an assertion.
type Row struct {
	// Columns is an array of Objects, each representing a column in a database row.
	Columns []Object `yaml:"columns"`
}

// Object represents a generic value with a type.
type Object struct {
	// Type describes the type of the Object (e.g., "int", "string", "customStruct").
	Type string `yaml:"type"`
	// Value holds the actual value of the Object.
	Value interface{} `yaml:"value"`
}

// Load reads from an io.Reader and attempts to decode the content into a TestSet object.
// It returns the populated TestSet object and any error encountered during decoding.
func Load(r io.Reader) (*TestSet, error) {
	var tests *TestSet
	if err := yaml.NewDecoder(r).Decode(&tests); err != nil {
		return nil, err
	}
	return tests, nil
}

var customTypes = make(map[string]reflect.Type)

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

// BuildObject constructs an object based on the provided Object structure, handling different data types and custom types.
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
		var elementType reflect.Type
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
				elementType = t
			} else {
				if t != elementType {
					return nil, errors.New("type mismatch in array")
				}
			}
			objs[i] = obj
		}
		s := reflect.MakeSlice(reflect.SliceOf(elementType), len(objs), len(objs))
		for i, el := range objs {
			s.Index(i).Set(reflect.ValueOf(el))
		}
		return s.Interface(), nil
	default:
		if obj.Type == "" {
			return nil, errors.New("object type mustn't be empty")
		}
		comps := strings.Split(obj.Type, ".")
		typeName := comps[len(comps)-1]
		if typeName[:1] != strings.ToUpper(typeName[:1]) {
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

// Run executes all the tests in a TestSet.
// It takes a context, a database DSN (Data Source Name), and a service interface containing the methods to be tested.
func (ts *TestSet) Run(ctx context.Context, dbDsn string, service interface{}) error {

	// Open a new database connection.
	db, err := sql.Open("postgres", dbDsn)
	if err != nil {
		log.Fatal(err)
	}

	// Make sure to close the database connection when the function returns.
	defer func() {
		err := db.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()

	// Ping the database to ensure we're connected.
	if err := db.Ping(); err != nil {
		log.Fatal(err)
	}

	// Register the types in the service for use in reflection-based operations.
	if err := registerServiceTypes(service); err != nil {
		return err
	}

	// Iterate through each test in the TestSet and run it.
	for _, t := range ts.Tests {
		// Run each test and return an error if any test fails.
		if err := t.Run(ctx, db, service); err != nil {
			return err
		}
	}

	return nil
}

// Run executes a DBTest. This function uses reflection to call methods dynamically and run SQL statements.
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

		return func() error {

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
				defer func() {
					err := rows.Close()
					if err != nil {
						log.Fatal(err)
					}
				}()
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
					return &TestError{name: t.Name, message: fmt.Sprintf("return values not equal: '%v' /= '%v'", expected, actual)}
				}
			}
			return nil
		}()
	}
	return nil
}

// TestError is a test error.
type TestError struct {
	name    string
	message string
	data    map[string]interface{}
}

// Error returns the error formatted as a string.
func (te *TestError) Error() string {
	return fmt.Sprintf("%s: %s", te.name, te.message)
}

// Data returns a value for the provided key.
func (te *TestError) Data(key string) (interface{}, bool) {
	v, ok := te.data[key]
	return v, ok
}
