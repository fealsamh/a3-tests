package dbtest

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLoad(t *testing.T) {
	a := assert.New(t)

	input := `
tests:
  - name: Test 1
    arrange:
    - statement: INSERT 1
    - statement: INSERT 2
    act:
      method: AMethod
	  arguments:
	  - type: context
	    value: background
	  - type: dbtest.SomeStruct
	    value:
		  field1:
		    type: string
			value: AString
		  field2:
		    type: int
			value: 1234
		  field3:
		    type: dbtest.AnotherStruct
			value:
			  field1:
			    type: string
				value: abcd
			  field2:
			    type: int
				value: 5678
		  field4:
		    type: array
			value:
			- type: int
			  value: 1
			- type: int
			  value: 2
			- type: int
			  value: 3
		  field5:
			type: array
			value:			
		    - type: dbtest.AnotherStruct
			  value:
			    field1:
			      type: string
				  value: A
			    field2:
			      type: int
				  value: 1
		    - type: dbtest.AnotherStruct
			  value:
			    field1:
			      type: string
				  value: B
			    field2:
			      type: int
				  value: 2
	  - type: dbtest.SomeStruct
		value: |
			{"field1": "AString", "field2": 1234, "field3": {"field1": "abcd", "field2": 5678}, "field4": [1, 2, 3], "field5": [{"field1": "A", "field2": 1}, {"field1": "B", "field2": 2}]}
	a:
	- query: SELECT 1
	  rows:
	  	- columns:
		  - type: string
		    value: AnotherString
`
	input = strings.ReplaceAll(input, "\t", "    ")
	ts, err := Load(strings.NewReader(input))

	a.Nil(err)
	t.Logf("%+v", ts)

	registerType(reflect.TypeOf((*SomeStruct)(nil)).Elem())

	arg := ts.Tests[0].Act.Arguments[1]
	t.Log(arg)

	obj, err := BuildObject(&arg)
	a.Nil(err)
	t.Logf("%+v (%T)", obj, obj)

	a.Equal("AString", obj.(*SomeStruct).Field1)
	a.Equal(1234, obj.(*SomeStruct).Field2)
	a.Equal(&AnotherStruct{Field1: "abcd", Field2: 5678}, obj.(*SomeStruct).Field3)
	a.Equal([]int{1, 2, 3}, obj.(*SomeStruct).Field4)
	a.Equal([]*AnotherStruct{{Field1: "A", Field2: 1}, {Field1: "B", Field2: 2}}, obj.(*SomeStruct).Field5)

	arg = ts.Tests[0].Act.Arguments[2]
	t.Log(arg)

	obj, err = BuildObject(&arg)
	a.Nil(err)
	t.Logf("%+v (%T)", obj, obj)

	a.Equal("AString", obj.(*SomeStruct).Field1)
	a.Equal(1234, obj.(*SomeStruct).Field2)
	a.Equal(&AnotherStruct{Field1: "abcd", Field2: 5678}, obj.(*SomeStruct).Field3)
	a.Equal([]int{1, 2, 3}, obj.(*SomeStruct).Field4)
	a.Equal([]*AnotherStruct{{Field1: "A", Field2: 1}, {Field1: "B", Field2: 2}}, obj.(*SomeStruct).Field5)
}

type SomeStruct struct {
	Field1 string
	Field2 int
	Field3 *AnotherStruct
	Field4 []int
	Field5 []*AnotherStruct
}

type AnotherStruct struct {
	Field1 string
	Field2 int
}

func (s *AnotherStruct) String() string { return fmt.Sprintf("%+v", *s) }
