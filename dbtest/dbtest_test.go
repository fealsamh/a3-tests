package dbtest

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLoad(t *testing.T) {
	assert := assert.New(t)

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
	  - type: SomeStruct
	    value:
		  field1:
		    type: string
			value: AString
		  field2:
		    type: int
			value: 1234
		  field3:
		    type: AnotherStruct
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
		    - type: AnotherStruct
			  value:
			    field1:
			      type: string
				  value: A
			    field2:
			      type: int
				  value: 1
		    - type: AnotherStruct
			  value:
			    field1:
			      type: string
				  value: B
			    field2:
			      type: int
				  value: 2
	assert:
	- query: SELECT 1
	  rows:
	  	- columns:
		  - type: string
		    value: AnotherString
`
	input = strings.ReplaceAll(input, "\t", "    ")
	ts, err := Load(strings.NewReader(input))

	assert.Nil(err)
	t.Logf("%+v", ts)

	err = RegisterTypes((*SomeStruct)(nil), (*AnotherStruct)(nil))
	assert.Nil(err)

	arg := ts.Tests[0].Act.Arguments[1]
	t.Log(arg)

	obj, err := buildObject(&arg)
	assert.Nil(err)
	t.Logf("%+v (%T)", obj, obj)
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
