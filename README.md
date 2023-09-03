# A3 Tests

## Overview

`dbtest` is a Go package designed to facilitate database testing. It provides a straightforward way to define test scenarios in YAML format, allowing you to perform actions and assertions on the database within your Go tests.

## Features

- Define test scenarios using YAML
- Automate SQL execution for test setups (`Arrange`)
- Dynamically build and inject complex test arguments (`Act`)
- Execute queries and compare with expected results (`Assert`)

## Installation

```bash
go get github.com/faelsamh/a3-tests/dbtest
```

## YAML Test Scenario Format

Your YAML file should contain an array of test objects, each having:

- `name`: The name of the test
- `arrange`: An array of SQL statements for database setup
- `act`: A method call description (`Method` and `Arguments`)
- `assert`: An array of expected database states

Example:

```yaml
tests:
  - name: Test 1
    arrange:
      - statement: INSERT INTO table1 VALUES (1, 'a')
        act:
        method: DoSomething
        arguments:
          - type: int
            value: 1
            assert:
      - query: SELECT * FROM table1
        rows:
          - columns:
              - type: int
                value: 1
```

## Writing Test Code

In your Go test file, you can write a test function that performs several operations:

1. Load the YAML content into a `TestSuite` object.
2. Execute test setups or database arrangements specified in the `TestSuite`.
3. Call the methods under test.
4. Assert that the database state matches the expected state.

Here's how to accomplish this:

```go
package main

import (
	"context"
	"database/sql"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

func TestDBOperations(t *testing.T) {
	
	a := assert.New(t)

	// 1. Load YAML test data
	yamlContent := `
    tests:
      - name: Test 1
        arrange:
          - statement: INSERT INTO table1 VALUES (1, 'a')
        act:
          method: DoSomething
          arguments: [...]  // Your arguments here
        assert:
          - query: SELECT * FROM table1
            rows:
              - columns:
                - type: int
                  value: 1
    `

	ts, err := dbtest.Load(strings.NewReader(yamlContent))
	assert.Nil(err)

	// 2. Setup database and 3. Run tests
	ctx := context.Background()
	db, _ := sql.Open("your_driver", "your_dsn")
	defer db.Close()

	for _, test := range ts.Tests {
		err = test.Run(ctx, db, yourService)
		a.Nil(err)

		// 4. Additional assertions here, if necessary
	}
}

```

Replace `yourService` with the actual service that implements the methods you wish to test.
