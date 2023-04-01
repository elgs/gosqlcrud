package gosqlcrud

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	_ "modernc.org/sqlite"
)

type Test struct {
	Id   int    `db:"id" pk:"true"`
	Name string `db:"name"`
}

func TestQueries(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	assert.NoError(t, err)
	result, err := Exec(db, "CREATE TABLE test (ID INTEGER PRIMARY KEY, NAME TEXT)")
	assert.NoError(t, err)
	assert.Equal(t, int64(0), result["last_insert_id"])
	assert.Equal(t, int64(0), result["rows_affected"])

	tx, err := db.Begin()
	assert.NoError(t, err)
	result, err = Exec(tx, "INSERT INTO test (ID, NAME) VALUES (?, ?)", 1, "Alpha")
	assert.NoError(t, err)
	assert.Equal(t, int64(1), result["last_insert_id"])
	assert.Equal(t, int64(1), result["rows_affected"])

	result, err = Exec(tx, "INSERT INTO test (ID, NAME) VALUES (?, ?)", 2, "Beta")
	assert.NoError(t, err)
	assert.Equal(t, int64(2), result["last_insert_id"])
	assert.Equal(t, int64(1), result["rows_affected"])

	result, err = Exec(tx, "INSERT INTO test (ID, NAME) VALUES (?, ?)", 3, "Gamma")
	assert.NoError(t, err)
	assert.Equal(t, int64(3), result["last_insert_id"])
	assert.Equal(t, int64(1), result["rows_affected"])
	tx.Commit()

	cols, resultArray, err := QueryToArrays(db, AsIs, "SELECT * FROM test WHERE ID > ?", 1)
	assert.NoError(t, err)
	assert.Equal(t, "ID", cols[0])
	assert.Equal(t, "NAME", cols[1])
	assert.Equal(t, int64(2), resultArray[0][0])
	assert.Equal(t, "Beta", resultArray[0][1])
	assert.Equal(t, int64(3), resultArray[1][0])
	assert.Equal(t, "Gamma", resultArray[1][1])

	resultMaps, err := QueryToMaps(db, AsIs, "SELECT * FROM test WHERE ID < ?", 3)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), resultMaps[0]["ID"])
	assert.Equal(t, "Alpha", resultMaps[0]["NAME"])
	assert.Equal(t, int64(2), resultMaps[1]["ID"])
	assert.Equal(t, "Beta", resultMaps[1]["NAME"])

	resultStructs := []Test{}
	err = QueryToStructs(db, &resultStructs, "SELECT NAME,ID FROM test WHERE ID > ?", 0)
	assert.NoError(t, err)
	assert.Equal(t, "Alpha", resultStructs[0].Name)
	assert.Equal(t, 1, resultStructs[0].Id)
	assert.Equal(t, "Beta", resultStructs[1].Name)
	assert.Equal(t, 2, resultStructs[1].Id)
	assert.Equal(t, "Gamma", resultStructs[2].Name)
	assert.Equal(t, 3, resultStructs[2].Id)

	resultStruct := Test{}
	err = Retrieve(db, SQLite, &resultStruct, "test", 1)
	assert.NoError(t, err)
	assert.Equal(t, "Alpha", resultStruct.Name)
	assert.Equal(t, 1, resultStruct.Id)

	resultStruct = Test{}
	err = Retrieve(db, SQLite, &resultStruct, "test", 2)
	assert.NoError(t, err)
	assert.Equal(t, "Beta", resultStruct.Name)
	assert.Equal(t, 2, resultStruct.Id)

}

func TestReflect(t *testing.T) {
	test := Test{Id: 1, Name: "test"}

	fields, pks := StructFieldToDbField(&test)
	assert.Equal(t, "id", fields[0])
	assert.Equal(t, "name", fields[1])
	assert.Equal(t, "id", pks[0])
}

func TestSqlSafe(t *testing.T) {
	ss := []string{
		"asdf",
		"asdf'asdf",
		"asdf--asdf",
	}
	for i := range ss {
		SqlSafe(&ss[i])
	}

	assert.Equal(t, "asdf", ss[0])
	assert.Equal(t, "asdf''asdf", ss[1])
	assert.Equal(t, "asdfasdf", ss[2])
}
