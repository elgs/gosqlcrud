package gosqlcrud

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	_ "modernc.org/sqlite"
)

type Test struct {
	Id   int    `db:"ID" pk:"true"`
	Name string `db:"NAME"`
}

func TestQueries(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	assert.NoError(t, err)
	result, err := Exec(db, "CREATE TABLE test (ID INTEGER PRIMARY KEY, NAME TEXT)") // Exec
	assert.NoError(t, err)
	assert.Equal(t, int64(0), result.LastInsertId)
	assert.Equal(t, int64(0), result.RowsAffected)

	tx, err := db.Begin()
	assert.NoError(t, err)
	result, err = Exec(tx, "INSERT INTO test (ID, NAME) VALUES (?, ?)", 1, "Alpha") // Exec
	assert.NoError(t, err)
	assert.Equal(t, int64(1), result.LastInsertId)
	assert.Equal(t, int64(1), result.RowsAffected)

	result, err = Exec(tx, "INSERT INTO test (ID, NAME) VALUES (?, ?)", 2, "Beta") // Exec
	assert.NoError(t, err)
	assert.Equal(t, int64(2), result.LastInsertId)
	assert.Equal(t, int64(1), result.RowsAffected)

	result, err = Exec(tx, "INSERT INTO test (ID, NAME) VALUES (?, ?)", 3, "Gamma") // Exec
	assert.NoError(t, err)
	assert.Equal(t, int64(3), result.LastInsertId)
	assert.Equal(t, int64(1), result.RowsAffected)
	tx.Commit()

	cols, resultArray, err := QueryToArrays(db, "SELECT * FROM test WHERE ID > ?", 1) // QueryToArrays
	assert.NoError(t, err)
	assert.Equal(t, "id", cols[0])
	assert.Equal(t, "name", cols[1])
	assert.Equal(t, int64(2), resultArray[0][0])
	assert.Equal(t, "Beta", resultArray[0][1])
	assert.Equal(t, int64(3), resultArray[1][0])
	assert.Equal(t, "Gamma", resultArray[1][1])

	resultMaps, err := QueryToMaps(db, "SELECT * FROM test WHERE ID < ?", 3) // QueryToMaps
	assert.NoError(t, err)
	assert.Equal(t, int64(1), resultMaps[0]["id"])
	assert.Equal(t, "Alpha", resultMaps[0]["name"])
	assert.Equal(t, int64(2), resultMaps[1]["id"])
	assert.Equal(t, "Beta", resultMaps[1]["name"])

	resultStructs := []Test{}
	err = QueryToStructs(db, &resultStructs, "SELECT NAME,ID FROM test WHERE ID > ?", 0) // QueryToStructs
	assert.NoError(t, err)
	assert.Equal(t, "Alpha", resultStructs[0].Name)
	assert.Equal(t, 1, resultStructs[0].Id)
	assert.Equal(t, "Beta", resultStructs[1].Name)
	assert.Equal(t, 2, resultStructs[1].Id)
	assert.Equal(t, "Gamma", resultStructs[2].Name)
	assert.Equal(t, 3, resultStructs[2].Id)

	resultStruct := Test{Id: 1}
	err = Retrieve(db, &resultStruct, "test") // Retrieve
	assert.NoError(t, err)
	assert.Equal(t, "Alpha", resultStruct.Name)
	assert.Equal(t, 1, resultStruct.Id)
	resultStruct.Id = 2
	err = Retrieve(db, &resultStruct, "test") // Retrieve
	assert.NoError(t, err)
	assert.Equal(t, "Beta", resultStruct.Name)
	assert.Equal(t, 2, resultStruct.Id)
	resultStruct.Id = 3
	err = Retrieve(db, &resultStruct, "test") // Retrieve
	assert.NoError(t, err)
	assert.Equal(t, "Gamma", resultStruct.Name)
	assert.Equal(t, 3, resultStruct.Id)
	resultStruct.Id = 4
	err = Retrieve(db, &resultStruct, "test") // Retrieve
	assert.Error(t, err)

	data := Test{Id: 4, Name: "Delta"}
	result, err = Create(db, &data, "test") // Create
	assert.NoError(t, err)
	assert.Equal(t, int64(4), result.LastInsertId)
	assert.Equal(t, int64(1), result.RowsAffected)

	resultStruct.Id = 4
	err = Retrieve(db, &resultStruct, "test") // Retrieve
	assert.NoError(t, err)
	assert.Equal(t, "Delta", resultStruct.Name)
	assert.Equal(t, 4, resultStruct.Id)

	data.Name = "Omega"
	result, err = Update(db, &data, "test") // Update
	assert.NoError(t, err)
	assert.Equal(t, int64(4), result.LastInsertId)
	assert.Equal(t, int64(1), result.RowsAffected)

	resultStruct.Id = 4
	err = Retrieve(db, &resultStruct, "test") // Retrieve
	assert.NoError(t, err)
	assert.Equal(t, "Omega", resultStruct.Name)
	assert.Equal(t, 4, resultStruct.Id)

	result, err = Delete(db, &data, "test") // Delete
	assert.NoError(t, err)
	assert.Equal(t, int64(4), result.LastInsertId)
	assert.Equal(t, int64(1), result.RowsAffected)

	resultStruct.Id = 4
	err = Retrieve(db, &resultStruct, "test") // Retrieve
	assert.Error(t, err)
}

func TestReflect(t *testing.T) {
	test := Test{Id: 1, Name: "test"}

	fields := StructFieldToDbField(&test)
	assert.Equal(t, "ID", fields[0])
	assert.Equal(t, "NAME", fields[1])

	nonPkMap, pkMap := StructToDbMap(&test)
	assert.Equal(t, "test", nonPkMap["NAME"])
	assert.Equal(t, 1, pkMap["ID"])
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
