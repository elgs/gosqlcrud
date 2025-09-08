package gosqlcrud

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
)

const Version = "6"

type DB interface {
	Query(query string, args ...any) (*sql.Rows, error)
	Exec(query string, args ...any) (sql.Result, error)
	QueryRow(query string, args ...any) *sql.Row
}

type DBResult struct {
	RowsAffected int64 `json:"rows_affected"`
	LastInsertId int64 `json:"last_insert_id"`
}

// QueryToArrays - run sql and return an array of arrays
func QueryToArrays[T DB](conn T, sqlStatement string, sqlParams ...any) ([]string, [][]any, error) {
	dbType := GetDbType(conn)
	data := [][]any{}
	rows, err := conn.Query(sqlStatement, sqlParams...)
	if err != nil {
		if os.Getenv("env") == "dev" {
			fmt.Println("Error executing: ", sqlStatement)
		}
		return []string{}, data, err
	}
	cols, err := rows.Columns()
	if err != nil {
		return []string{}, data, err
	}
	lenCols := len(cols)
	for i, v := range cols {
		cols[i] = strings.ToLower(v)
	}

	rawResult := make([]any, lenCols)
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return []string{}, data, err
	}
	dest := make([]any, lenCols) // A temporary any slice
	for i := range rawResult {
		dest[i] = &rawResult[i] // Put pointers to each string in the interface slice
	}
	for rows.Next() {
		result := make([]any, lenCols)
		rows.Scan(dest...)
		for i, raw := range rawResult {
			if raw == nil {
				result[i] = nil
			} else {
				colType := colTypes[i].DatabaseTypeName()
				result[i] = convertBytes(raw, colType)
				if dbType == Oracle {
					result[i] = convertStrings(raw, colType)
				} else if dbType == SQLite {
					// in sqlite, json columns fall here, if columnName contains "json" case insensitively
					if v, ok := raw.(string); ok {
						if colType == "" {
							_v := strings.TrimSpace(v)
							if strings.HasPrefix(_v, "{") && strings.HasSuffix(_v, "}") || strings.HasPrefix(_v, "[") && strings.HasSuffix(_v, "]") {
								var a any
								err := json.Unmarshal([]byte(_v), &a)
								if err == nil {
									result[i] = &a
								}
							}
						}
					}
				}
			}
		}
		data = append(data, result)
	}
	return cols, data, nil
}

// QueryToMaps - run sql and return an array of maps
func QueryToMaps[T DB](conn T, sqlStatement string, sqlParams ...any) ([]map[string]any, error) {
	dbType := GetDbType(conn)
	results := []map[string]any{}
	rows, err := conn.Query(sqlStatement, sqlParams...)
	if err != nil {
		if os.Getenv("env") == "dev" {
			fmt.Println("Error executing: ", sqlStatement)
		}
		return results, err
	}
	cols, err := rows.Columns()
	if err != nil {
		return results, err
	}
	lenCols := len(cols)

	for i, v := range cols {
		cols[i] = strings.ToLower(v)
	}

	rawResult := make([]any, lenCols)
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return results, err
	}
	dest := make([]any, lenCols) // A temporary any slice
	for i := range rawResult {
		dest[i] = &rawResult[i] // Put pointers to each string in the interface slice
	}
	for rows.Next() {
		result := make(map[string]any, lenCols)
		rows.Scan(dest...)
		for i, raw := range rawResult {
			if raw == nil {
				result[cols[i]] = nil
			} else {
				colType := colTypes[i].DatabaseTypeName()
				result[cols[i]] = convertBytes(raw, colType)
				if dbType == Oracle {
					result[cols[i]] = convertStrings(raw, colType)
				} else if dbType == SQLite {
					// in sqlite, json columns fall here, if columnName contains "json" case insensitively
					if v, ok := raw.(string); ok {
						if colType == "" {
							_v := strings.TrimSpace(v)
							if strings.HasPrefix(_v, "{") && strings.HasSuffix(_v, "}") || strings.HasPrefix(_v, "[") && strings.HasSuffix(_v, "]") {
								var a any
								err := json.Unmarshal([]byte(_v), &a)
								if err == nil {
									result[cols[i]] = &a
								}
							}
						}
					}
				}
			}
		}
		results = append(results, result)
	}
	return results, nil
}

// faulty mysql driver workaround https://github.com/go-sql-driver/mysql/issues/1401
func convertBytes(raw any, colType string) any {
	if v, ok := raw.([]byte); ok {
		value := string(v)
		switch colType {
		case "SMALLINT", "MEDIUMINT", "INT", "INTEGER", "BIGINT", "YEAR":
			raw, _ = strconv.Atoi(value)
		case "TINYINT", "BOOL", "BOOLEAN", "BIT":
			raw, _ = strconv.ParseBool(value)
		case "FLOAT", "DOUBLE", "DECIMAL":
			raw, _ = strconv.ParseFloat(value, 64)
		case "DATETIME", "TIMESTAMP":
			raw, _ = time.Parse("2006-01-02 15:04:05", value)
		case "DATE":
			raw, _ = time.Parse("2006-01-02", value)
		case "TIME":
			raw, _ = time.Parse("15:04:05", value)
		case "JSON":
			raw = json.RawMessage(value)
		case "NULL":
			raw = nil
		default:
			raw = value
		}
	}
	return raw
}

// faulty oracle driver workaround https://github.com/sijms/go-ora/issues/533
func convertStrings(raw any, colType string) any {
	if v, ok := raw.(string); ok {
		switch colType {
		case "NUMBER":
			raw, _ = strconv.ParseFloat(v, 64)
		case "DATE", "TIMESTAMP":
			raw, _ = time.Parse("2006-01-02 15:04:05", v)
		default:
			raw = v
		}
	}
	return raw
}

func QueryToStructs[T DB, S any](conn T, results *[]S, sqlStatement string, sqlParams ...any) error {
	rows, err := conn.Query(sqlStatement, sqlParams...)
	if err != nil {
		if os.Getenv("env") == "dev" {
			fmt.Println("Error executing: ", sqlStatement)
		}
		return err
	}
	cols, err := rows.Columns()
	if err != nil {
		return err
	}
	lenCols := len(cols)

	for rows.Next() { // iterate through rows
		colValues := make([]any, lenCols)
		var result S
		structValue := reflect.ValueOf(&result).Elem()
		for colIndex, colName := range cols { // iterate through columns
			found := false
			for fieldIndex := 0; fieldIndex < structValue.NumField(); fieldIndex++ { // iterate through struct fields
				dbTag := structValue.Type().Field(fieldIndex).Tag.Get("db")
				if strings.EqualFold(colName, dbTag) {
					colValues[colIndex] = structValue.Field(fieldIndex).Addr().Interface()
					found = true
					break
				}
			}
			if !found {
				colValues[colIndex] = new(any)
			}
		}
		rows.Scan(colValues...)
		*results = append(*results, result)
	}

	return nil
}

func Retrieve[T DB, S any](conn T, result *S, table string) error {
	fields := StructFieldToDbField(result)
	_, pkMap := StructToDbMap(result)
	dbType := GetDbType(conn)
	if dbType == Unknown {
		return errors.New("unknown database type")
	}
	where, values, err := MapForSqlWhere(pkMap, 0, dbType)
	if err != nil {
		return err
	}
	fieldsString := strings.Join(fields, ", ")
	SqlSafe(&fieldsString)
	SqlSafe(&table)
	sqlStatement := fmt.Sprintf("SELECT %s FROM %s WHERE 1=1 %s", fieldsString, table, where)

	rows, err := conn.Query(sqlStatement, values...)
	if err != nil {
		if os.Getenv("env") == "dev" {
			fmt.Println("Error executing: ", sqlStatement)
		}
		return err
	}
	cols, err := rows.Columns()
	if err != nil {
		return err
	}
	lenCols := len(cols)

	if rows.Next() {
		colValues := make([]any, lenCols)
		structValue := reflect.ValueOf(result).Elem()
		for colIndex, colName := range cols { // iterate through columns
			found := false
			for fieldIndex := 0; fieldIndex < structValue.NumField(); fieldIndex++ { // iterate through struct fields
				dbTag := structValue.Type().Field(fieldIndex).Tag.Get("db")
				if strings.EqualFold(colName, dbTag) {
					colValues[colIndex] = structValue.Field(fieldIndex).Addr().Interface()
					found = true
					break
				}
			}
			if !found {
				colValues[colIndex] = new(any)
			}
		}
		rows.Scan(colValues...)
		rows.Close()
		return nil
	}
	return fmt.Errorf("no record found for %s, %v", table, pkMap)
}

func Create[T DB, S any](conn T, data *S, table string) (*DBResult, error) {
	fieldMap, pkMap := StructToDbMap(data)
	for k, v := range pkMap {
		fieldMap[k] = v
	}
	dbType := GetDbType(conn)
	if dbType == Unknown {
		return nil, errors.New("unknown database type")
	}
	qms, keys, values, err := MapForSqlInsert(fieldMap, dbType)
	if err != nil {
		return nil, err
	}
	SqlSafe(&table)
	sqlStatement := fmt.Sprintf(`INSERT INTO %s (%s) VALUES (%s)`, table, keys, qms)
	return Exec(conn, sqlStatement, values...)
}

func Update[T DB, S any](conn T, data *S, table string) (*DBResult, error) {
	nonPkMap, pkMap := StructToDbMap(data)
	dbType := GetDbType(conn)
	if dbType == Unknown {
		return nil, errors.New("unknown database type")
	}
	setClause, setValues, err := MapForSqlUpdate(nonPkMap, dbType)
	if err != nil {
		return nil, err
	}
	where, whereValues, err := MapForSqlWhere(pkMap, len(nonPkMap), dbType)
	if err != nil {
		return nil, err
	}
	values := append(setValues, whereValues...)
	sqlStatement := fmt.Sprintf(`UPDATE %s SET %s WHERE 1=1 %s`, table, setClause, where)
	return Exec(conn, sqlStatement, values...)
}

func Delete[T DB, S any](conn T, data *S, table string) (*DBResult, error) {
	_, pkMap := StructToDbMap(data)
	dbType := GetDbType(conn)
	if dbType == Unknown {
		return nil, errors.New("unknown database type")
	}
	where, whereValues, err := MapForSqlWhere(pkMap, 0, dbType)
	if err != nil {
		return nil, err
	}
	SqlSafe(&table)
	sqlStatement := fmt.Sprintf(`DELETE FROM %s WHERE 1=1 %s`, table, where)
	return Exec(conn, sqlStatement, whereValues...)
}

// Exec - run sql and return the number of rows affected
func Exec[T DB](conn T, sqlStatement string, sqlParams ...any) (*DBResult, error) {
	result, err := conn.Exec(sqlStatement, sqlParams...)
	if err != nil {
		if os.Getenv("env") == "dev" {
			fmt.Println("Error executing: ", sqlStatement)
			fmt.Println(err)
		}
		return nil, err
	}
	rowsffected, err := result.RowsAffected()
	if err != nil {
		return nil, err
	}
	ret := &DBResult{
		RowsAffected: rowsffected,
	}
	lastInsertId, err := result.LastInsertId()
	if err == nil {
		ret.LastInsertId = lastInsertId
	}
	return ret, nil
}

func SqlSafe(s *string) {
	*s = strings.Replace(*s, "'", "''", -1)
	*s = strings.Replace(*s, "--", "", -1)
}

func StructFieldToDbField[T any](s *T) (fields []string) {
	structValue := reflect.ValueOf(s).Elem()
	for fieldIndex := 0; fieldIndex < structValue.NumField(); fieldIndex++ {
		fieldTag := structValue.Type().Field(fieldIndex).Tag
		dbTag := fieldTag.Get("db")
		if dbTag != "" {
			fields = append(fields, dbTag)
		}
	}
	return
}

func StructToDbMap[T any](s *T) (nonPkMap map[string]any, pkMap map[string]any) {
	nonPkMap = make(map[string]any)
	pkMap = make(map[string]any)
	structValue := reflect.ValueOf(s).Elem()
	for fieldIndex := 0; fieldIndex < structValue.NumField(); fieldIndex++ {
		var field = structValue.Type().Field(fieldIndex)
		if !field.IsExported() {
			continue
		}
		fieldTag := structValue.Type().Field(fieldIndex).Tag
		value := structValue.Field(fieldIndex).Interface()
		pkTag := fieldTag.Get("pk")
		dbTag := fieldTag.Get("db")
		if dbTag != "" && pkTag != "true" {
			nonPkMap[dbTag] = value
		}
		if pkTag == "true" {
			pkMap[dbTag] = value
		}
	}
	return
}

func MapForSqlInsert(m map[string]any, dbType DbType) (placeholders string, keys string, values []any, err error) {
	length := len(m)
	if length == 0 {
		return "", "", nil, fmt.Errorf("empty parameter map")
	}

	for i := 0; i < length; i++ {
		placeholders += GetPlaceHolder(i, dbType) + ","
	}
	placeholders = placeholders[:len(placeholders)-1]

	values = make([]any, length)
	i := 0
	for k, v := range m {
		keys += k + ","
		values[i] = v
		i++
	}
	keys = keys[:len(keys)-1]
	SqlSafe(&keys)
	return
}

func MapForSqlUpdate(m map[string]any, dbType DbType) (set string, values []any, err error) {
	length := len(m)
	if length == 0 {
		return "", nil, fmt.Errorf("empty parameter map")
	}

	values = make([]any, length)
	i := 0
	for k, v := range m {
		set += fmt.Sprintf("%s=%s,", k, GetPlaceHolder(i, dbType))
		values[i] = v
		i++
	}
	set = set[:len(set)-1]
	SqlSafe(&set)
	return
}

func MapForSqlWhere(m map[string]any, startIndex int, dbType DbType) (where string, values []any, err error) {
	length := len(m)
	if length == 0 {
		return
	}

	i := startIndex
	for k, v := range m {
		if strings.HasPrefix(k, ".") {
			continue
		}
		where += fmt.Sprintf("AND %s=%s ", k, GetPlaceHolder(i, dbType))
		values = append(values, v)
		i++
	}
	where = strings.TrimSpace(where)
	SqlSafe(&where)
	return
}

func GetPlaceHolder(index int, dbType DbType) string {
	if dbType == PostgreSQL {
		return fmt.Sprintf("$%d", index+1)
	} else if dbType == SQLServer {
		return fmt.Sprintf("@p%d", index+1)
	} else if dbType == Oracle {
		return fmt.Sprintf(":%d", index+1)
	} else {
		return "?"
	}
}

type DbType int

const (
	Unknown DbType = iota
	SQLite
	MySQL
	PostgreSQL
	SQLServer
	Oracle
)

var dbTypeMap = map[string]DbType{}
var mutex = sync.RWMutex{}

func GetDbType(conn DB) DbType {
	connPtrStr := fmt.Sprintf("%p\n", conn)
	if val, ok := dbTypeMap[connPtrStr]; ok {
		return val
	}

	var v string
	err := conn.QueryRow("SELECT VERSION() AS version").Scan(&v)
	if err == nil {
		if strings.Contains(strings.ToLower(v), "postgres") {
			mutex.Lock()
			dbTypeMap[connPtrStr] = PostgreSQL
			mutex.Unlock()
			return PostgreSQL
		} else {
			mutex.Lock()
			dbTypeMap[connPtrStr] = MySQL
			mutex.Unlock()
			return MySQL
		}
	}

	err = conn.QueryRow("SELECT @@VERSION AS version").Scan(&v)
	if err == nil {
		if strings.Contains(strings.ToLower(v), "microsoft") {
			mutex.Lock()
			dbTypeMap[connPtrStr] = SQLServer
			mutex.Unlock()
			return SQLServer
		} else {
			mutex.Lock()
			dbTypeMap[connPtrStr] = MySQL
			mutex.Unlock()
			return MySQL
		}
	}

	err = conn.QueryRow("SELECT BANNER FROM v$version").Scan(&v)
	if err == nil {
		if strings.Contains(strings.ToLower(v), "oracle") {
			mutex.Lock()
			dbTypeMap[connPtrStr] = Oracle
			mutex.Unlock()
			return Oracle
		}
	}
	err = conn.QueryRow("SELECT sqlite_version()").Scan(&v)
	if err == nil {
		mutex.Lock()
		dbTypeMap[connPtrStr] = SQLite
		mutex.Unlock()
		return SQLite
	}

	mutex.Lock()
	dbTypeMap[connPtrStr] = Unknown
	mutex.Unlock()
	return Unknown
}

// func GetAllTables(conn DB) (tables []string, err error) {
// 	dbType := GetDbType(conn)
// 	if dbType == PostgreSQL {
// 		tableMaps, err := QueryToMaps(conn, "SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
// 	} else if dbType == SQLServer {
// 		tableMaps, err := QueryToMaps(conn, "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'")
// 	} else if dbType == Oracle {
// 		tableMaps, err := QueryToMaps(conn, "SELECT table_name FROM user_tables")
// 	} else if dbType == SQLite {
// 		tableMaps, err := QueryToMaps(conn, "SELECT name FROM sqlite_master WHERE type='table'")
// 	} else {
// 		tableMaps, err := QueryToMaps(conn, "SHOW TABLES")
// 	}
// 	return
// }

// func GetTableColumns(conn DB, tableName string) (columns []string, err error) {
// 	dbType := GetDbType(conn)
// 	if dbType == PostgreSQL {
// 		columnMaps, err := QueryToMaps(conn, "SELECT column_name FROM information_schema.columns WHERE table_name=$1", tableName)
// 	} else if dbType == SQLServer {
// 		columnMaps, err := QueryToMaps(conn, "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME=$1", tableName)
// 	} else if dbType == Oracle {
// 		columnMaps, err := QueryToMaps(conn, "SELECT column_name FROM user_tab_columns WHERE table_name=$1", tableName)
// 	} else if dbType == SQLite {
// 		columnMaps, err := QueryToMaps(conn, "PRAGMA table_info($1)", tableName)
// 	} else {
// 		columnMaps, err := QueryToMaps(conn, "SHOW COLUMNS FROM $1", tableName)
// 	}
// 	return
// }

// func GetTablePrimaryKeys(conn DB, tableName string) (primaryKeys []string, err error) {
// 	dbType := GetDbType(conn)
// 	if dbType == PostgreSQL {
// 		pkMaps, err := QueryToMaps(conn, "SELECT column_name FROM information_schema.key_column_usage WHERE table_name=$1 AND constraint_name='PRIMARY KEY'", tableName)
// 	} else if dbType == SQLServer {
// 		pkMaps, err := QueryToMaps(conn, "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_NAME=$1 AND CONSTRAINT_NAME='PRIMARY'", tableName)
// 	} else if dbType == Oracle {
// 		pkMaps, err := QueryToMaps(conn, "SELECT column_name FROM user_cons_columns WHERE table_name=$1 AND constraint_name=(SELECT constraint_name FROM user_constraints WHERE table_name=$1 AND constraint_type='P')", tableName)
// 	} else if dbType == SQLite {
// 		pkMaps, err := QueryToMaps(conn, "PRAGMA table_info($1)", tableName)
// 	} else {
// 		pkMaps, err := QueryToMaps(conn, "SHOW KEYS FROM $1 WHERE Key_name='PRIMARY'", tableName)
// 	}
// 	return
// }
