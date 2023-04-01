package gosqlcrud

import (
	"database/sql"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"
)

const Version = "1"

type DbType int

const (
	SQLite DbType = iota
	MySQL
	PostgreSQL
	MSSQLServer
	Oracle
)

type CaseType int

const (
	AsIs CaseType = iota
	Lower
	Upper
	Camel
)

type DB interface {
	Query(query string, args ...any) (*sql.Rows, error)
	Exec(query string, args ...any) (sql.Result, error)
}

// QueryToArrays - run sql and return an array of arrays
func QueryToArrays[T DB](conn T, theCase CaseType, sqlStatement string, sqlParams ...any) ([]string, [][]any, error) {
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
		if theCase == Lower {
			cols[i] = strings.ToLower(v)
		} else if theCase == Upper {
			cols[i] = strings.ToUpper(v)
		} else if theCase == Camel {
			cols[i] = toCamel(v)
		}
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
			// faulty mysql driver workaround https://github.com/go-sql-driver/mysql/issues/1401
			if v, ok := raw.([]byte); ok {
				value := string(v)
				switch colTypes[i].DatabaseTypeName() {
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
				case "NULL":
					raw = nil
				default:
					raw = value
				}
			}
			result[i] = raw
		}
		data = append(data, result)
	}
	return cols, data, nil
}

// QueryToMaps - run sql and return an array of maps
func QueryToMaps[T DB](conn T, theCase CaseType, sqlStatement string, sqlParams ...any) ([]map[string]any, error) {
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
		if theCase == Lower {
			cols[i] = strings.ToLower(v)
		} else if theCase == Upper {
			cols[i] = strings.ToUpper(v)
		} else if theCase == Camel {
			cols[i] = toCamel(v)
		}
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
			// faulty mysql driver workaround https://github.com/go-sql-driver/mysql/issues/1401
			if v, ok := raw.([]byte); ok {
				value := string(v)
				switch colTypes[i].DatabaseTypeName() {
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
				case "NULL":
					raw = nil
				default:
					raw = value
				}
			}
			result[cols[i]] = raw
		}
		results = append(results, result)
	}
	return results, nil
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

func Retrieve[T DB, S any](conn T, dbType DbType, result *S, table string, idValues ...any) error {
	fields, pks := StructFieldToDbField(result)
	pkValues := make(map[string]any)
	for i, pk := range pks {
		pkValues[pk] = idValues[i]
	}
	where, values, err := MapForSqlWhere(pkValues, 0, dbType)
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
	return fmt.Errorf("no record found for %s, %v", table, idValues)
}

func Create[T DB, S any](conn T, dbType DbType, data *S, table string) (map[string]int64, error) {
	fieldMap, pkMap := StructToDbMap(data)
	for k, v := range pkMap {
		fieldMap[k] = v
	}
	qms, keys, values, err := MapForSqlInsert(fieldMap, dbType)
	if err != nil {
		return nil, err
	}
	SqlSafe(&table)
	sqlStatement := fmt.Sprintf(`INSERT INTO %s (%s) VALUES (%s)`, table, keys, qms)
	return Exec(conn, sqlStatement, values...)
}

func Update[T DB, S any](conn T, dbType DbType, data *S, table string) (map[string]int64, error) {
	nonPkMap, pkMap := StructToDbMap(data)
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

func Delete[T DB, S any](conn T, dbType DbType, data *S, table string) (map[string]int64, error) {
	_, pkMap := StructToDbMap(data)
	where, whereValues, err := MapForSqlWhere(pkMap, 0, dbType)
	if err != nil {
		return nil, err
	}
	SqlSafe(&table)
	sqlStatement := fmt.Sprintf(`DELETE FROM %s WHERE 1=1 %s`, table, where)
	fmt.Println(sqlStatement)
	return Exec(conn, sqlStatement, whereValues...)
}

// Exec - run sql and return the number of rows affected
func Exec[T DB](conn T, sqlStatement string, sqlParams ...any) (map[string]int64, error) {
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
	ret := map[string]int64{
		"rows_affected": rowsffected,
	}
	lastInsertId, err := result.LastInsertId()
	if err == nil {
		ret["last_insert_id"] = lastInsertId
	}
	return ret, nil
}

func toCamel(s string) (ret string) {
	s = strings.ToLower(s)
	a := strings.Split(s, "_")
	for i, v := range a {
		if i == 0 {
			ret += v
		} else {
			f := strings.ToUpper(string(v[0]))
			n := string(v[1:])
			ret += fmt.Sprint(f, n)
		}
	}
	return
}

func SqlSafe(s *string) {
	*s = strings.Replace(*s, "'", "''", -1)
	*s = strings.Replace(*s, "--", "", -1)
}

func StructFieldToDbField[T any](s *T) (fields []string, pks []string) {
	structValue := reflect.ValueOf(s).Elem()
	for fieldIndex := 0; fieldIndex < structValue.NumField(); fieldIndex++ {
		fieldTag := structValue.Type().Field(fieldIndex).Tag
		dbTag := fieldTag.Get("db")
		if dbTag != "" {
			fields = append(fields, dbTag)
		}
		pkTag := fieldTag.Get("pk")
		if pkTag == "true" {
			pks = append(pks, dbTag)
		}
	}
	return
}

func StructToDbMap[T any](s *T) (nonPkMap map[string]any, pkMap map[string]any) {
	nonPkMap = make(map[string]any)
	pkMap = make(map[string]any)
	structValue := reflect.ValueOf(s).Elem()
	for fieldIndex := 0; fieldIndex < structValue.NumField(); fieldIndex++ {
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
	} else if dbType == MSSQLServer {
		return fmt.Sprintf("@p%d", index+1)
	} else if dbType == Oracle {
		return fmt.Sprintf(":%d", index+1)
	} else {
		return "?"
	}
}
