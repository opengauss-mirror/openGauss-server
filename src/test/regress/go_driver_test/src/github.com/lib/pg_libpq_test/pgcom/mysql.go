package pgcom

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"log"
	"strconv"
	"strings"
)


func PrintRowData(rows *sql.Rows){
	columns,err :=rows.Columns()
	if err!=nil{
		log.Fatal("rows coulumns error",err)
	}
	cols:=len(columns)

	var scanResults = make([]interface{}, cols)
	for i := 0; i < cols; i++ {
		var s sql.NullString
		scanResults[i] = &s
	}
	if err :=rows.Scan(scanResults...);err!=nil{
		log.Fatal("rows Scan error, ",err)
	}
	for i,key:=range columns{
		s := scanResults[i].(*sql.NullString)
		if !s.Valid{
			fmt.Println(key+": NULL")
		}else {
			fmt.Println(key+": "+s.String)
		}
	}
}

func IterScanRows(rows *sql.Rows)(idx int){
	for rows.Next(){
		idx++
		PrintRowData(rows)
	}
	return
}

func IterScanDbTable(db *sql.DB, tname string)(idx int){
	sql :="select * from "+tname+" order by 1"
	row,err:=db.Query(sql)
	CheckErr(err)
	return IterScanRows(row)
}

func IterScanStmtTable(stmt *sql.Stmt, tname string)(idx int){
	sql :="select * from "+tname+" order by 1"
	row,err:=stmt.Query(sql)
	CheckErr(err)
	return IterScanRows(row)
}


func IterScanTxTable(txn *sql.Tx, tname string)(idx int){
	sql :="select * from "+tname+" order by 1"
	row,err := txn.Query(sql)
	CheckErr(err)
	return IterScanRows(row)
}

type QueryInterface interface {
	Query(query string, args ...interface{}) (*sql.Rows, error)
}

func IterScanTable(sqlObject interface{}, tname string)(idx int){
	sql :="select * from "+tname+" order by 1"
	if fQuery,ok:=sqlObject.(QueryInterface);ok{
		row,err := fQuery.Query(sql)
		CheckErr(err)
		return IterScanRows(row)
	} else {
		log.Fatal("cannot parse your input args")
	}
	return -1
}

func PrintQuery(db *sql.DB, sql string){
	rows,err:=db.Query(sql)
	if err !=nil{
		//t.Fatal(err)
		fmt.Println(err)
		panic(err)
	}
	sqls :=strings.Split(sql,";")
	tab_idx :=0
LOOP:
	tab_idx++
	fmt.Println("Query["+strconv.Itoa(tab_idx)+"]==> "+sqls[tab_idx-1])
	rowsCount := IterScanRows(rows)
	fmt.Println("<==Query["+strconv.Itoa(tab_idx)+"]"+" has "+strconv.Itoa(rowsCount)+" rows.")
	if rows.NextResultSet(){
		goto LOOP
	}
}


func GetTableRowCounts(db *sql.DB,table string)(rows int){
	sql :="select count(*) from "+table;
	err :=db.QueryRow(sql).Scan(&rows)
	if err!=nil{
		log.Fatal("query "+table+" row counts error",err)
	}
	return
}
func PrintTableRowCounts(db *sql.DB,table string){
	fmt.Printf("%s : %d\n",table, GetTableRowCounts(db,table))
}

func TruncateTable(db *sql.DB,table string){
	sql :="select count(*) from "+table;
	var rows int
	err :=db.QueryRow(sql).Scan(&rows)
	res,err:=db.Exec("truncate table "+table)
	if err!=nil{
		log.Fatal("truncate table error,",err)
	}
	_,err = res.RowsAffected()
	if err!=nil{
		log.Fatal("get rows affected error,",err)
	}
}

func CheckTabExists(db *sql.DB, schema,table string)bool{
	sql :="select count(*) from information_schema.columns where table_schema='"+schema+"' and table_name='"+table+"'"
	var rowcount int
	if err :=db.QueryRow(sql).Scan(&rowcount);err!=nil{
		fmt.Println(err)
		return false
	}
	return rowcount==1
}

func PrintListenPort(db *sql.DB){
	var ips,port string
	err :=db.QueryRow("select (select setting from pg_settings where name ='listen_addresses'), (select setting from pg_settings where name ='port')").Scan(&ips,&port)
	if err!=nil{
		log.Fatal("PrintListenPort error,",err)
	}
	fmt.Println(ips,port)
}

func SqlExec(db *sql.DB, sql string, args ...interface{}) {
	r,err :=db.Exec(sql,args...)
	if err !=nil{
		log.Fatal("sql["+sql+"] exec error,",err)
	}
	n,err:=r.RowsAffected()
	if err!=nil{
		log.Fatal("sql["+sql+"] exec rows affect error,",err)
	}
	log.Println("sql["+sql+"] exec rows affect=",n)
}