package common

import (
	"database/sql"
	"fmt"
	"gitee.com/opengauss/openGauss-connector-go-pq"
	ini "github.com/GaussKernel/ini-main"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
)

var (
	parse_config =make(map[string]string)
	onceDsnBase sync.Once

	t =&testing.T{}
	SQL_QUERYTIME="select current_date"
	//pg
	DSN_PG_WIN = "host=127.0.0.1 port=5433 user=postgres password=Gauss_234 dbname=postgres sslmode=require"
	DSN_PG_L_G="host=7.189.51.79 port=5431 user=gauss password=Gauss_234 dbname=gauss sslmode=disable"
	//openGauss
	DSN_OG_L_G   = GetOGDsnBase()
)


func init() {
	goPath := os.Getenv("GOPATH")

	//filePathPrefix :=d[:strings.Index(d, keyPath)]
	filePathPrefix:=strings.Join([]string{goPath,"src","github.com","GaussKernel"},string(os.PathSeparator))
	iniPath := strings.Join([]string{filePathPrefix,"property.ini"},string(os.PathSeparator))
	cfg, err := ini.Load(iniPath)
	if err!=nil{
		log.Fatal("load property error, ",err)
	}
	for _,key :=range cfg.Section("opengauss").Keys(){
		if len(key.Value())>0{
			parse_config[key.Name()]=key.Value()
		}
	}
	sslPath :=strings.Join([]string{filePathPrefix,"certs"},string(os.PathSeparator))
	parse_config["sslkey"]=strings.Join([]string{sslPath,"postgresql.key"},string(os.PathSeparator))
	parse_config["sslcert"]=strings.Join([]string{sslPath,"postgresql.crt"},string(os.PathSeparator))
	parse_config["sslrootcert"]=strings.Join([]string{sslPath,"root.crt"},string(os.PathSeparator))
}

func GetOGDsnFull()(res string){
	for k,v :=range parse_config{
		res+=" "+k+"="+v
	}
	return
}
func GetOGDsnBase()(res string){
	baseOpt :=[]string{
		"host",
		"port",
		"user",
		"password",
		"dbname",
	}
	for _,opt :=range baseOpt{
		if v,ok:=parse_config[opt];ok{
			res+=" "+opt+"="+v
		}
	}
	return
}
func GetOGUrlBase()(res string){
	return ""
}

func GetPropertyUser()string{
	res,ok := parse_config["user"]
	if ok{
		return res
	}
	log.Fatal("Your should set user in proterty.ini")
	return ""
}
func GetPropertyPort()string{
	res,ok := parse_config["port"]
	if ok{
		return res
	}
	log.Fatal("Your should set port in proterty.ini")
	return ""
}
func GetPropertyDbName()string{
	res,ok := parse_config["dbname"]
	if ok{
		return res
	}
	log.Fatal("Your should set hot in proterty.ini")
	return ""
}
func GetPropertyHost()string{
	res,ok := parse_config["host"]
	if ok{
		return res
	}
	log.Fatal("Your should set host in proterty.ini")
	return ""
}

func GetPropertyPassword()string{
	res,ok := parse_config["password"]
	if ok{
		return res
	}
	log.Fatal("Your should set password in proterty.ini")
	return ""
}

//func GetPropertySSLKey()string{
//	return parse_config["sslkey"]
//}
//func GetPropertySSLCert()string{
//	return parse_config["sslcert"]
//}
//func GetPropertySSLRootCert()string{
//	return parse_config["sslrootcert"]
//}
//func GetPropertySSLDSN()string {
//	return " sslkey="+GetPropertySSLKey()+" sslcert="+GetPropertySSLCert()+" sslrootcert="+GetPropertySSLRootCert()
//}

func GetDefaultOGDB()(*sql.DB){
	db,err :=sql.Open("opengauss",GetOGDsnBase())
	CheckErr(err)
	return db
}

func TestDB(dv string, dsn string){
	db, err := sql.Open(dv, dsn)
	if err != nil {
		CheckErr(err)
	}
	defer db.Close()
	var rc = 0
	err =db.QueryRow("select 1").Scan(&rc)
	CheckErr(err)
	if rc != 1 {
		log.Fatal(dv,dsn,"["+dsn+"]query 1 wrong, rc=",rc)
		return
	}
}

func TestConnExpect(dv string, dsn string, isExpectTrue bool){
	db, err := sql.Open(dv, dsn)
	if err != nil {
		if isExpectTrue {
			log.Fatalf("this dsn \"%v\" should shout be ok, but error: %v",dsn,err)
		}
		return
	}
	defer db.Close()
	var rc = 0
	err =db.QueryRow("select 1").Scan(&rc)
	if err != nil {
		if isExpectTrue {
			log.Fatalf("this dsn \"%v\" should shout be ok, but error: %v",dsn,err)
		}
		return
	}
	if rc != 1 {
		log.Fatal(dv,dsn,"["+dsn+"]query 1 wrong, rc=",rc)
		return
	}
}

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
	rowsCount :=IterScanRows(rows)
	fmt.Println("<==Query["+strconv.Itoa(tab_idx)+"]"+" has "+strconv.Itoa(rowsCount)+" rows.")
	if rows.NextResultSet(){
		goto LOOP
	}
}

func PrintTable(db *sql.DB, tables... string){
	for _,table := range tables{
		sql := "select * from "+table+" order by 1"
		PrintQuery(db,sql)
	}
}

func PrintTableMeta(db *sql.DB, tableName, schameName string){
	getColumn:="SELECT column_name, column_default, is_nullable, data_type, character_maximum_length, description," +
		"    CASE WHEN p.contype = 'p' THEN true ELSE false END AS primarykey," +
		"    CASE WHEN p.contype = 'u' THEN true ELSE false END AS uniquekey" +
		"    FROM pg_attribute f" +
		"    JOIN pg_class c ON c.oid = f.attrelid JOIN pg_type t ON t.oid = f.atttypid" +
		"    LEFT JOIN pg_attrdef d ON d.adrelid = c.oid AND d.adnum = f.attnum" +
		"    LEFT JOIN pg_description de ON f.attrelid=de.objoid AND f.attnum=de.objsubid" +
		"    LEFT JOIN pg_namespace n ON n.oid = c.relnamespace" +
		"    LEFT JOIN pg_constraint p ON p.conrelid = c.oid AND f.attnum = ANY (p.conkey)" +
		"    LEFT JOIN pg_class AS g ON p.confrelid = g.oid" +
		"    LEFT JOIN INFORMATION_SCHEMA.COLUMNS s ON s.column_name=f.attname AND c.relname=s.table_name" +
		"    WHERE n.nspname= s.table_schema AND c.relkind = 'r'::char AND c.relname = $1 AND s.table_schema = $2 AND f.attnum > 0 ORDER BY f.attnum;"
	getIndex :="SELECT indexname, indexdef FROM pg_indexes WHERE tablename=$1 AND schemaname=$2"
	row,err:=db.Query(getColumn,tableName,schameName)
	CheckErr(err)
	IterScanRows(row)
	row,err=db.Query(getIndex,tableName,schameName)
	CheckErr(err)
	IterScanRows(row)
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
	fmt.Printf("%s : %d\n",table,GetTableRowCounts(db,table))
}

func GetDriverErrorMessage(err error)string{
	if nerr,ok :=err.(*pq.Error);ok{
		return nerr.Message
	}
	return ""
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