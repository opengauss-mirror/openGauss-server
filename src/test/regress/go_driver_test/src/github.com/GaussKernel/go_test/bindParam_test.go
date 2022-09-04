package bench

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/GaussKernel/common"
	"log"
	"strings"
	"testing"
	"time"
)

func QueryStmt_noctx(pool *sql.DB) {
	st,err :=pool.Prepare("select :name,:id;")
	if err!=nil{
		log.Fatal("prepare error",err)
	}
	row,err :=st.Query(sql.Named("name","liu"),sql.Named("id","3"))
	if  err!=nil {
		log.Fatal("query error",err)
	}
	cs,err:=row.Columns()
	if err!=nil{
		log.Fatal("get columns error",err)
	}
	fmt.Println(cs)
	columns, err := row.ColumnTypes()
	if err!=nil{
		log.Fatal("get col error",err)
	}
	for _,col :=range columns{
		fmt.Println(col.Length())
		fmt.Println(col.Name(),col.ScanType())
		fmt.Println(col.DecimalSize())
		fmt.Println(col.Nullable())
	}
	common.IterScanRows(row)
	row.Close()
	st.Close()
}

func QueryStmtCtx(pool *sql.DB) {
	st,err :=pool.Prepare("select :name as name,:id as ID;")
	if err!=nil{
		log.Fatal("prepare error",err)
	}
	res,err:=st.Exec(sql.Named("name","liu"),sql.Named("id",1.34))
	if err!=nil{
		log.Fatal(err)
	}
	fmt.Println(res.RowsAffected())
	row,err :=st.QueryContext(context.Background(),sql.Named("name","liu"),sql.Named("id",3.34))
	if  err !=nil {
		log.Fatal("query error",err)
	}
	cs,err:=row.Columns()
	if err!=nil{
		log.Fatal("get columns error",err)
	}
	fmt.Println(cs)
	columns, err := row.ColumnTypes()
	if err!=nil{
		log.Fatal("get col error",err)
	}
	for _,col :=range columns{
		fmt.Println(col.Length())
		fmt.Println(col.Name(),col.ScanType())
		fmt.Println(col.DecimalSize())
		fmt.Println(col.Nullable())
	}
	common.IterScanRows(row)
	row.Close()
	st.Close()
}

func QueryBindWithoutName(pool *sql.DB) {
	st,err :=pool.Prepare("select :name as name,:id as ID;")
	if err!=nil{
		log.Fatal("prepare error",err)
	}

	res,err:=st.Exec("liuhang",3)
	if err!=nil{
		log.Fatal(err)
	}
	fmt.Println(res.RowsAffected())
}

func TestBindParam(t *testing.T){
	pool:=common.GetDefaultOGDB()
	QueryStmtCtx(pool)
	QueryStmt_noctx(pool)
	QueryBindWithoutName(pool)
}

func TestBinaryParam(t *testing.T){
	dsn :=common.GetOGDsnBase()
	dsn +=" binary_parameters=yes"
	db,err:=sql.Open("opengauss",dsn)
	common.CheckErr(err)
	defer db.Close()
	tx,err:=db.Begin()
	common.CheckErr(err)
	var name,id string
	err =tx.QueryRow("select :name as name,:id as ID;",sql.Named("name","abc"),sql.Named("id",[]byte("123"))).Scan(&name,&id)
	common.CheckErr(err)
	fmt.Println(name,id)
	stmt,err:=tx.Prepare("select :name as name,:id as ID;")
	common.CheckErr(err)
	err = stmt.QueryRow(sql.Named("name","abc"),sql.Named("id",123)).Scan(&name,&id)
}

func TestBindWithLocation(t *testing.T) {
	fmt.Println("********************Bind With Location********************")
	dsn := common.GetOGDsnBase()
	db, err:= sql.Open("opengauss", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	sqls := []string {
		"drop table if exists testBindWithLocation",
		"create table testBindWithLocation(f1 int, f2 varchar(20), f3 number, f4 timestamptz, f5 boolean)",
		"insert into testBindWithLocation values(:f1, :f2, :f3, :f4, :f5)",
	}

	inF1 := []int{2, 3, 4, 5, 6}
	intF2 := []string{"hello world", "华为", "北京2022冬奥会", "nanjing", "研究所"}
	intF3 := []float64{641.43, 431.54, 5423.52, 665537.63, 6503.1}
	intF4 := []time.Time{
		time.Date(2022, 2, 8, 10, 35, 43, 623431, time.Local),
		time.Date(2022, 2, 10, 19, 11, 54, 353431, time.Local),
		time.Date(2022, 2, 12, 6, 11, 15, 636431, time.Local),
		time.Date(2022, 2, 14, 4, 51, 22, 747653, time.Local),
		time.Date(2022, 2, 16, 13, 45, 55, 674636, time.Local),
	}
	intF5 := []bool{false, true, false, true, true}

	for _, str := range sqls {
		if strings.Contains(str, ":f") {
			for i, _ := range inF1 {
				_, err := db.Exec(str, inF1[i], intF2[i], intF3[i], intF4[i], intF5[i])
				if err != nil {
					t.Fatal(err)
				}
			}
		} else {
			_, err = db.Exec(str)
			if err != nil {
				t.Fatal(err)
			}
		}
	}

	var f1 int
	var f2 string
	var f3 float64
	var f4 time.Time
	var f5 bool
	row, err :=db.Query("select * from testBindWithLocation")
	if err != nil {
		t.Fatal(err)
	}
	defer row.Close()

	for row.Next() {
		err = row.Scan(&f1, &f2, &f3, &f4, &f5)
		if err != nil {
			t.Fatal(err)
		} else {
			fmt.Printf("f1:%v, f2:%v, f3:%v, f4:%v, f5:%v\n", f1, f2, f3, f4, f5)
		}
	}

	if row.Err() != nil {
		t.Fatal(err)
	}
}

func TestBindWithName(t *testing.T) {
	fmt.Println("********************Bind With Name********************")
	dsn := common.GetOGDsnBase()
	db, err:= sql.Open("opengauss", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	sqls := []string {
		"drop table if exists testBindWithName",
		"create table testBindWithName(f1 int, f2 varchar(20), f3 number, f4 timestamptz, f5 boolean)",
		"insert into testBindWithName values(:f1, :f2, :f3, :f4, :f5)",
	}

	inF1 := []int{2, 3, 4, 5, 6}
	inF2 := []string{"hello world", "华为", "北京2022冬奥会", "nanjing", "研究所"}
	inF3 := []float64{641.43, 431.54, 5423.52, 665537.63, 6503.1}
	inF4 := []time.Time{
		time.Date(2022, 2, 8, 10, 35, 43, 623431, time.Local),
		time.Date(2022, 2, 10, 19, 11, 54, 353431, time.Local),
		time.Date(2022, 2, 12, 6, 11, 15, 636431, time.Local),
		time.Date(2022, 2, 14, 4, 51, 22, 747653, time.Local),
		time.Date(2022, 2, 16, 13, 45, 55, 674636, time.Local),
	}
	inF5 := []bool{false, true, false, true, true}

	for _, str := range sqls {
		if strings.Contains(str, ":f") {
			for i, _ := range inF1 {
				_, err := db.Exec(str, sql.Named("f1", inF1[i]), sql.Named("f2", inF2[i]), sql.Named("f3", inF3[i]),
					sql.Named("f4", inF4[i]), sql.Named("f5", inF5[i]))
				if err != nil {
					t.Fatal(err)
				}
			}
		} else {
			_, err = db.Exec(str)
			if err != nil {
				t.Fatal(err)
			}
		}
	}

	var f1 int
	var f2 string
	var f3 float64
	var f4 time.Time
	var f5 bool
	row, err :=db.Query("select * from testBindWithName")
	if err != nil {
		t.Fatal(err)
	}
	defer row.Close()

	for row.Next() {
		err = row.Scan(&f1, &f2, &f3, &f4, &f5)
		if err != nil {
			t.Fatal(err)
		} else {
			fmt.Printf("f1:%v, f2:%v, f3:%v, f4:%v, f5:%v\n", f1, f2, f3, f4, f5)
		}
	}

	if row.Err() != nil {
		t.Fatal(err)
	}
}
