package bench

import (
	"context"
	"database/sql"
	"fmt"
	. "github.com/GaussKernel/common"
	"log"
	"testing"
	"time"
)

func TestTx(t *testing.T){
	insertRows := 10
	db :=GetDefaultOGDB()
	CheckErr(db.Exec("drop table if exists testtx1"))
	CheckErr(db.Exec("create table testtx1(id int,name varchar(8000))"))

	st,err:=db.Prepare("insert into testtx1(id,name) values(:id,:name)")
	CheckErr(err)
	for i:=0;i<insertRows;i++{
		tname:=GetRandomString(4)
		CheckErr(st.Exec(sql.Named("id",i),sql.Named("name",tname)))
	}
	PrintTableRowCounts(db,"testtx1")
	tx,err:=db.Begin()
	CheckErr(err)
	stmt,err :=tx.Prepare("delete from testtx1 where id=:id")
	CheckErr(err)
	idArg :=sql.Named("id",1)
	res,err :=stmt.Exec(idArg)
	CheckErr(err)
	rowsA,err:=res.RowsAffected()
	CheckErr(err)
	fmt.Println(rowsA)
	PrintTableRowCounts(db,"testtx1")  // expect 10
	var rowCounts int
	err = tx.QueryRow("select count(*) from testtx1").Scan(&rowCounts)
	if err!=nil || rowCounts != 9 {
		log.Fatal("tx queryrow error",err)
	}
	err = tx.QueryRow("select count(*) from testtx1 where id=:id",idArg).Scan(&rowCounts)
	if err!=nil || rowCounts != 0 {
		log.Fatal("tx queryrow error",err)
	}
	tx.Rollback()
	if GetTableRowCounts(db,"testtx1") != insertRows{
		log.Fatal("tx queryrow error")
	}
	CheckErr(db.Exec("drop table if exists testtx1"))
	db.Close()
}

func TestTxOption(t *testing.T){
	db :=GetDefaultOGDB()
	defer db.Close()
	CheckErr(db.Exec("drop table if exists testtx2"))
	CheckErr(db.Exec("create table testtx2(id int,name varchar(8000))"))
	tx,err:=db.BeginTx(context.Background(),&sql.TxOptions{sql.LevelDefault,true})
	CheckErr(err)
	_,err = tx.Exec("delete from testtx2 where id =:id",sql.Named("id",1))
	if err==nil{
		log.Fatal("expect delete error")
	}
	expectErrMgs := "cannot execute DELETE in a read-only transaction"
	if GetDriverErrorMessage(err) != expectErrMgs{
		log.Fatal("tx error message changed")
	}
	CheckErr(db.Exec("drop table if exists testtx2"))

}

func TestTxPrepare(t *testing.T){
	db:=GetDefaultOGDB()
	defer db.Close()

	CheckErr(db.Exec("drop table if exists testtx3"))
	CheckErr(db.Exec("create table testtx3(id int,name varchar(8000))"))
	insertRows := 100

	tx,err:=db.Begin()
	CheckErr(err)
	stmt,err:=tx.Prepare("insert into testtx3 values(:id,:name)")
	CheckErr(err)
	for i:=0;i<insertRows;i++{
		tname:=GetRandomString(20)
		CheckErr(stmt.Exec(sql.Named("id",i),sql.Named("name",tname)))
	}
	var rows int
	err = tx.QueryRow("select count(*) from testtx3").Scan(&rows)
	if err!=nil || rows !=insertRows{
		log.Fatal("tx prepare err")
	}
	tx.Rollback()
	CheckErr(db.Exec("drop table if exists testtx3"))
}

func TestTxDml(t *testing.T){
	db :=GetDefaultOGDB()
	defer db.Close()
	CheckErr(db.Exec("drop table if exists testtx4"))
	CheckErr(db.Exec("create table testtx4(id int,name varchar(8000))"))
	tx,err :=db.Begin()
	CheckErr(err)
	CheckErr(tx.Exec("insert into testtx4 values(1,'abc')"))
	var res string
	CheckErr(tx.QueryRow("select name from testtx4 where id=1").Scan(&res))
	fmt.Println(res)

	CheckErr(tx.ExecContext(context.Background(),"update testtx4 set name='def' where id=1"))
	CheckErr(tx.QueryRow("select name from testtx4 where id=1").Scan(&res))
	fmt.Println(res)

	CheckErr(tx.Exec("delete testtx4 where id=1"))
	IterScanTxTable(tx,"testtx4")
	CheckErr(tx.Rollback())
	CheckErr(db.Exec("drop table if exists testtx4"))
}

func TestTxCommit(t *testing.T){
	db :=GetDefaultOGDB()
	defer db.Close()
	CheckErr(db.Exec("drop table if exists testtx6"))
	CheckErr(db.Exec("create table testtx6(id int,name varchar(8000))"))

	tx,err :=db.Begin()
	CheckErr(err)
	CheckErr(tx.Exec("insert into testtx6 values(1,'abc')"))
	IterScanTxTable(tx,"testtx6")
	CheckErr(tx.Commit())
	IterScanDbTable(db,"testtx6")
	CheckErr(db.Exec("drop table if exists testtx6"))
}

func TestTxRollback(t *testing.T){
	db :=GetDefaultOGDB()
	CheckErr(db.Exec("drop table if exists testtx7"))
	CheckErr(db.Exec("create table testtx7(id int,name varchar(8000))"))
	defer db.Close()
	tx,err :=db.Begin()
	CheckErr(err)
	CheckErr(tx.Exec("insert into testtx7 values(1,'abc')"))
	IterScanTxTable(tx,"testtx7")
	CheckErr(tx.Rollback())
	IterScanDbTable(db,"testtx7")
	CheckErr(db.Exec("drop table if exists testtx7"))
}

func TestTxConnSameIdTest(t *testing.T){
	db :=GetDefaultOGDB()
	tx,_:=db.Begin()
	sqlText :="select pg_backend_pid()"
	var res,res2 string
	err := tx.QueryRow(sqlText).Scan(&res)
	if err!=nil{
		log.Fatal(err)
	}
	for i:=0;i<10;i++{
		err = tx.QueryRow(sqlText).Scan(&res2)
		if err!=nil{
			log.Fatal(err)
		}
		if res2!=res{
			log.Fatal("tx conn should have save backend pid")
		}
	}
}

func TestTxWithoutOpts1(t *testing.T) {
	fmt.Println("********************Tx without opts: Scenario 1********************")
	dsn := GetOGDsnBase()
	db, err:= sql.Open("opengauss", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	sqls := []string {
		"drop table if exists testTxWithoutOpts",
		"create table testTxWithoutOpts(f1 int, f2 varchar(20), f3 number, f4 timestamptz, f5 boolean)",
		"insert into testTxWithoutOpts values(1, '华为', 123.3, '2022-02-08 10:30:43.31 +08', true)",
		"insert into testTxWithoutOpts values(2, '研究所', 123.3, '2022-02-08 10:30:43.31 +08', false)",
		"update testTxWithoutOpts set f3 = f3 + 100 where f5 = true",
		"update testTxWithoutOpts set f3 = f3 - 100 where f5 = false",
	}

	for i := 0; i <= 3; i++ {
		_, err = db.Exec(sqls[i])
		if err != nil {
			t.Fatal(err)
		}
	}

	tx1, err := db.Begin()   // default: read committed
	if err != nil {
		t.Fatal(err)
	}
	defer tx1.Rollback()

	for i := 4; i <= 5; i++ {
		_, err = tx1.Exec(sqls[i])
		if err != nil {
			t.Fatal(err)
		}
	}

	var f1 int
	var f2 string
	var f3 float64
	var f4 time.Time
	var f5 bool

	fmt.Println("----------IN TX1: update and select----------")
	row, err := tx1.Query("select * from testTxWithoutOpts")
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

	tx2, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx2.Rollback()

	fmt.Println("----------IN TX2: select, and TX1 not commit ----------")
	row, err = tx2.Query("select * from testTxWithoutOpts")
	if err != nil {
		t.Fatal(err)
	}
	for row.Next() {
		err = row.Scan(&f1, &f2, &f3, &f4, &f5)
		if err != nil {
			t.Fatal(err)
		} else {
			fmt.Printf("f1:%v, f2:%v, f3:%v, f4:%v, f5:%v\n", f1, f2, f3, f4, f5)
		}
	}

	err = tx1.Rollback()
	if err != nil {
		t.Fatal(err)
	}

	err = tx2.Commit()
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println("----------IN DB: select, after TX1 rollback and TX2 commit----------")
	row, err = db.Query("select * from testTxWithoutOpts")
	if err != nil {
		t.Fatal(err)
	}
	for row.Next() {
		err = row.Scan(&f1, &f2, &f3, &f4, &f5)
		if err != nil {
			t.Fatal(err)
		} else {
			fmt.Printf("f1:%v, f2:%v, f3:%v, f4:%v, f5:%v\n", f1, f2, f3, f4, f5)
		}
	}
}

func TestTxWithoutOpts2(t *testing.T) {
	fmt.Println("********************Tx without opts: Scenario 2********************")
	dsn := GetOGDsnBase()
	db, err:= sql.Open("opengauss", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	sqls := []string {
		"drop table if exists testTxWithoutOpts",
		"create table testTxWithoutOpts(f1 int, f2 varchar(20), f3 number, f4 timestamptz, f5 boolean)",
		"insert into testTxWithoutOpts values(1, '华为', 123.3, '2022-02-08 10:30:43.31 +08', true)",
		"insert into testTxWithoutOpts values(2, '研究所', 123.3, '2022-02-08 10:30:43.31 +08', false)",
		"update testTxWithoutOpts set f3 = f3 + 100 where f5 = true",
		"update testTxWithoutOpts set f3 = f3 - 100 where f5 = false",
		"update testTxWithoutOpts set f3 = f3 + 300 where f5 = true",
		"update testTxWithoutOpts set f3 = f3 - 300 where f5 = false",
	}

	for i := 0; i <= 3; i++ {
		_, err = db.Exec(sqls[i])
		if err != nil {
			t.Fatal(err)
		}
	}

	tx1, err := db.Begin()   // default: read committed
	if err != nil {
		t.Fatal(err)
	}
	defer tx1.Rollback()

	for i := 4; i <= 5; i++ {
		_, err = tx1.Exec(sqls[i])
		if err != nil {
			t.Fatal(err)
		}
	}

	var f1 int
	var f2 string
	var f3 float64
	var f4 time.Time
	var f5 bool

	fmt.Println("----------IN TX1: update and select----------")
	row, err := tx1.Query("select * from testTxWithoutOpts")
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

	err = tx1.Rollback()
	if err != nil {
		t.Fatal(err)
	}

	tx2, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx2.Rollback()

	fmt.Println("----------IN TX2: update and select, and after TX1 roolback ----------")

	for i := 6; i <= 7; i++ {
		_, err = tx2.Exec(sqls[i])
		if err != nil {
			t.Fatal(err)
		}
	}

	row, err = tx2.Query("select * from testTxWithoutOpts")
	if err != nil {
		t.Fatal(err)
	}
	for row.Next() {
		err = row.Scan(&f1, &f2, &f3, &f4, &f5)
		if err != nil {
			t.Fatal(err)
		} else {
			fmt.Printf("f1:%v, f2:%v, f3:%v, f4:%v, f5:%v\n", f1, f2, f3, f4, f5)
		}
	}

	fmt.Println("----------IN DB: select, after TX1 rollback and TX2 not commit----------")
	row, err = db.Query("select * from testTxWithoutOpts")
	if err != nil {
		t.Fatal(err)
	}
	for row.Next() {
		err = row.Scan(&f1, &f2, &f3, &f4, &f5)
		if err != nil {
			t.Fatal(err)
		} else {
			fmt.Printf("f1:%v, f2:%v, f3:%v, f4:%v, f5:%v\n", f1, f2, f3, f4, f5)
		}
	}

	err = tx2.Commit()
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println("----------IN DB: select, after TX1 rollback and TX2 commit----------")
	row, err = db.Query("select * from testTxWithoutOpts")
	if err != nil {
		t.Fatal(err)
	}
	for row.Next() {
		err = row.Scan(&f1, &f2, &f3, &f4, &f5)
		if err != nil {
			t.Fatal(err)
		} else {
			fmt.Printf("f1:%v, f2:%v, f3:%v, f4:%v, f5:%v\n", f1, f2, f3, f4, f5)
		}
	}
}

func TestTxWithoutOpts3(t *testing.T) {
	fmt.Println("********************Tx without opts: Scenario 3********************")
	dsn := GetOGDsnBase()
	db, err:= sql.Open("opengauss", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	sqls := []string {
		"drop table if exists testTxWithoutOpts",
		"create table testTxWithoutOpts(f1 int, f2 varchar(20), f3 number, f4 timestamptz, f5 boolean)",
		"insert into testTxWithoutOpts values(1, '华为', 123.3, '2022-02-08 10:30:43.31 +08', true)",
		"insert into testTxWithoutOpts values(2, '研究所', 123.3, '2022-02-08 10:30:43.31 +08', false)",
		"update testTxWithoutOpts set f3 = f3 + 100 where f5 = true",
		"update testTxWithoutOpts set f3 = f3 - 100 where f5 = false",
		"update testTxWithoutOpts set f3 = f3 + 300 where f5 = true",
		"update testTxWithoutOpts set f3 = f3 - 300 where f5 = false",
	}

	for i := 0; i <= 3; i++ {
		_, err = db.Exec(sqls[i])
		if err != nil {
			t.Fatal(err)
		}
	}

	tx1, err := db.Begin()   // default: read committed
	if err != nil {
		t.Fatal(err)
	}
	defer tx1.Rollback()

	for i := 4; i <= 5; i++ {
		_, err = tx1.Exec(sqls[i])
		if err != nil {
			t.Fatal(err)
		}
	}

	var f1 int
	var f2 string
	var f3 float64
	var f4 time.Time
	var f5 bool

	fmt.Println("----------IN TX1: update and select----------")
	row, err := tx1.Query("select * from testTxWithoutOpts")
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

	err = tx1.Commit()
	if err != nil {
		t.Fatal(err)
	}

	tx2, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx2.Rollback()

	fmt.Println("----------IN TX2: update and select, and after TX1 commit ----------")

	for i := 6; i <= 7; i++ {
		_, err = tx2.Exec(sqls[i])
		if err != nil {
			t.Fatal(err)
		}
	}

	row, err = tx2.Query("select * from testTxWithoutOpts")
	if err != nil {
		t.Fatal(err)
	}
	for row.Next() {
		err = row.Scan(&f1, &f2, &f3, &f4, &f5)
		if err != nil {
			t.Fatal(err)
		} else {
			fmt.Printf("f1:%v, f2:%v, f3:%v, f4:%v, f5:%v\n", f1, f2, f3, f4, f5)
		}
	}

	fmt.Println("----------IN DB: select, after TX1 commit and TX2 not commit----------")
	row, err = db.Query("select * from testTxWithoutOpts")
	if err != nil {
		t.Fatal(err)
	}
	for row.Next() {
		err = row.Scan(&f1, &f2, &f3, &f4, &f5)
		if err != nil {
			t.Fatal(err)
		} else {
			fmt.Printf("f1:%v, f2:%v, f3:%v, f4:%v, f5:%v\n", f1, f2, f3, f4, f5)
		}
	}

	err = tx2.Commit()
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println("----------IN DB: select, after TX1 commit and TX2 commit----------")
	row, err = db.Query("select * from testTxWithoutOpts")
	if err != nil {
		t.Fatal(err)
	}
	for row.Next() {
		err = row.Scan(&f1, &f2, &f3, &f4, &f5)
		if err != nil {
			t.Fatal(err)
		} else {
			fmt.Printf("f1:%v, f2:%v, f3:%v, f4:%v, f5:%v\n", f1, f2, f3, f4, f5)
		}
	}
}

func TestTxWithOpts1(t *testing.T) {
	fmt.Println("********************Tx with opts: Scenario 1********************")
	ctx := context.Background()
	ctx2SecondTimeout, cancelFunc2SecondTimeout := context.WithTimeout(ctx, 5 * time.Second)
	defer cancelFunc2SecondTimeout()

	dsn := GetOGDsnBase()
	db, err:= sql.Open("opengauss", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	sqls := []string {
		"drop table if exists testTxWithOpts",
		"create table testTxWithOpts(f1 int, f2 varchar(20), f3 number, f4 timestamptz, f5 boolean)",
		"insert into testTxWithOpts values(1, '华为', 123.3, '2022-02-08 10:30:43.31 +08', true)",
		"insert into testTxWithOpts values(2, '研究所', 123.3, '2022-02-08 10:30:43.31 +08', false)",
		"update testTxWithOpts set f3 = f3 + 100 where f5 = true",
		"update testTxWithOpts set f3 = f3 - 100 where f5 = false",
	}

	for i := 0; i <= 3; i++ {
		_, err = db.ExecContext(ctx2SecondTimeout, sqls[i])
		if err != nil {
			t.Fatal(err)
		}
	}

	opts := []sql.IsolationLevel{ sql.LevelReadUncommitted, sql.LevelReadCommitted, sql.LevelRepeatableRead }

	for _, opt := range opts {
		fmt.Printf("****ISOLATION LEVEL:%v****\n", opt)
		// start tx1
		tx1, err := db.BeginTx(ctx2SecondTimeout, &sql.TxOptions{
			Isolation: opt,
			ReadOnly:  false,
		})
		if err != nil {
			t.Fatal(err)
		}

		for i := 4; i <= 5; i++ {
			_, err = tx1.ExecContext(ctx2SecondTimeout, sqls[i])
			if err != nil {
				t.Fatal(err)
			}
		}

		var f1 int
		var f2 string
		var f3 float64
		var f4 time.Time
		var f5 bool

		fmt.Println("----------IN TX1: update and select----------")
		row, err := tx1.QueryContext(ctx2SecondTimeout, "select * from testTxWithOpts")
		if err != nil {
			t.Fatal(err)
		}

		for row.Next() {
			err = row.Scan(&f1, &f2, &f3, &f4, &f5)
			if err != nil {
				t.Fatal(err)
			} else {
				fmt.Printf("f1:%v, f2:%v, f3:%v, f4:%v, f5:%v\n", f1, f2, f3, f4, f5)
			}
		}

		// start tx2
		tx2, err := db.BeginTx(ctx2SecondTimeout, &sql.TxOptions{
			Isolation: opt,
			ReadOnly:  false,
		})
		if err != nil {
			t.Fatal(err)
		}

		fmt.Println("----------IN TX2: select, and TX1 not commit ----------")
		row, err = tx2.QueryContext(ctx2SecondTimeout, "select * from testTxWithOpts")
		if err != nil {
			t.Fatal(err)
		}

		for row.Next() {
			err = row.Scan(&f1, &f2, &f3, &f4, &f5)
			if err != nil {
				t.Fatal(err)
			} else {
				fmt.Printf("f1:%v, f2:%v, f3:%v, f4:%v, f5:%v\n", f1, f2, f3, f4, f5)
			}
		}

		err = tx1.Rollback()
		if err != nil {
			t.Fatal(err)
		}

		err = tx2.Commit()
		if err != nil {
			tx2.Rollback()
			t.Fatal(err)
		}

		fmt.Println("----------IN DB: select, after TX1 rollback and TX2 commit----------")
		row, err = db.QueryContext(ctx2SecondTimeout, "select * from testTxWithOpts")
		if err != nil {
			t.Fatal(err)
		}

		for row.Next() {
			err = row.Scan(&f1, &f2, &f3, &f4, &f5)
			if err != nil {
				t.Fatal(err)
			} else {
				fmt.Printf("f1:%v, f2:%v, f3:%v, f4:%v, f5:%v\n", f1, f2, f3, f4, f5)
			}
		}
		row.Close()
	}
}

func TestTxWithOpts2(t *testing.T) {
	fmt.Println("********************Tx with opts: Scenario 2********************")
	ctx := context.Background()
	ctx2SecondTimeout, cancelFunc2SecondTimeout := context.WithTimeout(ctx, 5 * time.Second)
	defer cancelFunc2SecondTimeout()

	dsn := GetOGDsnBase()
	db, err:= sql.Open("opengauss", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	sqls := []string {
		"drop table if exists testTxWithOpts",
		"create table testTxWithOpts(f1 int, f2 varchar(20), f3 number, f4 timestamptz, f5 boolean)",
		"insert into testTxWithOpts values(1, '华为', 123.3, '2022-02-08 10:30:43.31 +08', true)",
		"insert into testTxWithOpts values(2, '研究所', 123.3, '2022-02-08 10:30:43.31 +08', false)",
		"update testTxWithOpts set f3 = f3 + 100 where f5 = true",
		"update testTxWithOpts set f3 = f3 - 100 where f5 = false",
		"update testTxWithOpts set f3 = f3 + 300 where f5 = true",
		"update testTxWithOpts set f3 = f3 - 300 where f5 = false",
	}

	for i := 0; i <= 3; i++ {
		_, err = db.ExecContext(ctx2SecondTimeout, sqls[i])
		if err != nil {
			t.Fatal(err)
		}
	}

	opts := []sql.IsolationLevel{ sql.LevelReadUncommitted, sql.LevelReadCommitted, sql.LevelRepeatableRead }

	for _, opt := range opts {
		fmt.Printf("****ISOLATION LEVEL:%v****\n", opt)
		// start tx1
		tx1, err := db.BeginTx(ctx2SecondTimeout, &sql.TxOptions{
			Isolation: opt,
			ReadOnly:  false,
		})
		if err != nil {
			t.Fatal(err)
		}

		for i := 4; i <= 5; i++ {
			_, err = tx1.ExecContext(ctx2SecondTimeout, sqls[i])
			if err != nil {
				t.Fatal(err)
			}
		}

		var f1 int
		var f2 string
		var f3 float64
		var f4 time.Time
		var f5 bool

		fmt.Println("----------IN TX1: update and select----------")
		row, err := tx1.QueryContext(ctx2SecondTimeout, "select * from testTxWithOpts")
		if err != nil {
			t.Fatal(err)
		}
		for row.Next() {
			err = row.Scan(&f1, &f2, &f3, &f4, &f5)
			if err != nil {
				t.Fatal(err)
			} else {
				fmt.Printf("f1:%v, f2:%v, f3:%v, f4:%v, f5:%v\n", f1, f2, f3, f4, f5)
			}
		}

		err = tx1.Rollback()
		if err != nil {
			t.Fatal(err)
		}

		// start tx2
		tx2, err := db.BeginTx(ctx2SecondTimeout, &sql.TxOptions{
			Isolation: opt,
			ReadOnly:  false,
		})
		if err != nil {
			t.Fatal(err)
		}

		fmt.Println("----------IN TX2: update and select, and after TX1 roolback ----------")

		for i := 6; i <= 7; i++ {
			_, err = tx2.ExecContext(ctx2SecondTimeout, sqls[i])
			if err != nil {
				t.Fatal(err)
			}
		}

		row, err = tx2.QueryContext(ctx2SecondTimeout, "select * from testTxWithOpts")
		if err != nil {
			t.Fatal(err)
		}
		for row.Next() {
			err = row.Scan(&f1, &f2, &f3, &f4, &f5)
			if err != nil {
				t.Fatal(err)
			} else {
				fmt.Printf("f1:%v, f2:%v, f3:%v, f4:%v, f5:%v\n", f1, f2, f3, f4, f5)
			}
		}

		fmt.Println("----------IN DB: select, after TX1 rollback and TX2 not commit----------")
		row, err = db.QueryContext(ctx2SecondTimeout, "select * from testTxWithOpts")
		if err != nil {
			t.Fatal(err)
		}
		for row.Next() {
			err = row.Scan(&f1, &f2, &f3, &f4, &f5)
			if err != nil {
				t.Fatal(err)
			} else {
				fmt.Printf("f1:%v, f2:%v, f3:%v, f4:%v, f5:%v\n", f1, f2, f3, f4, f5)
			}
		}

		err = tx2.Commit()
		if err != nil {
			t.Fatal(err)
		}

		fmt.Println("----------IN DB: select, after TX1 rollback and TX2 commit----------")
		row, err = db.QueryContext(ctx2SecondTimeout, "select * from testTxWithOpts")
		if err != nil {
			t.Fatal(err)
		}
		for row.Next() {
			err = row.Scan(&f1, &f2, &f3, &f4, &f5)
			if err != nil {
				t.Fatal(err)
			} else {
				fmt.Printf("f1:%v, f2:%v, f3:%v, f4:%v, f5:%v\n", f1, f2, f3, f4, f5)
			}
		}
		row.Close()
	}
}

func TestTxWithOpts3(t *testing.T) {
	fmt.Println("********************Tx with opts: Scenario 3********************")
	ctx := context.Background()
	ctx2SecondTimeout, cancelFunc2SecondTimeout := context.WithTimeout(ctx, 5 * time.Second)
	defer cancelFunc2SecondTimeout()

	dsn := GetOGDsnBase()
	db, err:= sql.Open("opengauss", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	sqls := []string {
		"drop table if exists testTxWithOpts",
		"create table testTxWithOpts(f1 int, f2 varchar(20), f3 number, f4 timestamptz, f5 boolean)",
		"insert into testTxWithOpts values(1, '华为', 123.3, '2022-02-08 10:30:43.31 +08', true)",
		"insert into testTxWithOpts values(2, '研究所', 123.3, '2022-02-08 10:30:43.31 +08', false)",
		"update testTxWithOpts set f3 = f3 + 100 where f5 = true",
		"update testTxWithOpts set f3 = f3 - 100 where f5 = false",
		"update testTxWithOpts set f3 = f3 + 300 where f5 = true",
		"update testTxWithOpts set f3 = f3 - 300 where f5 = false",
	}

	for i := 0; i <= 3; i++ {
		_, err = db.ExecContext(ctx2SecondTimeout, sqls[i])
		if err != nil {
			t.Fatal(err)
		}
	}

	opts := []sql.IsolationLevel{ sql.LevelReadUncommitted, sql.LevelReadCommitted, sql.LevelRepeatableRead }

	for _, opt := range opts {
		fmt.Printf("****ISOLATION LEVEL:%v****\n", opt)
		// start tx1
		tx1, err := db.BeginTx(ctx2SecondTimeout, &sql.TxOptions{
			Isolation: opt,
			ReadOnly:  false,
		})
		if err != nil {
			t.Fatal(err)
		}

		for i := 4; i <= 5; i++ {
			_, err = tx1.ExecContext(ctx2SecondTimeout, sqls[i])
			if err != nil {
				t.Fatal(err)
			}
		}

		var f1 int
		var f2 string
		var f3 float64
		var f4 time.Time
		var f5 bool

		fmt.Println("----------IN TX1: update and select----------")
		row, err := tx1.QueryContext(ctx2SecondTimeout, "select * from testTxWithoutOpts")
		if err != nil {
			t.Fatal(err)
		}
		for row.Next() {
			err = row.Scan(&f1, &f2, &f3, &f4, &f5)
			if err != nil {
				t.Fatal(err)
			} else {
				fmt.Printf("f1:%v, f2:%v, f3:%v, f4:%v, f5:%v\n", f1, f2, f3, f4, f5)
			}
		}

		err = tx1.Commit()
		if err != nil {
			t.Fatal(err)
		}

		tx2, err := db.BeginTx(ctx2SecondTimeout, &sql.TxOptions{
			Isolation: opt,
			ReadOnly:  false,
		})
		if err != nil {
			t.Fatal(err)
		}

		fmt.Println("----------IN TX2: update and select, and after TX1 commit ----------")

		for i := 6; i <= 7; i++ {
			_, err = tx2.ExecContext(ctx2SecondTimeout, sqls[i])
			if err != nil {
				t.Fatal(err)
			}
		}

		row, err = tx2.QueryContext(ctx2SecondTimeout, "select * from testTxWithoutOpts")
		if err != nil {
			t.Fatal(err)
		}
		for row.Next() {
			err = row.Scan(&f1, &f2, &f3, &f4, &f5)
			if err != nil {
				t.Fatal(err)
			} else {
				fmt.Printf("f1:%v, f2:%v, f3:%v, f4:%v, f5:%v\n", f1, f2, f3, f4, f5)
			}
		}

		fmt.Println("----------IN DB: select, after TX1 commit and TX2 not commit----------")
		row, err = db.QueryContext(ctx2SecondTimeout, "select * from testTxWithoutOpts")
		if err != nil {
			t.Fatal(err)
		}
		for row.Next() {
			err = row.Scan(&f1, &f2, &f3, &f4, &f5)
			if err != nil {
				t.Fatal(err)
			} else {
				fmt.Printf("f1:%v, f2:%v, f3:%v, f4:%v, f5:%v\n", f1, f2, f3, f4, f5)
			}
		}

		err = tx2.Commit()
		if err != nil {
			t.Fatal(err)
		}

		fmt.Println("----------IN DB: select, after TX1 commit and TX2 commit----------")
		row, err = db.QueryContext(ctx2SecondTimeout, "select * from testTxWithoutOpts")
		if err != nil {
			t.Fatal(err)
		}
		for row.Next() {
			err = row.Scan(&f1, &f2, &f3, &f4, &f5)
			if err != nil {
				t.Fatal(err)
			} else {
				fmt.Printf("f1:%v, f2:%v, f3:%v, f4:%v, f5:%v\n", f1, f2, f3, f4, f5)
			}
		}
		row.Close()
	}
}

func TestTxWithOptsReadOnly(t *testing.T) {
	fmt.Println("********************Tx with opts and read only********************")
	ctx := context.Background()
	ctx2SecondTimeout, cancelFunc2SecondTimeout := context.WithTimeout(ctx, 2 * time.Second)
	defer cancelFunc2SecondTimeout()

	dsn := GetOGDsnBase()
	db, err:= sql.Open("opengauss", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	sqls := []string {
		"drop table if exists testTxWithOptsReadOnly",
		"create table testTxWithOptsReadOnly(f1 int, f2 varchar(20), f3 number, f4 timestamptz, f5 boolean)",
		"insert into testTxWithOptsReadOnly values(1, '华为', 123.3, '2022-02-08 10:30:43.31 +08', true)",
	}

	for i := 0; i <= 2; i++ {
		_, err = db.ExecContext(ctx2SecondTimeout, sqls[i])
		if err != nil {
			t.Fatal(err)
		}
	}

	// start tx1
	tx, err := db.BeginTx(ctx2SecondTimeout, &sql.TxOptions{
		Isolation: sql.LevelReadCommitted,
		ReadOnly:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	// in read only, cannot execute write operation, only execute read operation
	_, err = tx.ExecContext(ctx2SecondTimeout, "update testTxWithOptsReadOnly set f3 = f3 + 100 where f5 = true")
	if err != nil {
		fmt.Println("In a read-only transaction, cannot execute write operation.")
	}

	var f1 int
	var f2 string
	var f3 float64
	var f4 time.Time
	var f5 bool
	// current transaction is aborted, an error is reported.
	err = tx.QueryRowContext(ctx2SecondTimeout, "select * from testTxWithOptsReadOnly").Scan(&f1,&f2,&f3,&f4,&f5)
	if err != nil {
		fmt.Println("current transaction is aborted.")
	}
}

func TestTxWithoutOptsStmt(t *testing.T) {
	fmt.Println("********************Tx without opts and stmt********************")
	dsn := GetOGDsnBase()
	db, err:= sql.Open("opengauss", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	sqls := []string {
		"drop table if exists testTxWithoutOptsStmt",
		"create table testTxWithoutOptsStmt(f1 int, f2 varchar(20), f3 number, f4 timestamptz, f5 boolean)",
		"insert into testTxWithoutOptsStmt values(1, '华为', 123.3, '2022-02-08 10:30:43.31 +08', true)",
	}

	for i := 0; i <= 2; i++ {
		_, err = db.Exec(sqls[i])
		if err != nil {
			t.Fatal(err)
		}
	}

	// start tx1
	tx1, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}


	s1, err := tx1.Prepare("update testTxWithoutOptsStmt set f3 = f3 + 100")
	if err != nil {
		t.Fatal(err)
	}

	_, err = s1.Exec()
	if err != nil {
		t.Fatal(err)
	}
	err = tx1.Rollback()
	if err != nil {
		t.Fatal(err)
	}
	s1.Close()

	// start tx2
	tx2, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	s2, err := tx2.Prepare("update testTxWithoutOptsStmt set f3 = f3 + 300")
	if err != nil {
		t.Fatal(err)
	}

	_, err = tx2.Stmt(s2).Exec()
	if err != nil {
		t.Fatal(err)
	}
	err = tx2.Commit()
	if err != nil {
		t.Fatal(err)
	}
	s2.Close()

	var f1 int
	var f2 string
	var f3 float64
	var f4 time.Time
	var f5 bool
	// current transaction is aborted, an error is reported.
	err = db.QueryRow("select * from testTxWithoutOptsStmt").Scan(&f1,&f2,&f3,&f4,&f5)
	if err != nil {
		t.Fatal(err)
	} else {
		fmt.Printf("f1:%v, f2:%v, f3:%v, f4:%v, f5:%v\n", f1, f2, f3, f4, f5)
	}
}

func TestTxWithOptsStmt(t *testing.T) {
	fmt.Println("********************Tx with opts and stmt********************")
	ctx := context.Background()
	ctx2SecondTimeout, cancelFunc2SecondTimeout := context.WithTimeout(ctx, 2 * time.Second)
	defer cancelFunc2SecondTimeout()

	dsn := GetOGDsnBase()
	db, err:= sql.Open("opengauss", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	sqls := []string {
		"drop table if exists testTxWithOptsStmt",
		"create table testTxWithOptsStmt(f1 int, f2 varchar(20), f3 number, f4 timestamptz, f5 boolean)",
		"insert into testTxWithOptsStmt values(1, '华为', 123.3, '2022-02-08 10:30:43.31 +08', true)",
	}

	for i := 0; i <= 2; i++ {
		_, err = db.ExecContext(ctx2SecondTimeout, sqls[i])
		if err != nil {
			t.Fatal(err)
		}
	}

	// start tx1
	tx1, err := db.BeginTx(ctx2SecondTimeout, &sql.TxOptions{
		Isolation: sql.LevelReadCommitted,
		ReadOnly:  false,
	})
	if err != nil {
		t.Fatal(err)
	}


	s1, err := tx1.PrepareContext(ctx2SecondTimeout, "update testTxWithOptsStmt set f3 = f3 + 100")
	if err != nil {
		t.Fatal(err)
	}

	_, err = s1.ExecContext(ctx2SecondTimeout)
	if err != nil {
		t.Fatal(err)
	}
	err = tx1.Rollback()
	if err != nil {
		t.Fatal(err)
	}
	s1.Close()

	// start tx2
	tx2, err := db.BeginTx(ctx2SecondTimeout, &sql.TxOptions{
		Isolation: sql.LevelReadCommitted,
		ReadOnly:  false,
	})
	if err != nil {
		t.Fatal(err)
	}

	s2, err := tx2.PrepareContext(ctx2SecondTimeout, "update testTxWithOptsStmt set f3 = f3 + 300")
	if err != nil {
		t.Fatal(err)
	}

	_, err = tx2.StmtContext(ctx2SecondTimeout, s2).ExecContext(ctx2SecondTimeout)
	if err != nil {
		t.Fatal(err)
	}
	err = tx2.Commit()
	if err != nil {
		t.Fatal(err)
	}
	s2.Close()

	var f1 int
	var f2 string
	var f3 float64
	var f4 time.Time
	var f5 bool
	// current transaction is aborted, an error is reported.
	err = db.QueryRowContext(ctx2SecondTimeout, "select * from testTxWithOptsStmt").Scan(&f1,&f2,&f3,&f4,&f5)
	if err != nil {
		t.Fatal(err)
	} else {
		fmt.Printf("f1:%v, f2:%v, f3:%v, f4:%v, f5:%v\n", f1, f2, f3, f4, f5)
	}
}