package bench

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/GaussKernel/common"
	"strings"
	"testing"
	"time"
)

func TestDBExec(t *testing.T) {
	fmt.Println("********************DB Exec********************")
	dsn := common.GetOGDsnBase()
	db, err:= sql.Open("opengauss", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		t.Fatal(err)
	}

	sqls := []string {
		"drop table if exists testExec",
		"create table testExec(f1 int, f2 varchar(20), f3 number, f4 timestamptz, f5 boolean)",
		"insert into testExec values(1, 'abcdefg', 123.3, '2022-02-08 10:30:43.31 +08', true)",
		"insert into testExec values(:f1, :f2, :f3, :f4, :f5)",
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
	err = db.QueryRow("select * from testExec").Scan(&f1, &f2, &f3, &f4, &f5)
	if err != nil {
		t.Fatal(err)
	} else {
		fmt.Printf("f1:%v, f2:%v, f3:%v, f4:%v, f5:%v\n", f1, f2, f3, f4, f5)
	}

	row, err :=db.Query("select * from testExec where f1 > :1", 1)
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
}

func TestDBExecContext(t *testing.T) {
	fmt.Println("********************DB ExecContext********************")
	ctx := context.Background()
	ctx2SecondTimeout, cancelFunc2SecondTimeout := context.WithTimeout(ctx, 2 * time.Second)
	defer cancelFunc2SecondTimeout()

	dsn := common.GetOGDsnBase()
	db, err:= sql.Open("opengauss", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Ping database connection with 2 second timeout
	err = db.PingContext(ctx2SecondTimeout)
	if err != nil {
		t.Fatal(err)
	}

	sqls := []string {
		"drop table if exists testExecContext",
		"create table testExecContext(f1 int, f2 varchar(20), f3 number, f4 timestamptz, f5 boolean)",
		"insert into testExecContext values(1, 'abcdefg', 123.3, '2022-02-08 10:30:43.31 +08', true)",
		"insert into testExecContext values(:f1, :f2, :f3, :f4, :f5)",
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
				_, err := db.ExecContext(ctx2SecondTimeout, str, inF1[i], intF2[i], intF3[i], intF4[i], intF5[i])
				if err != nil {
					t.Fatal(err)
				}
			}
		} else {
			_, err = db.ExecContext(ctx2SecondTimeout, str)
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
	err = db.QueryRowContext(ctx2SecondTimeout, "select * from testExecContext").Scan(&f1, &f2, &f3, &f4, &f5)
	if err != nil {
		t.Fatal(err)
	} else {
		fmt.Printf("f1:%v, f2:%v, f3:%v, f4:%v, f5:%v\n", f1, f2, f3, f4, f5)
	}

	row, err :=db.QueryContext(ctx2SecondTimeout, "select * from testExecContext where f1 > :1", 1)
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
}

