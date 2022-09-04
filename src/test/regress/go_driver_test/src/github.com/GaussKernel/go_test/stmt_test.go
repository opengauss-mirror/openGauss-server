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

func TestStmtExec(t *testing.T) {
	fmt.Println("********************Stmt Exec********************")
	dsn := common.GetOGDsnBase()
	db, err:= sql.Open("opengauss", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	sqls := []string {
		"drop table if exists testStmtExec",
		"create table testStmtExec(f1 int, f2 varchar(20), f3 number, f4 timestamptz, f5 boolean)",
		"insert into testStmtExec values(1, 'abcdefg', 123.3, '2022-02-08 10:30:43.31 +08', true)",
		"insert into testStmtExec values(:f1, :f2, :f3, :f4, :f5)",
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
		s, err := db.Prepare(str)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(str, ":f") {
			for i, _ := range inF1 {
				_, err := s.Exec(inF1[i], intF2[i], intF3[i], intF4[i], intF5[i])
				if err != nil {
					t.Fatal(err)
				}
			}
		} else {
			_, err = s.Exec()
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
	s, err := db.Prepare("select * from testStmtExec")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	err =s.QueryRow().Scan(&f1, &f2, &f3, &f4, &f5)
	if err != nil {
		t.Fatal(err)
	} else {
		fmt.Printf("f1:%v, f2:%v, f3:%v, f4:%v, f5:%v\n", f1, f2, f3, f4, f5)
	}

	s, err = db.Prepare("select * from testStmtExec where f1 > :1")
	if err != nil {
		t.Fatal(err)
	}
	row, err :=s.Query(1)
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

func TestStmtExecCtx(t *testing.T) {
	fmt.Println("********************Stmt ExecContext********************")
	ctx := context.Background()
	ctx2SecondTimeout, cancelFunc2SecondTimeout := context.WithTimeout(ctx, 2 * time.Second)
	defer cancelFunc2SecondTimeout()

	dsn := common.GetOGDsnBase()
	db, err:= sql.Open("opengauss", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	sqls := []string {
		"drop table if exists testStmtExecCtx",
		"create table testStmtExecCtx(f1 int, f2 varchar(20), f3 number, f4 timestamptz, f5 boolean)",
		"insert into testStmtExecCtx values(1, 'abcdefg', 123.3, '2022-02-08 10:30:43.31 +08', true)",
		"insert into testStmtExecCtx values(:f1, :f2, :f3, :f4, :f5)",
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
		s, err := db.PrepareContext(ctx2SecondTimeout, str)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(str, ":f") {
			for i, _ := range inF1 {
				_, err := s.ExecContext(ctx2SecondTimeout, inF1[i], intF2[i], intF3[i], intF4[i], intF5[i])
				if err != nil {
					t.Fatal(err)
				}
			}
		} else {
			_, err = s.ExecContext(ctx2SecondTimeout)
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
	s, err := db.PrepareContext(ctx2SecondTimeout, "select * from testStmtExecCtx")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	err = s.QueryRowContext(ctx2SecondTimeout).Scan(&f1, &f2, &f3, &f4, &f5)
	if err != nil {
		t.Fatal(err)
	} else {
		fmt.Printf("f1:%v, f2:%v, f3:%v, f4:%v, f5:%v\n", f1, f2, f3, f4, f5)
	}

	s, err = db.PrepareContext(ctx2SecondTimeout, "select * from testStmtExecCtx where f1 > :1")
	if err != nil {
		t.Fatal(err)
	}
	row, err :=s.QueryContext(ctx2SecondTimeout, 1)
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
