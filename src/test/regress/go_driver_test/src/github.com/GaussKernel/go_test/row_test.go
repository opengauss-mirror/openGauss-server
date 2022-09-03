package bench

import (
	"database/sql"
	"fmt"
	"github.com/GaussKernel/common"
	"strings"
	"testing"
	"time"
)

func TestRows(t *testing.T) {
	fmt.Println("********************Query with Row********************")
	dsn := common.GetOGDsnBase()
	db, err:= sql.Open("opengauss", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	sqls := []string {
		"drop table if exists testRows",
		"create table testRows(f1 int, f2 varchar(20), f3 number, f4 timestamptz, f5 boolean)",
		"insert into testRows values(1, 'abcdefg', 123.3, '2022-02-08 10:30:43.31 +08', true)",
		"insert into testRows values(:f1, :f2, :f3, :f4, :f5)",
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
	err = db.QueryRow("select * from testRows").Scan(&f1, &f2, &f3, &f4, &f5)
	if err != nil {
		t.Fatal(err)
	} else {
		fmt.Printf("f1:%v, f2:%v, f3:%v, f4:%v, f5:%v\n", f1, f2, f3, f4, f5)
	}

	row, err :=db.Query("select * from testRows where f1 > :1", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer row.Close()

	column, err := row.Columns()
	if err != nil {
		t.Fatal(err)
	} else {
		fmt.Printf("The name of columns is ")
		for _, c := range column {
			fmt.Printf("%v ",c)
		}
	}

	fmt.Println("\n********************Query with Rows********************")
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

func TestColumnType(t *testing.T) {
	fmt.Println("********************Get ColumnType********************")
	dsn := common.GetOGDsnBase()
	db, err:= sql.Open("opengauss", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	sqls := []string {
		"drop table if exists testRows",
		"create table testRows(f1 int, f2 VARCHAR(20), f3 number(20,4), f4 timestamptz, f5 boolean, f6 float, f7 money, f8 polygon, f9 bytea)",
		"insert into testRows values(1, 'abcdefg', 123.3, '2022-02-08 10:30:43.31 +08', true, 123.3, '$12.432', '1,0,2,0,3,4', 'deqde')",
		"insert into testRows values(:f1, :f2, :f3, :f4, :f5, :f6, :f7, :f8, :f9)",
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
	intF6 := []float32{641.43, 431.54, 5423.52, 665537.63, 6503.1}
	intF7 := []string{"123.11", "$1234.41", "11.41", "41.41", "$12.525"}
	intF8 := []string{"1,2,3,4,5,6", "2,3,4,5,6,7", "3,4,5,6,7,8", "4,5,6,7,8,9", "1,3,5,7,9,0", "2,4,6,8,0,2"}
	intF9 := []string{"rwr", "fafa", "dfaj", "fhfijheio", "fnak"}

	for _, str := range sqls {
		if strings.Contains(str, ":f") {
			for i, _ := range inF1 {
				_, err := db.Exec(str, inF1[i], intF2[i], intF3[i], intF4[i], intF5[i], intF6[i], intF7[i], intF8[i], intF9[i])
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

	row, err :=db.Query("select * from testRows where f1 > :1", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer row.Close()

	ct, err := row.ColumnTypes()
	if err != nil {
		t.Fatal(err)
	} else {
		for _, c := range ct {
			name := c.Name()
			typeName := c.DatabaseTypeName()
			fmt.Printf("Name is %v, DatabaseTypeName is %v. ", name, typeName)
			precision, scale, isDecimal := c.DecimalSize()
			if isDecimal {
				fmt.Printf("Decimal precision is %v, scale is %v. ", precision, scale)
			}
			length, isLen := c.Length()
			if isLen {
				fmt.Printf("Sting length is %v. ",length)
			}
			scanType := c.ScanType()
			fmt.Printf("ScanType is %v.\n", scanType)
		}
	}

}