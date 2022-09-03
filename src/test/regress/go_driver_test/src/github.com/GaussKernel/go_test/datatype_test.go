package bench

import (
	"database/sql"
	"fmt"
	"github.com/GaussKernel/common"
	"testing"
	"time"
)

func TestDatatype(t *testing.T){
	db :=common.GetDefaultOGDB()
	common.CheckErr(db.Exec("drop table if exists mydata"))
	common.CheckErr(db.Exec("create table mydata(a int, b varchar(100), d decimal(20,10),e tinyint, f boolean, g raw,k tsrange)"))
	common.CheckErr(db.Exec("INSERT INTO mydata VALUES (1,'abc',1.234,127,false,'123456','[2010-01-01 14:30, 2010-01-01 15:30]')"))
	common.CheckErr(db.Exec("INSERT INTO mydata VALUES (2,'abc',1.234,127,true,'223456','[2010-01-01 14:30, 2010-01-01 15:30]')"))

	common.IterScanDbTable(db,"mydata")
	common.CheckErr(db.Exec("drop table if exists mydata"))
	db.Close()
}

func TestNumber(t *testing.T) {
	dsn := common.GetOGDsnBase()
	db, err:= sql.Open("opengauss", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	sqls := []string {
		"drop table if exists testInteger",
		"create table testInteger(f1 TINYINT, f2 SMALLINT, f3 INTEGER, f4 INTEGER, f5 BIGINT)",
		"insert into testInteger values(255, 32767, 2147483647, 2147483647, 9223372036854775807)",
		"insert into testInteger values(0, 0, 0, 0, 0)",
		"insert into testInteger values(0, -32768, -2147483648, -2147483648, -9223372036854775808)",
		"drop table if exists testNumeric",
		"create table testNumeric(f1 numeric, f2 decimal(10,4), f3 number(20))",
		"insert into testNumeric values(437164.41907403710, 71.6149329, 34819647147.0438148321)",
		"drop table if exists testFloat",
		"create table testFloat(f1 float4, f2 float8, f3 float, f4 binary_double, f5 dec(10,4), f6 integer(20,2))",
		"insert into testFloat values(4164.5190, 34847.0438321, 437164.419403710, 71.6149329, 71.693291, 34819647147.04381321)",
		"drop table if exists testSerial",
		"create table testSerial(f1 smallserial, f2 serial, f3 bigserial)",
		"insert into testSerial values(default, default, default)",
	}

	for _, s := range sqls {
		_, err = db.Exec(s)
		if err != nil {
			t.Fatal(err)
		}
	}

	infos := []struct {
		descrip, datatype string
	} {
		{"----------result is golang string----------", "string"},
		{"\n----------result is golang interface{}----------", "interface{}"},
		{"\n----------result is golang []byte----------", "[]byte"},
		{"\n----------result is golang sql.RawBytes----------", "sql.RawBytes"},
		{"\n----------result is golang bool----------", "bool"},
		{"\n----------result is golang integer----------", "integer"},
		{"\n----------result is golang float----------", "float"},
		{"\n----------result is golang others----------", "others"},
	}

	queryInfos := []struct {
		typeDescrip, querySql string
	} {
		{"********************Integer Datatype********************", "select *,i1toi2(123) from testInteger"},
		{"\n********************Numeric Datatype********************", "select *,* from testNumeric"},
		{"\n********************Float Datatype********************", "select * from testFloat"},
		{"\n********************Serial Datatype********************", "select *,* from testSerial"},
	}

	row, err := db.Query("select 1")
	if err != nil {
		t.Fatal(err)
	}
	defer row.Close()

	for _, q := range queryInfos {
		fmt.Println(q.typeDescrip)

		for _, info := range infos {
			fmt.Println(info.descrip)

			row, err = db.Query(q.querySql)
			if err != nil {
				t.Fatal(err)
			}

			for row.Next() {
				switch info.datatype {
				case "string":
					var f1, f2, f3, f4, f5, f6 string
					err = row.Scan(&f1, &f2, &f3, &f4, &f5, &f6)
					if err != nil {
						fmt.Println(err)
					} else {
						fmt.Printf("F1:%v, F2:%v, F3:%v, F4:%v, F5:%v, F6:%v\n", f1, f2, f3, f4, f5, f6)
					}
				case "interface{}":
					var f1, f2, f3, f4, f5, f6 interface{}
					err = row.Scan(&f1, &f2, &f3, &f4, &f5, &f6)
					if err != nil {
						fmt.Println(err)
					} else {
						fmt.Printf("F1:%v, F2:%v, F3:%v, F4:%v, F5:%v, F6:%v\n", f1, f2, f3, f4, f5, f6)
					}
				case "[]byte":
					var f1, f2, f3, f4, f5, f6 []byte
					err = row.Scan(&f1, &f2, &f3, &f4, &f5, &f6)
					if err != nil {
						fmt.Println(err)
					} else {
						fmt.Printf("F1:%v, F2:%v, F3:%v, F4:%v, F5:%v, F6:%v\n", f1, f2, f3, f4, f5, f6)
					}
				case "sql.RawBytes":
					var f1, f2, f3, f4, f5, f6 sql.RawBytes
					err = row.Scan(&f1, &f2, &f3, &f4, &f5, &f6)
					if err != nil {
						fmt.Println(err)
					} else {
						fmt.Printf("F1:%v, F2:%v, F3:%v, F4:%v, F5:%v, F6:%v\n", f1, f2, f3, f4, f5, f6)
					}
				case "bool":
					var f1, f2, f3, f4, f5, f6 bool
					err = row.Scan(&f1, &f2, &f3, &f4, &f5, &f6)
					if err != nil {
						fmt.Println(err)
					} else {
						fmt.Printf("F1:%v, F2:%v, F3:%v, F4:%v, F5:%v, F6:%v\n", f1, f2, f3, f4, f5, f6)
					}
				case "integer":
					var f1, f2, f3, f4, f5, f6 int64
					err = row.Scan(&f1, &f2, &f3, &f4, &f5, &f6)
					if err != nil {
						fmt.Println(err)
					} else {
						fmt.Printf("F1:%v, F2:%v, F3:%v, F4:%v, F5:%v, F6:%v\n", f1, f2, f3, f4, f5, f6)
					}
				case "float":
					var f1, f2, f3, f4, f5, f6 float64
					err = row.Scan(&f1, &f2, &f3, &f4, &f5, &f6)
					if err != nil {
						fmt.Println(err)
					} else {
						fmt.Printf("F1:%v, F2:%v, F3:%v, F4:%v, F5:%v, F6:%v\n", f1, f2, f3, f4, f5, f6)
					}
				case "others":
					var f1, f2, f3, f4, f5, f6 time.Time
					err = row.Scan(&f1, &f2, &f3, &f4, &f5, &f6)
					if err != nil {
						fmt.Println(err)
					} else {
						fmt.Printf("F1:%v, F2:%v, F3:%v, F4:%v, F5:%v, F6:%v\n", f1, f2, f3, f4, f5, f6)
					}
				}
			}

			if err = row.Err(); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestDateAndTime(t *testing.T) {
	dsn := common.GetOGDsnBase()
	db, err:= sql.Open("opengauss", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	sqls := []string {
		"drop table if exists testTime",
		"create table testTime(f1 date, f2 time, f3 timetz, f4 timestamp, f5 timestamptz)",
		"insert into testTime values('2022-01-28', '14:56:38.687146', '14:56:38.687146 GMT', '2022-01-28 14:56:38.687146', '2022-01-28 15:01:28.382264 GMT')",
		"drop table if exists testInterval",
		"create table testInterval(f1 smalldatetime, f2 interval day(1) to second(2), f3 interval hour(2), f4 reltime, f5 abstime)",
		"insert into testInterval values('2022-01-28 15:01:00', interval '3' day, interval '2' year, '2022 year 1 mons 28 days 15:00:00', '2022-1-28 15:00:00 GMT')",
	}

	for _, s := range sqls {
		_, err = db.Exec(s)
		if err != nil {
			t.Fatal(err)
		}
	}

	infos := []struct {
		descrip, datatype string
	} {
		{"----------result is golang string----------", "string"},
		{"\n----------result is golang interface{}----------", "interface{}"},
		{"\n----------result is golang []byte----------", "[]byte"},
		{"\n----------result is golang sql.RawBytes----------", "sql.RawBytes"},
		{"\n----------result is golang time.Time----------", "time.Time"},
		{"\n----------result is golang others----------", "others"},
	}

	queryInfos := []struct {
		typeDescrip, querySql string
	} {
		{"********************Datetime Datatype********************", "select * from testTime"},
		{"\n********************Interval Datatype********************", "select * from testInterval"},
	}

	row, err := db.Query("select 1")
	if err != nil {
		t.Fatal(err)
	}
	defer row.Close()

	for _, q := range queryInfos {
		fmt.Println(q.typeDescrip)

		for _, info := range infos {
			fmt.Println(info.descrip)

			row, err = db.Query(q.querySql)
			if err != nil {
				t.Fatal(err)
			}

			for row.Next() {
				switch info.datatype {
				case "string":
					var f1, f2, f3, f4, f5 string
					err = row.Scan(&f1, &f2, &f3, &f4, &f5)
					if err != nil {
						fmt.Println(err)
					} else {
						fmt.Printf("F1:%v, F2:%v, F3:%v, F4:%v, F5:%v\n", f1, f2, f3, f4, f5)
					}
				case "interface{}":
					var f1, f2, f3, f4, f5 interface{}
					err = row.Scan(&f1, &f2, &f3, &f4, &f5)
					if err != nil {
						fmt.Println(err)
					} else {
						fmt.Printf("F1:%v, F2:%v, F3:%v, F4:%v, F5:%v\n", f1, f2, f3, f4, f5)
					}
				case "[]byte":
					var f1, f2, f3, f4, f5 []byte
					err = row.Scan(&f1, &f2, &f3, &f4, &f5)
					if err != nil {
						fmt.Println(err)
					} else {
						fmt.Printf("F1:%v, F2:%v, F3:%v, F4:%v, F5:%v\n", f1, f2, f3, f4, f5)
					}
				case "sql.RawBytes":
					var f1, f2, f3, f4, f5 sql.RawBytes
					err = row.Scan(&f1, &f2, &f3, &f4, &f5)
					if err != nil {
						fmt.Println(err)
					} else {
						fmt.Printf("F1:%v, F2:%v, F3:%v, F4:%v, F5:%v\n", f1, f2, f3, f4, f5)
					}
				case "time.Time":
					var f1, f2, f3, f4, f5 time.Time
					err = row.Scan(&f1, &f2, &f3, &f4, &f5)
					if err != nil {
						fmt.Println(err)
					} else {
						fmt.Printf("F1:%v, F2:%v, F3:%v, F4:%v, F5:%v\n", f1, f2, f3, f4, f5)
					}
				case "others":
					var f1, f2, f3, f4, f5 int64
					err = row.Scan(&f1, &f2, &f3, &f4, &f5)
					if err != nil {
						fmt.Println(err)
					} else {
						fmt.Printf("F1:%v, F2:%v, F3:%v, F4:%v, F5:%v\n", f1, f2, f3, f4, f5)
					}
				}
			}

			if err = row.Err(); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestMoneyAndBool(t *testing.T) {
	dsn := common.GetOGDsnBase()
	db, err:= sql.Open("opengauss", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	sqls := []string {
		"drop table if exists testMoney",
		"create table testMoney(f1 money)",
		"insert into testMoney values(100), (2090.11), ('$12.432')",
		"drop table if exists testBool",
		"create table testBool(f1 bool)",
		"insert into testBool values('t'),('no'),(6473)",
	}

	for _, s := range sqls {
		_, err = db.Exec(s)
		if err != nil {
			t.Fatal(err)
		}
	}

	infos := []struct {
		descrip, datatype string
	} {
		{"----------result is golang string----------", "string"},
		{"\n----------result is golang interface{}----------", "interface{}"},
		{"\n----------result is golang []byte----------", "[]byte"},
		{"\n----------result is golang sql.RawBytes----------", "sql.RawBytes"},
		{"\n----------result is golang bool----------", "bool"},
		{"\n----------result is golang others----------", "others"},
	}

	queryInfos := []struct {
		typeDescrip, querySql string
	} {
		{"********************Money Datatype********************", "select * from testMoney"},
		{"\n********************Bool Datatype********************", "select * from testBool"},
	}

	row, err := db.Query("select 1")
	if err != nil {
		t.Fatal(err)
	}
	defer row.Close()

	for _, q := range queryInfos {
		fmt.Println(q.typeDescrip)

		for _, info := range infos {
			fmt.Println(info.descrip)

			row, err = db.Query(q.querySql)
			if err != nil {
				t.Fatal(err)
			}

			for row.Next() {
				switch info.datatype {
				case "string":
					var f1 string
					err = row.Scan(&f1)
					if err != nil {
						fmt.Println(err)
					} else {
						fmt.Printf("F1:%v\n", f1)
					}
				case "interface{}":
					var f1 interface{}
					err = row.Scan(&f1)
					if err != nil {
						fmt.Println(err)
					} else {
						fmt.Printf("F1:%v\n", f1)
					}
				case "[]byte":
					var f1 []byte
					err = row.Scan(&f1)
					if err != nil {
						fmt.Println(err)
					} else {
						fmt.Printf("F1:%v\n", f1)
					}
				case "sql.RawBytes":
					var f1 sql.RawBytes
					err = row.Scan(&f1)
					if err != nil {
						fmt.Println(err)
					} else {
						fmt.Printf("F1:%v\n", f1)
					}
				case "bool":
					var f1 bool
					err = row.Scan(&f1)
					if err != nil {
						fmt.Println(err)
					} else {
						fmt.Printf("F1:%v\n", f1)
					}
				case "others":
					var f1 int64
					err = row.Scan(&f1)
					if err != nil {
						fmt.Println(err)
					} else {
						fmt.Printf("F1:%v\n", f1)
					}
				}
			}

			if err = row.Err(); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestCharacterAndBinary(t *testing.T) {
	dsn := common.GetOGDsnBase()
	db, err:= sql.Open("opengauss", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	sqls := []string {
		"drop table if exists testCharacter",
		"create table testCharacter(f1 char(1), f2 nchar(4), f3 varchar(10), f4 varchar2(10), f5 nvarchar2(2), f6 text, f7 clob)",
		"insert into testCharacter values('a', '你', 'bbb', '2090.11', '中国', 'dgjqgdgfakgfekikgqif', 'dgjqgdgfakgfekikgqif')",
		"insert into testCharacter values('1', 'true', '0', 'false', 'f', 'TRUE', 'FALSE')",
		"insert into testCharacter values('1', '2000', '0', '4', '5', '6', '7')",
		"drop table if exists testBinary",
		"create table testBinary(f1 blob, f2 raw, f3 bytea)",
		"insert into testBinary values('123','abc','deqde')",
	}

	for _, s := range sqls {
		_, err = db.Exec(s)
		if err != nil {
			t.Fatal(err)
		}
	}

	infos := []struct {
		descrip, datatype string
	} {
		{"----------result is golang string----------", "string"},
		{"\n----------result is golang interface{}----------", "interface{}"},
		{"\n----------result is golang []byte----------", "[]byte"},
		{"\n----------result is golang sql.RawBytes----------", "sql.RawBytes"},
		{"\n----------result is golang bool----------", "bool"},
		{"\n----------result is golang integer----------", "integer"},
		{"\n----------result is golang float----------", "float"},
		{"\n----------result is golang others----------", "others"},
	}

	queryInfos := []struct {
		typeDescrip, querySql string
	} {
		{"********************Character Datatype********************", "select * from testCharacter"},
		{"\n********************Binary Datatype********************", "select *,*,f1 from testBinary"},
	}

	row, err := db.Query("select 1")
	if err != nil {
		t.Fatal(err)
	}
	defer row.Close()

	for _, q := range queryInfos {
		fmt.Println(q.typeDescrip)

		for _, info := range infos {
			fmt.Println(info.descrip)

			row, err = db.Query(q.querySql)
			if err != nil {
				t.Fatal(err)
			}

			for row.Next() {
				switch info.datatype {
				case "string":
					var f1, f2, f3, f4, f5, f6, f7 string
					err = row.Scan(&f1, &f2, &f3, &f4, &f5, &f6, &f7)
					if err != nil {
						fmt.Println(err)
					} else {
						fmt.Printf("F1:%v, F2:%v, F3:%v, F4:%v, F5:%v, F6:%v, F7:%v\n", f1, f2, f3, f4, f5, f6, f7)
					}
				case "interface{}":
					var f1, f2, f3, f4, f5, f6, f7 interface{}
					err = row.Scan(&f1, &f2, &f3, &f4, &f5, &f6, &f7)
					if err != nil {
						fmt.Println(err)
					} else {
						fmt.Printf("F1:%v, F2:%v, F3:%v, F4:%v, F5:%v, F6:%v, F7:%v\n", f1, f2, f3, f4, f5, f6, f7)
					}
				case "[]byte":
					var f1, f2, f3, f4, f5, f6, f7 []byte
					err = row.Scan(&f1, &f2, &f3, &f4, &f5, &f6, &f7)
					if err != nil {
						fmt.Println(err)
					} else {
						fmt.Printf("F1:%v, F2:%v, F3:%v, F4:%v, F5:%v, F6:%v, F7:%v\n", f1, f2, f3, f4, f5, f6, f7)
					}
				case "sql.RawBytes":
					var f1, f2, f3, f4, f5, f6, f7 sql.RawBytes
					err = row.Scan(&f1, &f2, &f3, &f4, &f5, &f6, &f7)
					if err != nil {
						t.Fatal(err)
					} else {
						fmt.Printf("F1:%v, F2:%v, F3:%v, F4:%v, F5:%v, F6:%v, F7:%v\n", f1, f2, f3, f4, f5, f6, f7)
					}
				case "bool":
					var f1, f2, f3, f4, f5, f6, f7 bool
					err = row.Scan(&f1, &f2, &f3, &f4, &f5, &f6, &f7)
					if err != nil {
						fmt.Println(err)
					} else {
						fmt.Printf("F1:%v, F2:%v, F3:%v, F4:%v, F5:%v, F6:%v, F7:%v\n", f1, f2, f3, f4, f5, f6, f7)
					}
				case "integer":
					var f1, f2, f3, f4, f5, f6, f7 int64
					err = row.Scan(&f1, &f2, &f3, &f4, &f5, &f6, &f7)
					if err != nil {
						fmt.Println(err)
					} else {
						fmt.Printf("F1:%v, F2:%v, F3:%v, F4:%v, F5:%v, F6:%v, F7:%v\n", f1, f2, f3, f4, f5, f6, f7)
					}
				case "float":
					var f1, f2, f3, f4, f5, f6, f7 float64
					err = row.Scan(&f1, &f2, &f3, &f4, &f5, &f6, &f7)
					if err != nil {
						fmt.Println(err)
					} else {
						fmt.Printf("F1:%v, F2:%v, F3:%v, F4:%v, F5:%v, F6:%v, F7:%v\n", f1, f2, f3, f4, f5, f6, f7)
					}
				case "others":
					var f1, f2, f3, f4, f5, f6, f7 time.Time
					err = row.Scan(&f1, &f2, &f3, &f4, &f5, &f6, &f7)
					if err != nil {
						fmt.Println(err)
					} else {
						fmt.Printf("F1:%v, F2:%v, F3:%v, F4:%v, F5:%v, F6:%v, F7:%v\n", f1, f2, f3, f4, f5, f6, f7)
					}
				}
			}

			if err = row.Err(); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestGeometry(t *testing.T) {
	dsn := common.GetOGDsnBase()
	db, err:= sql.Open("opengauss", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	sqls := []string {
		"drop table if exists testGeometry",
		"create table testGeometry(f1 point, f2 lseg, f3 box, f4 path, f5 polygon, f6 circle)",
		"insert into testGeometry values('1,2', '1,2,3,4', '1,1,2,2', '1,0,2,0,3,4', '1,0,2,0,3,4', '0,0,2')",
	}

	for _, s := range sqls {
		_, err = db.Exec(s)
		if err != nil {
			t.Fatal(err)
		}
	}

	infos := []struct {
		descrip, datatype string
	} {
		{"----------result is golang string----------", "string"},
		{"\n----------result is golang interface{}----------", "interface{}"},
		{"\n----------result is golang []byte----------", "[]byte"},
		{"\n----------result is golang sql.RawBytes----------", "sql.RawBytes"},
		{"\n----------result is golang others----------", "others"},
	}

	fmt.Println("********************Geometry Datatype********************")
	row, err := db.Query("select 1")
	if err != nil {
		t.Fatal(err)
	}
	defer row.Close()

	for _, info := range infos {
		fmt.Println(info.descrip)

		row, err = db.Query("select * from testGeometry")
		if err != nil {
			t.Fatal(err)
		}

		for row.Next() {
			switch info.datatype {
			case "string":
				var f1, f2, f3, f4, f5, f6 string
				err = row.Scan(&f1, &f2, &f3, &f4, &f5, &f6)
				if err != nil {
					fmt.Println(err)
				} else {
					fmt.Printf("F1:%v, F2:%v, F3:%v, F4:%v, F5:%v, F6:%v\n", f1, f2, f3, f4, f5, f6)
				}
			case "interface{}":
				var f1, f2, f3, f4, f5, f6 interface{}
				err = row.Scan(&f1, &f2, &f3, &f4, &f5, &f6)
				if err != nil {
					fmt.Println(err)
				} else {
					fmt.Printf("F1:%v, F2:%v, F3:%v, F4:%v, F5:%v, F6:%v\n", f1, f2, f3, f4, f5, f6)
				}
			case "[]byte":
				var f1, f2, f3, f4, f5, f6 []byte
				err = row.Scan(&f1, &f2, &f3, &f4, &f5, &f6)
				if err != nil {
					fmt.Println(err)
				} else {
					fmt.Printf("F1:%v, F2:%v, F3:%v, F4:%v, F5:%v, F6:%v\n", f1, f2, f3, f4, f5, f6)
				}
			case "sql.RawBytes":
				var f1, f2, f3, f4, f5, f6 sql.RawBytes
				err = row.Scan(&f1, &f2, &f3, &f4, &f5, &f6)
				if err != nil {
					fmt.Println(err)
				} else {
					fmt.Printf("F1:%v, F2:%v, F3:%v, F4:%v, F5:%v, F6:%v\n", f1, f2, f3, f4, f5, f6)
				}
			case "others":
				var f1, f2, f3, f4, f5, f6 time.Time
				err = row.Scan(&f1, &f2, &f3, &f4, &f5, &f6)
				if err != nil {
					fmt.Println(err)
				} else {
					fmt.Printf("F1:%v, F2:%v, F3:%v, F4:%v, F5:%v, F6:%v\n", f1, f2, f3, f4, f5, f6)
				}
			}
		}

		if err = row.Err(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestMacBitTS(t *testing.T) {
	dsn := common.GetOGDsnBase()
	db, err:= sql.Open("opengauss", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	sqls := []string {
		"drop table if exists testMac",
		"create table testMac(f1 cidr, f2 inet, f3 macaddr)",
		"insert into testMac values('192.168/24', '192.168.1.5/8', '08:00:2b:01:02:03')",
		"insert into testMac values('192.1', '192.1.2.1', '08002b:010203')",
		"drop table if exists testBit",
		"create table testBit(f1 serial, f2 bit(3), f3 bit varying(5))",
		"insert into testBit values(default, B'101', B'1011')",
		"insert into testBit values(default, B'10'::bit(3), B'01')",
		"drop table if exists testTs",
		"create table testTs(f1 serial, f2 tsvector, f3 tsquery)",
		"insert into testTs values(default, 'a fat cat sat on a mat and ate a fat rat', 'fat & rat')",
	}

	for _, s := range sqls {
		_, err = db.Exec(s)
		if err != nil {
			t.Fatal(err)
		}
	}

	infos := []struct {
		descrip, datatype string
	} {
		{"----------result is golang string----------", "string"},
		{"\n----------result is golang interface{}----------", "interface{}"},
		{"\n----------result is golang []byte----------", "[]byte"},
		{"\n----------result is golang sql.RawBytes----------", "sql.RawBytes"},
		{"\n----------result is golang others----------", "others"},
	}

	queryInfos := []struct {
		typeDescrip, querySql string
	} {
		{"********************NetAddr Datatype********************", "select * from testMac"},
		{"\n********************BitStr Datatype********************", "select * from testBit"},
		{"\n********************TextSearch Datatype********************", "select * from testTs"},
	}

	row, err := db.Query("select 1")
	if err != nil {
		t.Fatal(err)
	}
	defer row.Close()
	for _, q := range queryInfos {
		fmt.Println(q.typeDescrip)

		for _, info := range infos {
			fmt.Println(info.descrip)

			row, err = db.Query(q.querySql)
			if err != nil {
				t.Fatal(err)
			}

			for row.Next() {
				switch info.datatype {
				case "string":
					var f1, f2, f3 string
					err = row.Scan(&f1, &f2, &f3)
					if err != nil {
						fmt.Println(err)
					} else {
						fmt.Printf("F1:%v, F2:%v, F3:%v\n", f1, f2, f3)
					}
				case "interface{}":
					var f1, f2, f3 interface{}
					err = row.Scan(&f1, &f2, &f3)
					if err != nil {
						fmt.Println(err)
					} else {
						fmt.Printf("F1:%v, F2:%v, F3:%v\n", f1, f2, f3)
					}
				case "[]byte":
					var f1, f2, f3 []byte
					err = row.Scan(&f1, &f2, &f3)
					if err != nil {
						fmt.Println(err)
					} else {
						fmt.Printf("F1:%v, F2:%v, F3:%v\n", f1, f2, f3)
					}
				case "sql.RawBytes":
					var f1, f2, f3 sql.RawBytes
					err = row.Scan(&f1, &f2, &f3)
					if err != nil {
						fmt.Println(err)
					} else {
						fmt.Printf("F1:%v, F2:%v, F3:%v\n", f1, f2, f3)
					}
				case "others":
					var f1, f2, f3 time.Time
					err = row.Scan(&f1, &f2, &f3)
					if err != nil {
						fmt.Println(err)
					} else {
						fmt.Printf("F1:%v, F2:%v, F3:%v\n", f1, f2, f3)
					}
				}
			}

			if err = row.Err(); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestRangeAndJson(t *testing.T) {
	dsn := common.GetOGDsnBase()
	db, err:= sql.Open("opengauss", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	sqls := []string {
		"drop table if exists testRange",
		"create table testRange(f1 int4range, f2 int8range, f3 numrange, f4 tsrange, f5 tstzrange, f6 daterange)",
		"insert into testRange values('[-1,10]', '[-2910,7738)', '(367.431, 731.431]', '(2010-01-01 14:30, 2010-01-04 15:30)', " +
			"'[2022-01-28 15:01:28.382264 GMT , 2022-01-30 15:01:28.382264 GMT]', '[2022-01-28, 2022-01-31]')",
		"drop table if exists testJson",
		"create table testJson(f1 json, f2 jsonb, f3 json, f4 jsonb, f5 json, f6 jsonb)",
		"insert into testJson values('   [1, \" a \", {\"a\"   :1    }]  ', '   [1, \" a \", {\"a\"   :1    }]  ', " +
			"'{\"a\" : 1, \"a\" : 2}', '{\"a\" : 1, \"a\" : 2}', " +
			"'{\"aa\" : 1, \"b\" : 2, \"a\" : 3}', '{\"aa\" : 1, \"b\" : 2, \"a\" : 3}')",
	}

	for _, s := range sqls {
		_, err = db.Exec(s)
		if err != nil {
			t.Fatal(err)
		}
	}

	infos := []struct {
		descrip, datatype string
	} {
		{"----------result is golang string----------", "string"},
		{"\n----------result is golang interface{}----------", "interface{}"},
		{"\n----------result is golang []byte----------", "[]byte"},
		{"\n----------result is golang sql.RawBytes----------", "sql.RawBytes"},
		{"\n----------result is golang others----------", "others"},
	}

	queryInfos := []struct {
		typeDescrip, querySql string
	} {
		{"********************Range Datatype********************", "select * from testRange"},
		{"\n********************Json Datatype********************", "select * from testJson"},
	}

	row, err := db.Query("select 1")
	if err != nil {
		t.Fatal(err)
	}
	defer row.Close()

	for _, q := range queryInfos {
		fmt.Println(q.typeDescrip)

		for _, info := range infos {
			fmt.Println(info.descrip)

			row, err = db.Query(q.querySql)
			if err != nil {
				t.Fatal(err)
			}

			for row.Next() {
				switch info.datatype {
				case "string":
					var f1, f2, f3, f4, f5, f6 string
					err = row.Scan(&f1, &f2, &f3, &f4, &f5, &f6)
					if err != nil {
						fmt.Println(err)
					} else {
						fmt.Printf("F1:%v, F2:%v, F3:%v, F4:%v, F5:%v, F6:%v\n", f1, f2, f3, f4, f5, f6)
					}
				case "interface{}":
					var f1, f2, f3, f4, f5, f6 interface{}
					err = row.Scan(&f1, &f2, &f3, &f4, &f5, &f6)
					if err != nil {
						fmt.Println(err)
					} else {
						fmt.Printf("F1:%v, F2:%v, F3:%v, F4:%v, F5:%v, F6:%v\n", f1, f2, f3, f4, f5, f6)
					}
				case "[]byte":
					var f1, f2, f3, f4, f5, f6 []byte
					err = row.Scan(&f1, &f2, &f3, &f4, &f5, &f6)
					if err != nil {
						fmt.Println(err)
					} else {
						fmt.Printf("F1:%v, F2:%v, F3:%v, F4:%v, F5:%v, F6:%v\n", f1, f2, f3, f4, f5, f6)
					}
				case "sql.RawBytes":
					var f1, f2, f3, f4, f5, f6 sql.RawBytes
					err = row.Scan(&f1, &f2, &f3, &f4, &f5, &f6)
					if err != nil {
						fmt.Println(err)
					} else {
						fmt.Printf("F1:%v, F2:%v, F3:%v, F4:%v, F5:%v, F6:%v\n", f1, f2, f3, f4, f5, f6)
					}
				case "others":
					var f1, f2, f3, f4, f5, f6 bool
					err = row.Scan(&f1, &f2, &f3, &f4, &f5, &f6)
					if err != nil {
						fmt.Println(err)
					} else {
						fmt.Printf("F1:%v, F2:%v, F3:%v, F4:%v, F5:%v, F6:%v\n", f1, f2, f3, f4, f5, f6)
					}
				}
			}

			if err = row.Err(); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestOthers(t *testing.T) {
	dsn := common.GetOGDsnBase()
	db, err:= sql.Open("opengauss", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	sqls := []string {
		"drop table if exists testOthers",
		"create table testOthers(f1 uuid, f2 hash16, f3 hash32, set hll)",
		"insert into testOthers values('A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11', 'e697da2eaa3a775b', '685847ed1fe38e18f6b0e2b18c00edee', hll_empty())",
	}

	for _, s := range sqls {
		_, err = db.Exec(s)
		if err != nil {
			t.Fatal(err)
		}
	}

	infos := []struct {
		descrip, datatype string
	} {
		{"----------result is golang string----------", "string"},
		{"\n----------result is golang interface{}----------", "interface{}"},
		{"\n----------result is golang []byte----------", "[]byte"},
		{"\n----------result is golang sql.RawBytes----------", "sql.RawBytes"},
		{"\n----------result is golang others----------", "others"},
	}

	fmt.Println("********************Other Datatype********************")

	row, err := db.Query("select 1")
	if err != nil {
		t.Fatal(err)
	}
	defer row.Close()

	for _, info := range infos {
		fmt.Println(info.descrip)

		row, err = db.Query("select * from testOthers")
		if err != nil {
			t.Fatal(err)
		}

		for row.Next() {
			switch info.datatype {
			case "string":
				var f1, f2, f3, f4 string
				err = row.Scan(&f1, &f2, &f3, &f4)
				if err != nil {
					fmt.Println(err)
				} else {
					fmt.Printf("F1:%v, F2:%v, F3:%v, F4:%v\n", f1, f2, f3, f4)
				}
			case "interface{}":
				var f1, f2, f3, f4 interface{}
				err = row.Scan(&f1, &f2, &f3, &f4)
				if err != nil {
					fmt.Println(err)
				} else {
					fmt.Printf("F1:%v, F2:%v, F3:%v, F4:%v\n", f1, f2, f3, f4)
				}
			case "[]byte":
				var f1, f2, f3, f4 []byte
				err = row.Scan(&f1, &f2, &f3, &f4)
				if err != nil {
					fmt.Println(err)
				} else {
					fmt.Printf("F1:%v, F2:%v, F3:%v, F4:%v\n", f1, f2, f3, f4)
				}
			case "sql.RawBytes":
				var f1, f2, f3, f4 sql.RawBytes
				err = row.Scan(&f1, &f2, &f3, &f4)
				if err != nil {
					fmt.Println(err)
				} else {
					fmt.Printf("F1:%v, F2:%v, F3:%v, F4:%v\n", f1, f2, f3, f4)
				}
			case "others":
				var f1, f2, f3, f4 bool
				err = row.Scan(&f1, &f2, &f3, &f4)
				if err != nil {
					fmt.Println(err)
				} else {
					fmt.Printf("F1:%v, F2:%v, F3:%v, F4:%v\n", f1, f2, f3, f4)
				}
			}
		}

		if err = row.Err(); err != nil {
			t.Fatal(err)
		}
	}
}