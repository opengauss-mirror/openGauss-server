package bench

import (
	"github.com/GaussKernel/common"
	"log"
	"regexp"
	"strings"
	"testing"
)

func TestTempTablet(t *testing.T){
	db :=common.GetDefaultOGDB()
	defer db.Close()
	common.CheckErr(db.Exec("drop table if exists temp"))
	txn, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	_, err = txn.Exec("CREATE TEMP TABLE temp (a int, b varchar)")
	if err != nil {
		t.Fatal(err)
	}

	stmt, err := txn.Prepare("insert into temp values(:1,:2)")
	common.CheckErr(err)

	var repeatCount =100
	var insertCount=10
	longString := strings.Repeat("#", repeatCount)
	for i := 0; i < insertCount; i++ {
		_, err := stmt.Exec(int64(i), longString)
		if err != nil {
			t.Fatal(err)
		}
	}

	row,err := txn.Query("SELECT COUNT(*) FROM temp")
	common.CheckErr(err)
	common.IterScanRows(row)

	err = stmt.Close()
	common.CheckErr(err)

	row,err = txn.Query("SELECT COUNT(*) FROM temp")
	if err != nil {
		t.Fatal(err)
	}
	common.IterScanRows(row)

	common.CheckErr(txn.Rollback())
	_,err = db.Query("SELECT COUNT(*) FROM temp")
	f,err :=regexp.MatchString("relation \"temp\" does not exist on datanode", err.Error())
	common.CheckErr(err)
	if !f {
		log.Fatal("message has changed")
	}
	db.Close()
}
