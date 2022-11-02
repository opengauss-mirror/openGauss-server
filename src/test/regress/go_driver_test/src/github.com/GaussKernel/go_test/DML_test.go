package bench

import (
	"database/sql"
	"github.com/GaussKernel/common"
	"log"
	"testing"
)

func TestDML(t *testing.T){
	insertRows :=100
	pool:=common.GetDefaultOGDB()
	common.CheckErr(pool.Exec("drop table if exists testdml"))
	common.CheckErr(pool.Exec("create table testdml(id int,name varchar(8000))"))
	defer pool.Close()
	st,err:=pool.Prepare("insert into testdml(id,name) values(:id,:name)")
	if err!=nil{
		log.Fatal("insert error",err)
	}
	defer st.Close()
	for i:=0;i<insertRows;i++{
		tname:=common.GetRandomString(20)
		st.Exec(sql.Named("id",i),sql.Named("name",tname))
	}
	rows := common.GetTableRowCounts(pool,"testdml")
	if rows!=insertRows{
		log.Fatal("insert error")
	}
	common.CheckErr(pool.Exec("drop table if exists testdml"))
}
