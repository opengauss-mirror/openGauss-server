package bench

import (
	//"fmt"
	"github.com/GaussKernel/common"
	"github.com/go-xorm/xorm"
	"log"
	"testing"
	"time"
)

func TestXorm(t *testing.T){
	engine, err := xorm.NewEngine("postgres", common.GetOGDsnBase())
	if err != nil{
		log.Fatal(err)
	}
	//engine.ShowSQL(true)                     //则会在控制台打印出生成的SQL语句；
	engine.SetSchema("gauss")

	if err = engine.DropTables("user"); err!=nil{
		log.Fatal(err)
	}
	if f,err:=engine.IsTableExist("user");err !=nil || f{
		log.Fatal("this table should not exists")
	}
	type User struct {
		Id int64
		Name string
		Salt string
		Age int
		Passwd string `xorm:"varchar(200)"`
		Created time.Time `xorm:"created"`
		Updated time.Time `xorm:"updated"`
	}

	err = engine.Sync2(new(User))
	common.CheckErr(err)
	time.Sleep(time.Millisecond*1000)

	if f,err:=engine.IsTableExist("user");err !=nil || !f{
		log.Fatal("this table should exists",err,f)
	}
	db:=common.GetDefaultOGDB()
	common.PrintTableMeta(db,"user","gauss")

	if err = engine.DropTables("user"); err!=nil{
		log.Fatal(err)
	}
}

