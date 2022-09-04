package pg_libpq_test

import (
    "database/sql"
    "fmt"
    . "github.com/lib/pg_libpq_test/pgcom"
    "log"
    "math/rand"
    "strconv"
    "strings"
    "sync"
    "testing"
)

// 为啥PG不支持绑定参数
func insertTab2(db *sql.DB, thread_id,id_left, insert_rows int,wg *sync.WaitGroup){
    log.Printf("thread %v is begining...",thread_id)
    tx,err :=db.Begin()
    CheckErr(err)
    stmt,err :=tx.Prepare("insert into people values(:1,:2,:3)")
    CheckErr(err)
    for i:=0;i<insert_rows;i++ {
        insert_id:=id_left+rand.Int()%10000
        name:= GetRandomString(30)
        info:=strings.Repeat(name,100 )
        stmt.Exec(insert_id,name,info)
    }
    tx.Commit()
    stmt.Close()
    log.Println("id_left=%u is finish.",id_left)
    wg.Done()
}
func insertTab(db *sql.DB, thread_id,id_left, insert_rows int,wg *sync.WaitGroup){
    log.Printf("thread %v is begining...",thread_id)

    for i:=0;i<insert_rows;i++ {
        insert_id:=id_left+rand.Int()%10000
        name:= GetRandomString(30)
        info:=strings.Repeat(name,100 )
        sql :="insert into people values("+strconv.Itoa(insert_id)+",'"+name+"','"+info+"')"
        CheckErr(db.Exec(sql))
    }
    log.Println("id_left=%u is finish.",id_left)
    wg.Done()
}


// 实现五百个线程并发插入表people,插入10000行
func TestBigCreate(t *testing.T){
    fmt.Println("TestBigCreate begin.")
    //db,err:=sql.Open("postgres","host=10.244.19.211 port=6432 user=gauss password=Gauss_234 dbname=gauss sslmode=disable")
    db,err:=sql.Open("postgres","host=7.189.51.79 port=5431 user=gauss password  =Gauss_234 dbname=gauss sslmode=disable")

    db.SetMaxOpenConns(500)
    CheckErr(err)
    CheckErr(db.Exec("drop table if exists people"))
    CheckErr(db.Exec("create table people(id int,name varchar(800),info varchar(8000))"))

    threads:=1
    wg :=&sync.WaitGroup{}
    id_begin,id_step,insert_rows:=0,100000,1000
    for i:=0;i<threads;i++{
        wg.Add(1)
        go insertTab(db,i,id_begin,insert_rows,wg)
        id_begin+=id_step
    }
    wg.Wait()
    db.Close()
}
