package bench

import (
	"database/sql"
	"fmt"
	"github.com/GaussKernel/common"
	"math/rand"
	"testing"
	"time"
)

// 连接池大小
var MAX_POOL_SIZE = 20
var dbPoll chan *sql.DB

func putDB(db *sql.DB) {
	// 基于函数和接口间互不信任原则，这里再判断一次，养成这个好习惯哦
	if dbPoll == nil {
		dbPoll = make(chan *sql.DB, MAX_POOL_SIZE)
	}
	if len(dbPoll) >= MAX_POOL_SIZE {
		db.Close()
		return
	}
	dbPoll <- db
}

func initDB(t *testing.T) {
	// 缓冲机制，相当于消息队列
	if len(dbPoll) == 0 {
		// 如果长度为0，就定义一个redis.Conn类型长度为MAX_POOL_SIZE的channel
		dbPoll = make(chan *sql.DB, MAX_POOL_SIZE)
		go func() {
			for i := 0; i < MAX_POOL_SIZE / 2; i++ {
				dsn := common.GetOGDsnBase()
				db, err:= sql.Open("opengauss", dsn)
				if err != nil {
					t.Fatal(err)
				}
				putDB(db)
			}
		} ()
	}
}

func GetDB(t *testing.T)  *sql.DB {
	// 如果为空就初始化或者长度为零
	if dbPoll == nil || len(dbPoll) == 0 {
		initDB(t)
	}
	return <- dbPoll
}

func changeCount(num int, t *testing.T)  {
	db := GetDB(t)

	tx, err := db.Begin() // begin transaction
	defer tx.Commit() // commit transaction

	res,_ := tx.Exec("update testTxLock set f3 = f3 + 100")
	RowsAffected, err := res.RowsAffected()
	if err != nil {
		t.Fatal("res.RowsAffected==================Err")
	}
	if RowsAffected <= 0 {
		fmt.Println("抢购失败")
	}
}

func TestConcurrency(t *testing.T) {
	dsn := common.GetOGDsnBase()
	db, err:= sql.Open("opengauss", dsn)
	if err != nil {
		t.Fatal(err)
	}

	sqls := []string {
		"drop table if exists testTxLock",
		"create table testTxLock(f1 int, f2 varchar(20), f3 number, f4 timestamptz, f5 boolean)",
		"insert into testTxLock values(1, '华为', 123.3, '2022-02-08 10:30:43.31 +08', true)",
	}

	for i := 0; i <= 2; i++ {
		_, err = db.Exec(sqls[i])
		if err != nil {
			t.Fatal(err)
		}
	}
	err = db.Close()
	if err != nil {
		t.Fatal(err)
	}

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < 10; i++  {
		go changeCount(r.Intn(50), t)
		go changeCount(r.Intn(50), t)
		go changeCount(r.Intn(50), t)
		go changeCount(r.Intn(50), t)
	}
	time.Sleep(3 * time.Second)
}