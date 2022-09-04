package bench

import (
	"context"
	"database/sql"
	"flag"
	. "github.com/GaussKernel/common"
	"log"
	"os"
	"os/signal"
	"testing"
	"time"
)

func TestPing(t *testing.T) {
	id := flag.Int64("id", 1, "person ID to find")
	flag.Parse()

	pool :=GetDefaultOGDB()
	defer pool.Close()
	CheckErr(pool.Exec("drop table if exists testping"))
	CheckErr(pool.Exec("create table testping(id int,name varchar(8000))"))
	CheckErr(pool.Exec("insert into testping values(:id,'abc')",*id))
	ctx, stop := context.WithCancel(context.Background())
	defer stop()

	appSignal := make(chan os.Signal, 3)
	signal.Notify(appSignal, os.Interrupt)

	go func() {
		select {
		case <-appSignal:
			stop()
		}
	}()
	Ping(pool,ctx)
	Query(pool, ctx, *id)
	row,err := pool.Query("select * from testping")
	CheckErr(err)
	IterScanRows(row)
	CheckErr(pool.Exec("drop table testping"))
}

func Ping(pool *sql.DB,ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()

	if err := pool.PingContext(ctx); err != nil {
		log.Fatalf("unable to connect to database: %v", err)
	}
}

func Query(pool *sql.DB, ctx context.Context, id int64) {
	ctx, cancel := context.WithTimeout(ctx, 5000*time.Second)
	defer cancel()

	row,err := pool.QueryContext(ctx, "select p.name from testping as p where p.id = :id", sql.Named("id", id))
	CheckErr(err)
	IterScanRows(row)
}
