package bench

import (
	"context"
	. "github.com/GaussKernel/common"
	"log"
	"regexp"
	"testing"
	"time"
)

func TestContextCancelExec(t *testing.T) {
	db := GetDefaultOGDB()
	defer db.Close()

	ctx, cancel := context.WithCancel(context.Background())

	// Delay execution for just a bit until db.ExecContext has begun.
	defer time.AfterFunc(time.Millisecond*10, cancel).Stop()
	// Not canceled until after the exec has started.
	if _, err := db.ExecContext(ctx, "select pg_sleep(1)"); err == nil {
		log.Fatal("expected error")
	} else {
		errStr := err.Error()
		//reg := regexp.Compile("cancel")
		f,err := regexp.MatchString("cancel", errStr)
		CheckErr(err)
		if !f{
			log.Fatalf("unexpected error: %s", err)
		}
	}

	// Context is already canceled, so error should come before execution.
	if _, err := db.ExecContext(ctx, "select pg_sleep(1)"); err == nil {
		log.Fatal("expected error")
	} else if err.Error() != "context canceled" {
		log.Fatalf("unexpected error: %s", err)
	}

	for i := 0; i < 100; i++ {
		func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if _, err := db.ExecContext(ctx, "select 1"); err != nil {
				log.Fatal(err)
			}
		}()

		if _, err := db.Exec("select 1"); err != nil {
			log.Fatal(err)
		}
	}
}
