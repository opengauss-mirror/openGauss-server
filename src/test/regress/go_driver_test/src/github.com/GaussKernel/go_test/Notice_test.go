package bench

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	pq "gitee.com/opengauss/openGauss-connector-go-pq"
	. "github.com/GaussKernel/common"
	"log"
	"testing"
)

func TestConnectorWithNoticeHandler_Simple(t *testing.T) {
	db,err :=pq.NewConnector(GetOGDsnBase())
	if err!=nil{
		log.Fatal(err)
	}
	TestConnExpect("opengauss",GetOGDsnBase(),true)
	var notice *pq.Error
	// Make connector w/ handler to set the local var
	c := pq.ConnectorWithNoticeHandler(db, func(n *pq.Error) {
		notice = n
	})
	raiseNotice(c, t, "Test notice #1")
	if notice == nil || notice.Message != "Test notice #1" {
		t.Fatalf("Expected notice w/ message, got %v", notice)
	}
	// Unset the handler on the same connector
	prevC := c
	if c = pq.ConnectorWithNoticeHandler(c, nil); c != prevC {
		t.Fatalf("Expected to not create new connector but did")
	}
	raiseNotice(c, t, "Test notice #2")
	if notice == nil || notice.Message != "Test notice #1" {
		t.Fatalf("Expected notice to not change, got %v", notice)
	}
	// Set it back on the same connector
	if c = pq.ConnectorWithNoticeHandler(c, func(n *pq.Error) { notice = n }); c != prevC {
		t.Fatal("Expected to not create new connector but did")
	}
	raiseNotice(c, t, "Test notice #3")
	if notice == nil || notice.Message != "Test notice #3" {
		t.Fatalf("Expected notice w/ message, got %v", notice)
	}
}

func raiseNotice(c driver.Connector, t *testing.T, escapedNotice string) {
	db := sql.OpenDB(c)
	defer db.Close()
	sql := "DO language plpgsql $$ BEGIN RAISE NOTICE '" + escapedNotice + "'; END $$"
	if _, err := db.Exec(sql); err != nil {
		fmt.Println(err)
		t.Fatal(err)
	}
}
