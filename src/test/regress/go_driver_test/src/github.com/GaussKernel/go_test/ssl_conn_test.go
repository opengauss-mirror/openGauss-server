package bench

import (
	"github.com/GaussKernel/common"
	"testing"
)

func TestSSL(t *testing.T){
	var dsn=common.GetOGDsnBase()
	sslmodes :=[]string{
		"disable",
		"prefer",
		"allow",
		"require",
		"verify-ca",
		//"verify-full",
	}
	for _,sslmode:= range sslmodes{
		fdsn :=dsn+" sslmode="+sslmode
		common.TestConnExpect("opengauss",fdsn,true)
	}
}
