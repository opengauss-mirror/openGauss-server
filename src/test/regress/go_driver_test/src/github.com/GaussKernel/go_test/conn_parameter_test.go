package bench

import (
	"database/sql"
	"fmt"
	"github.com/GaussKernel/common"
	"log"
	"math"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

func TestWrongPort(t *testing.T){
	var dsn="user=gauss dbname=gauss"
	ports:=[]int{0,math.MaxUint16+1}
	expectWrongMsg :="invalid port"
	reg :=regexp.MustCompile(expectWrongMsg)
	for _,port:=range ports{
		ndsn :=dsn+" port="+strconv.Itoa(port)
		_,err :=sql.Open("opengauss",ndsn)
		if err!=nil{
			errMsg :=err.Error()
			if !reg.MatchString(errMsg){
				log.Fatal("wrong information change")
			}
		} else{
			log.Fatal("expect wrong")
		}

	}
}

func TestConnApplicationName(t *testing.T){
	dsn :=common.GetOGDsnBase()
	db,err :=sql.Open("opengauss",dsn)
	if err!=nil{
		log.Fatal("conn err,",err)
	}
	var appName string
	err =db.QueryRow("select application_name from pg_stat_activity where pid=pg_backend_pid()").Scan(&appName)
	if err!=nil{
		log.Fatal(err)
	}
	if(appName != "go-driver"){  //default name
		log.Fatal("test application name fail")
	}
	db.Close()
	testAppName:="test_application_Name"
	dsn +=" application_name="+testAppName
	db,err =sql.Open("opengauss",dsn)
	if err!=nil{
		log.Fatal("conn err,",err)
	}
	err =db.QueryRow("select application_name from pg_stat_activity where pid=pg_backend_pid()").Scan(&appName)
	if err!=nil{
		log.Fatal(err)
	}
	if appName != testAppName {
		log.Fatal("test application name fail")
	}
}

func TestTargetSessionAttr(t *testing.T){
	attrs :=[]struct{
		attr string
		expect bool
	}{
		{"any", true},
		{"read-write", true},
		{"read-only", false},
		{"master",false},
		{"slave",false},
		{"preferSlave",false},
	}
	dsn :=common.GetOGDsnBase()
	for _,attr :=range attrs{
		ndsn :=dsn+" target_session_attrs=" + attr.attr
		common.TestConnExpect("opengauss",ndsn, attr.expect)
	}
}

func TestBinaryParameters(t *testing.T){
	dsn :=common.GetOGDsnBase()
	baseDB,err:=sql.Open("opengauss",dsn)
	if err!=nil{
		log.Fatal(err)
	}
	defer baseDB.Close()
	common.TruncateTable(baseDB,"people")

	binParams:=[]string{
		"yes",
		"no",
	}
	disablePreparedBinaryResult:=[]string{
		"yes",
		"no",
	}

	for _,v1:=range binParams{
		for _,v2:=range disablePreparedBinaryResult{
			ndsn:=dsn+" binary_parameters="+v1+" disable_prepared_binary_result="+v2
			db,err :=sql.Open("opengauss",ndsn)
			if err!=nil{
				log.Fatal("["+ndsn+"] error,",err)
			}
			stmt,err :=db.Prepare("insert into people values(:id,:name)")
			if err!=nil{
				log.Fatal(err)
			}
			_,err = stmt.Exec(sql.Named("id",10000),sql.Named("name","test"))
			if err!=nil{
				log.Fatal(err)
			}
			stmt.Close()
			db.Close()
		}
	}
	var rows int
	baseDB.QueryRow("select count(*) from people where id=10000 and name='test'").Scan(&rows)
	if rows != 4{
		log.Fatal("query res error")
	}
}

func TestErasePassword(t *testing.T){
	pws :=[]string{
		//"secretpw",
		//"secretpw@",
		//"@secretpw",
		//"@secretpw@",
		"secretpw\t",
		"secretpw\n",
		"secretpw\v",
		"secretpw\f",
		"secretpw,",
	}
	errorMode :=[]string{
		"xxx",
		"abc=",
		"abc",
		"xxx\\",
		"binary_parameters",
		"binary_parameters=",
		"binary_parameters=notval",
	}
	port:=common.GetPropertyPort()
	dbname:=common.GetPropertyDbName()
	host :=common.GetPropertyHost()
	user:="notexist"
	prefixDsn:="user="+user+" dbname="+dbname+" port="+port+" host="+host
	prefixUrl:="opengauss://"+user+":"
	suffixUrl:="@"+common.GetPropertyHost()+":"+common.GetPropertyPort()+"/"+common.GetPropertyDbName()+"?sslmode=disable&"
	regPw := regexp.MustCompile("secretpw")
	for _,emode :=range errorMode{
		for _,pw:=range pws{
			ndsn:=prefixDsn+emode+" password="+pw
			nurl :=prefixUrl+pw+suffixUrl+emode
			openinfo :=[]string{
				ndsn,
				nurl,
			}
			for _,info :=range openinfo{
				db,err:=sql.Open("opengauss",info)
				if err!=nil{
					s:=err.Error()
					if regPw.MatchString(s){
						log.Fatal("Error, password erase fail, dataSourceName="+info)
					}
					continue
				}
				stmt,err :=db.Prepare("select 1")
				if err!=nil{
					s:=err.Error()
					if regPw.MatchString(s){
						log.Fatal("Error, password erase fail, dataSourceName="+info)
					}
					continue
				}
				stmt.Close()
				db.Close()
			}
		}
	}
}

func TestErasePassword2(t *testing.T) {
	sqlconn0 := "opengauss://test:Gauss_234@10.244.49.15:17777/godb?loggerLevel=debug1&password=Gauss_234&password= Gauss_234&password =Gauss_234&password = Gauss_234"
	sqlconn00 := "opengauss://test:Gauss_234@10.244.49.15:17777/godb?loggerLevel=debug1&password='Gauss_234'&password= 'Gauss_234'&password ='Gauss_234'&password = 'Gauss_234'"
	sqlconn1 := "opengauss://test:Gauss_234@10.244.49.15:17777/godb?loggerLevel=debug1&password=Gauss_234&sslpassword=Gauss_234&sslpassword= Gauss_234&sslpassword =Gauss_234&&sslpassword = Gauss_234"
	sqlconn2 := "opengauss://test:Gauss_234@10.244.49.15:17777/godb?loggerLevel=debug1&password=Gauss_234&sslpassword='Gauss_234'&sslpassword= 'Gauss_234'&sslpassword ='Gauss_234'&&sslpassword = 'Gauss_234'"
	sqlconns :=[]string{sqlconn00,sqlconn0, sqlconn1,sqlconn2}
	for _,conn :=range sqlconns{
		_, err := sql.Open("opengauss", conn)
		if err == nil {
			log.Fatal("expect error")
		}
		errStr := err.Error()
		f,err := regexp.MatchString("Gauss_234", errStr)
		if err!=nil{
			log.Fatal(err)
		}
		if f {
			log.Fatal("erase password failed. ", errStr)
		}
	}
}

func TestConnParameter(t *testing.T) {
	fmt.Println("********************No Runtime Parameter********************")
	dsnStr := common.GetOGDsnBase()

	parameters := []string {
		" connect_timeout=5",
		" connect_timeout=-1",
		" connect_timeout=0",
		" connect_timeout=22.33",
		" disable_prepared_binary_result=no",
		" disable_prepared_binary_result=yes",
		" disable_prepared_binary_result=NO",
		" disable_prepared_binary_result=YES",
		" disable_prepared_binary_result=n",
		" disable_prepared_binary_result=y",
		" disable_prepared_binary_result=N",
		" disable_prepared_binary_result=Y",
		" binary_parameters=yes",
		" binary_parameters=YES",
		" binary_parameters=NO",
		" binary_parameters=no",
		" binary_parameters=y",
		" binary_parameters=Y",
		" binary_parameters=n",
		" binary_parameters=N",
	}

	for _, param := range parameters {
		db, err:= sql.Open("opengauss", dsnStr+param)
		if err != nil {
			arr := strings.Split(fmt.Sprintf("%v", err),":")
			fmt.Printf("FAILED:%v, parse error:%v\n", param, arr[len(arr)-1])
		} else {
			fmt.Printf("PARSE SUCCESS:%v\n", param)
			db.Close()
		}
	}
}

func TestConnRuntimeParameter(t *testing.T) {
	fmt.Println("********************Runtime Parameter********************")
	dsnStr := common.GetOGDsnBase()

	infos := []struct {
		parameter, value string
	} {
		{" application_name", "go-test-api"},
		{" application_name", "default"},
		{ " application_name", "123456"},
		{" datestyle", "SQL,YMD"},
		{" datestyle", "default"},
		{" datestyle", "SQL, YMD"},
		{" timezone", "GMT"},
		{" timezone", "ABC"},
		{" timezone", "+8"},
		{" timezone", "-05:00"},
		{" timezone", "+22"},
		{" timezone", "24"},
		{" timezone", "-23"},
		{" timezone", "25"},
		{" timezone", "-24"},
		{" timezone", "1234"},
		{" client_encoding", "SQL_ASCII"},
		{" client_encoding", "GBK"},
		{" client_encoding", "UTF8"},
		{" client_encoding", "ABCD"},
		{" client_encoding", "default"},
		{" client_encoding", "0"},
		{" search_path", "pqgotest"},
		{" search_path", "12345"},
		{" search_path", "default"},
		{" extra_float_digits", "0"},
		{" extra_float_digits", "-15"},
		{" extra_float_digits", "3"},
		{" extra_float_digits", "4"},
		{" extra_float_digits", "-16"},
		{" extra_float_digits", "3.4"},
	}

	for _, info := range infos {
		dsn := dsnStr + info.parameter + "=" + info.value
		db, err:= sql.Open("opengauss", dsn)
		if err != nil {
			arr := strings.Split(fmt.Sprintf("%v", err),":")
			fmt.Printf("FAILED:%v, parse error:%v\n", info.parameter + "=" + info.value, arr[len(arr)-1])
			continue
		}


		var f1 string
		err = db.QueryRow("show " + info.parameter).Scan(&f1)
		if err != nil {
			arr := strings.Split(fmt.Sprintf("%v", err),":")
			fmt.Printf("FAILED:%v, parse error:%v\n", info.parameter + "=" + info.value, arr[len(arr)-1])
		} else {
			fmt.Printf("RESULT: show%s: %s\n", info.parameter, f1)
		}

		db.Close()
	}
}
