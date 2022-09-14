package common

import (
	"fmt"
	"log"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
)

func init(){
	log.SetPrefix("TRACE: ")
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Llongfile)
}
func itoa(buf *[]byte, i int, wid int) {
	// Assemble decimal in reverse order.
	var b [20]byte
	bp := len(b) - 1
	for i >= 10 || wid > 1 {
		wid--
		q := i / 10
		b[bp] = byte('0' + i - q*10)
		bp--
		i = q
	}
	// i < 10
	b[bp] = byte('0' + i)
	*buf = append(*buf, b[bp:]...)
}

func formatHeader(buf []byte, t time.Time, file string, line int) []byte{
	t = t.UTC()
	year, month, day := t.Date()
	itoa(&buf, year, 4)
	buf = append(buf, '/')
	itoa(&buf, int(month), 2)
	buf = append(buf, '/')
	itoa(&buf, day, 2)
	buf = append(buf, ' ')
	hour, min, sec := t.Clock()
	itoa(&buf, hour, 2)
	buf = append(buf, ':')
	itoa(&buf, min, 2)
	buf = append(buf, ':')
	itoa(&buf, sec, 2)
	buf = append(buf, '.')
	itoa(&buf, t.Nanosecond()/1e3, 6)
	buf = append(buf, ' ')

	short := file
	for i := len(file) - 1; i > 0; i-- {
		if file[i] == '/' {
			short = file[i+1:]
			break
		}
	}
	file = short
	buf = append(buf, file...)
	buf = append(buf, ':')
	itoa(&buf, line, -1)
	return append(buf, ": "...)
}

func CheckErr(res ...interface{}){
	for _,oriErr :=range res{
		if oriErr == nil{
			continue
		}
		switch oriErr.(type) {
			case error:
				err :=oriErr.(error)
				s :=fmt.Sprintln(err)
				_,file,line,ok:=runtime.Caller(1)
				if ok {
					info :="[CheckErr] "
					buf :=make([]byte,0,1024)
					buf=append(buf,info...)
					buf = formatHeader(buf,time.Now(),file,line)
					buf = append(buf, s...)
					os.Stdout.Write(buf)
				}else{
					fmt.Printf("%s %s",time.Now(),s)
				}
				os.Exit(2)
			default:
		}
	}
}


func createNewFile(fileName string){
	err := os.Remove(fileName)
	if err!=nil{
		log.Println(err)
	}
	fd,err:=os.Create(fileName)
	if err!=nil{
		log.Fatal(err)
	}
	fd.Close()
}

func GetGoid() int64 {
	var (
		buf [64]byte
		n   = runtime.Stack(buf[:], false)
		stk = strings.TrimPrefix(string(buf[:n]), "goroutine ")
	)

	idField := strings.Fields(stk)[0]
	id, err := strconv.Atoi(idField)
	if err != nil {
		panic(fmt.Errorf("can not get goroutine id: %v", err))
	}

	return int64(id)
}

func PrintType(a interface{}){
	switch a.(type) {
	case int:
		fmt.Println("type is int")
	case string:
		fmt.Println("type is string")
	case float32:
		fmt.Println("type is float32")
	case float64:
		fmt.Println("type is float64")
	default:
		fmt.Println("type is unknown type")
	}
}
