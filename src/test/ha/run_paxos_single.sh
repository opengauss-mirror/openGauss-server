#!/bin/sh
# run all the test case of ha

#init some variables
loop_num=$1
if [ -z $1 ]; then
 loop_num=1
fi
count=0

source ./standby_env.sh
source ./deploy_paxos_single.sh
test -f regression.diffs.hacheck && rm regression.diffs.hacheck

total_starttime=`date +"%Y-%m-%d %H:%M:%S"`
total_startvalue=`date -d  "$total_starttime" +%s`

#init and start the database
printf "init and start the database\n"
dcf_port=15001
if [ ! -z $2 ]; then
	dcf_port=$2
fi

deploy_paxos ${dcf_port} 2>&1 | tee ./results/deploy_paxos_single.log

for((i=1;i<=$loop_num;i++))
do
	printf "run the paxos_schedule %d time\n" $i
	printf "%-50s%-10s%-10s\n" "testcase" "result" "time(s)"
	for line in `cat paxos_schedule_single | grep -v ^#`
	do
		printf "%-50s" $line
		starttime=`date +"%Y-%m-%d %H:%M:%S"`
		sh ./testcase/$line.sh ${dcf_port} > ./results/$line.log 2>&1
		count=`expr $count + 1`
		endtime=`date +"%Y-%m-%d %H:%M:%S"`
		starttime1=`date -d  "$starttime" +%s`
		endtime1=`date -d  "$endtime" +%s`
		interval=`expr $endtime1 - $starttime1`
		if [ $( grep "$failed_keyword" ./results/$line.log | grep -v "the database system is shutting down" | wc -l ) -eq 0 ]; then
			printf "%-10s%-10s\n" ".... ok" $interval
		else
			printf "%-10s%-10s\n" ".... FAILED" $interval
			cp ./results/$line.log regression.diffs.hacheck
			exit 1
		fi
	done
done

#stop the database
printf "stop the database\n"
stop_datanodes > ./results/stop_database_single.log 2>&1

total_endtime=`date +"%Y-%m-%d %H:%M:%S"`
total_endvalue=`date -d  "$total_endtime" +%s`
printf "all %d tests passed.\n" $count
printf "total time: %ss\n" $(($total_endvalue - $total_startvalue))
