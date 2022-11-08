#!/bin/sh
# run all the test case of ha 
CUR_DIR=$(cd $(dirname "${BASH_SOURCE[0]}") && pwd)
echo "CUR_DIR : $CUR_DIR"

#init some variables
loop_num=$1
if [ -z $1 ]; then
 loop_num=1
fi
count=0

source ./../ss/ss_database_build_env.sh

test -f regression.diffs.hacheck && rm regression.diffs.hacheck

RESULT_DIR=$CUR_DIR/../ha/results/dms_reform
total_starttime=`date +"%Y-%m-%d %H:%M:%S"`
total_startvalue=`date -d  "$total_starttime" +%s`

if [ -d ${RESULT_DIR} ]; then
        echo "${RESULT_DIR} exists, so need to clean and recreate"
        rm -rf ${RESULT_DIR}
    else
        echo "${RESULT_DIR} not exists, so need to recreate"
    fi
    mkdir ${RESULT_DIR}

#init and start the database
printf "init and start the database in shared storage based on dms and dss\n"
sh ./../ss/deploy_two_inst_ss.sh > ./results/deploy_two_inst_ss.log 2>&1

for((i=1;i<=$loop_num;i++))
do
	printf "run the ha_schedule %d time\n" $i
	printf "%-50s%-10s%-10s\n" "testcase" "result" "time(s)"
	for line in `cat ha_schedule_single_ss$2 | grep -v ^#`
	do
		printf "%-50s" $line
		starttime=`date +"%Y-%m-%d %H:%M:%S"` 
		sh ./testcase/$line.sh > ./results/$line.log 2>&1
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
sleep 5
stop_gaussdb ${SS_DATA}/dn0
stop_gaussdb ${SS_DATA}/dn1 
sleep 3

total_endtime=`date +"%Y-%m-%d %H:%M:%S"`
total_endvalue=`date -d  "$total_endtime" +%s`
printf "all %d tests passed.\n" $count
printf "total time: %ss\n" $(($total_endvalue - $total_startvalue))
