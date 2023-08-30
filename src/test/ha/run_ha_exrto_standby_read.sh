#!/bin/bash
# Copyright (c) Huawei Technologies Co., Ltd. 2010-2018. All rights reserved.
# run all the test case of ha

#init some variables
loop_num=$1
if [ -z $1 ]; then
 loop_num=1
fi
count=0

source ./standby_env.sh

total_starttime=`date +"%Y-%m-%d %H:%M:%S"`
total_startvalue=`date -d  "$total_starttime" +%s`

array=("exrtostandbyread")
for element in ${array[@]}
do
  mkdir -vp ./results/$element
done

#init and start the database
if [ "$#" != '2' ]; then
    printf "init and start the database\n"
    sh deploy_multi_single.sh > ./results/deploy_standby_multi_single.log 2>&1
fi

for((i=1;i<=$loop_num;i++))
do
	printf "run the ha_exrto_standby_read %d time\n" $i
	printf "%-50s%-10s%-10s\n" "testcase" "result" "time(s)"
	for line in `cat ha_exrto_standby_read | grep -v ^#`
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
			# exit 0
		fi
	done
done

#stop the database
printf "stop the database\n"
python $scripts_dir/pgxc_multi.py -o > ./results/stop_database_multi.log 2>&1

total_endtime=`date +"%Y-%m-%d %H:%M:%S"`
total_endvalue=`date -d  "$total_endtime" +%s`
printf "all %d tests passed.\n" $count
printf "total time: %ss\n" $(($total_endvalue - $total_startvalue))
