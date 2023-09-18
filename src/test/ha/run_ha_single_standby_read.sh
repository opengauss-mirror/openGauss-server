#!/bin/sh
# run the test case of standby-read

#init some variables
count=0
loop_num=$1
if [ -z $1 ]; then
 loop_num=1
fi

source ./standby_env.sh

total_starttime=`date +"%Y-%m-%d %H:%M:%S"`
total_startvalue=`date -d  "$total_starttime" +%s`

#init and start the database
printf "init and start the database\n"
sh deploy_multi_single.sh > ./results/deploy_standby_multi_single.log 2>&1

for((i=1;i<=$loop_num;i++))
do
	printf "run the ha_schedule %d time\n" $i
	printf "%-50s%-10s%-10s\n" "testcase" "result" "time(s)"
	for line in `cat ha_schedule_single_standby_read$2 | grep -v ^#`
	do
		printf "%-50s" $line
		starttime=`date +"%Y-%m-%d %H:%M:%S"`
		starttime1=`date -d  "$starttime" +%s`
		sh ./testcase/$line.sh > ./results/$line.log 2>&1
		endtime=`date +"%Y-%m-%d %H:%M:%S"` 
		endtime1=`date -d  "$endtime" +%s`
		interval=`expr $endtime1 - $starttime1`
		if [ $( grep "$failed_keyword" ./results/$line.log | grep -v "the database system is shutting down" | wc -l ) -eq 0 ]; then
			printf "%-10s%-10s\n" ".... ok" $interval
			count=`expr $count + 1`
		else
			printf "%-10s%-10s\n" ".... FAILED" $interval
			echo "hacheck mistakes info in " ./results/$line.log
		fi
	done
done

#stop the database
printf "stop the database\n"
python $scripts_dir/pgxc_psd_single.py -o > ./results/stop_database_single.log 2>&1

total_endtime=`date +"%Y-%m-%d %H:%M:%S"`
total_endvalue=`date -d  "$total_endtime" +%s`
printf "all %d tests passed.\n" $count
printf "total time: %ss\n" $(($total_endvalue - $total_startvalue))
