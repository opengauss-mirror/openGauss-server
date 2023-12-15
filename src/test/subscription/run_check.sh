#!/bin/sh

count=0
source $1/env_utils.sh $1 $2 $3

#clean temporary files generated after last check 
echo "removing $g_data_path"
if [ -d $g_data_path ]; then
	rm -rf $g_data_path
fi

echo "removing $1/results"
if [ -d "$1/results" ]; then
	rm -rf $1/results
fi

mkdir $1/results

total_starttime=`date +"%Y-%m-%d %H:%M:%S"`
total_startvalue=`date -d "$total_starttime" +%s`

#init and start the database
printf "init and start the database\n"
node_num=3
python2 $scripts_dir/pubsub.py -d $node_num > $1/results/deploy_cluster.log 2>&1

printf "%-50s%-10s%-10s\n" "testcase" "result" "time(s)"
for line in `cat $1/schedule | grep -v ^#`
do
	printf "%-50s" $line
	starttime=`date +"%Y-%m-%d %H:%M:%S"` 
	sh $1/testcase/$line.sh $1 $2 $3 > $1/results/$line.log 2>&1
	count=`expr $count + 1`
	endtime=`date +"%Y-%m-%d %H:%M:%S"` 
	starttime1=`date -d "$starttime" +%s`
	endtime1=`date -d "$endtime" +%s`
	interval=`expr $endtime1 - $starttime1`
	if [ $( grep "$failed_keyword" $1/results/$line.log | wc -l ) -eq 0 ]; then
		printf "%-10s%-10s\n" ".... ok" $interval
	else
		printf "%-10s%-10s\n" ".... FAILED" $interval
		break
	fi
done

printf "stop the database\n"
python2 $scripts_dir/pubsub.py -o -d $node_num > $1/results/stop_cluster.log 2>&1

total_endtime=`date +"%Y-%m-%d %H:%M:%S"`
total_endvalue=`date -d "$total_endtime" +%s`
printf "all %d tests passed.\n" $count
printf "total time: %ss\n" $(($total_endvalue - $total_startvalue))
