#!/bin/sh
# run all the test case for shared storage version

#init some variables
loop_num=$1
if [ -z $1 ]; then
 loop_num=2
fi
count=0
node_num=3

source ./standby_env.sh
test -f regression.diffs.hacheck && rm regression.diffs.hacheck

total_starttime=`date +"%Y-%m-%d %H:%M:%S"`
total_startvalue=`date -d  "$total_starttime" +%s`

array=("multi_standby_single_shared_storage")
for element in ${array[@]}
do
  mkdir -vp ./results/$element
done

#init and start the database
printf "init and start the database\n"
sh deploy_multi_single_shared_storage.sh > ./results/deploy_multi_single_shared_storage.log 2>&1

for((i=1;i<=$loop_num;i++))
do
	printf "run the ha_schedule %d time\n" $i
	printf "%-50s%-10s%-10s\n" "testcase" "result" "time(s)"
        
        # open extreme_rto
        if [ $i -eq 2 ]; then
               printf "open extreme_rto\n"
               gs_guc set -Z datanode -D $primary_data_dir -c "recovery_parse_workers = 3"
               gs_guc set -Z datanode -D $primary_data_dir -c "recovery_redo_workers = 3"
	       gs_guc set -Z datanode -D $primary_data_dir -c "hot_standby = off"
               for((j=1;j<=$node_num;j++))
               do
                   datanode_dir=$data_dir/datanode$j
                   datanode_dir=$datanode_dir"_standby"
                   echo $datanode_dir
                   gs_guc set -Z datanode -D $datanode_dir -c "recovery_parse_workers = 3"
                   gs_guc set -Z datanode -D $datanode_dir -c "recovery_redo_workers = 3"
		   gs_guc set -Z datanode -D $datanode_dir -c "hot_standby = off"
               done
        fi

	for line in `cat ha_schedule_multi_single_shared_storage$2 | grep -v ^#`
	do
		printf "%-50s" $line
		starttime=`date +"%Y-%m-%d %H:%M:%S"` 
		sh ./testcase/$line.sh > ./results/$line$i.log 2>&1
		count=`expr $count + 1`
		endtime=`date +"%Y-%m-%d %H:%M:%S"` 
		starttime1=`date -d  "$starttime" +%s`
		endtime1=`date -d  "$endtime" +%s`
		interval=`expr $endtime1 - $starttime1`
		if [ $( grep "$failed_keyword" ./results/$line$i.log | grep -v "the database system is shutting down" | wc -l ) -eq 0 ]; then
			printf "%-10s%-10s\n" ".... ok" $interval
		else
			printf "%-10s%-10s\n" ".... FAILED" $interval
			cp ./results/$line$i.log regression.diffs.hacheck
			exit 1
		fi
	done
done

#stop the database
printf "stop the database\n"
python $scripts_dir/pgxc_multi_single_shared_storage.py -o > ./results/stop_database_multi_single_shared_storage.log 2>&1

total_endtime=`date +"%Y-%m-%d %H:%M:%S"`
total_endvalue=`date -d  "$total_endtime" +%s`
printf "all %d tests passed.\n" $count
printf "total time: %ss\n" $(($total_endvalue - $total_startvalue))
