#!/bin/sh
# run all the test case(s) of hacheck_decode 

#init some variables

source ./standby_env.sh
test -f regression.diffs.hacheck && rm regression.diffs.hacheck

starttime_total=`date +"%Y-%m-%d %H:%M:%S"`
startpoint_total=`date -d  "$starttime_total" +%s`

array=("decode_single")
for element in ${array[@]}
do
    mkdir -vp ./results/$element
done

#init and start the database system
printf "init and start the database system\n"
sh deploy_multi_single.sh > ./results/decode_single.log 2>&1

printf "run the ha_schedule_decode_single\n"
printf "%-50s%-10s%-10s\n" "testcase" "result" "time(s)"
for line in `cat ha_schedule_decode_single | grep -v ^#`
do
    printf "%-50s" $line
    starttime_single=`date +"%Y-%m-%d %H:%M:%S"` 
    startpoint_single=`date -d "$starttime_single" +%s`
    sh ./testcase/$line.sh > ./results/$line.log 2>&1
    endtime_single=`date +"%Y-%m-%d %H:%M:%S"` 
    endpoint_single=`date -d "$endtime_single" +%s`
    interval=`expr $endpoint_single - $startpoint_single`
    if [ $( grep "$failed_keyword" ./results/$line.log | grep -v "the database system is shutting down" | wc -l ) -eq 0 ]; then
        printf "%-10s%-10s\n" ".... ok" $interval
    else
        printf "%-10s%-10s\n" ".... FAILED" $interval
        cp ./results/$line.log regression.diffs.hacheck
        exit 1
    fi
done

#stop the database system
printf "stop the database system\n"
python $scripts_dir/pgxc_multi_single.py -o > ./results/stop_decode_single.log 2>&1

total_endtime=`date +"%Y-%m-%d %H:%M:%S"`
total_endvalue=`date -d  "$total_endtime" +%s`
printf "decode test passed.\n"
printf "total time: %ss\n" $(($total_endvalue - $startpoint_total))
