#!/bin/sh
source ./standby_env.sh

bigdata_dir=$data_dir/datanode1/bigdata

function test_1()
{
check_instance

mkdir $bigdata_dir
dd if=/dev/zero of=$bigdata_dir/bigdata1 bs=1M count=1024
dd if=/dev/zero of=$bigdata_dir/bigdata2 bs=1M count=1024

gs_ctl querybuild -D $data_dir/datanode1_standby

stop_standby
gs_ctl build -Z single_node -D $data_dir/datanode1_standby -b full 1>"./results/pg_rewind/buildquery_normal.result" 2>&1 &

#wait build start
sleep 3
while [ $(gs_ctl querybuild -D $data_dir/datanode1_standby | grep -E $building_keyword | wc -l) -eq 1 ]
do
	sleep 1
done

if [ $(gs_ctl querybuild -D $data_dir/datanode1_standby | grep -E $build_keyword | wc -l) -eq 1 ]; then
	echo "build ended as expected"
else
	echo "$failed_keyword, querybuild error"
	exit 1
fi
}

function tear_down()
{
sleep 1
rm $bigdata_dir -rf

gs_ctl build -Z single_node -D $data_dir/datanode1_standby > /dev/null 2>&1
check_replication_setup
}

test_1
tear_down