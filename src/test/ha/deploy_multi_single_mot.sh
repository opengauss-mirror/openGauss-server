#!/bin/sh
# deploy primary-standby-dummystandby

source ./standby_env.sh

node_num=4
#python $scripts_dir/pgxc_multi.py 
#stop the database
python $scripts_dir/pgxc_multi_single.py -o

sleep 2
#init the database
python $scripts_dir/pgxc_multi_single.py -c 1 -d $node_num

#build the standby
gs_ctl build -D $data_dir/datanode1_standby -Z single_node
gs_ctl build -D $data_dir/datanode4_standby -Z single_node


#stop the database
python $scripts_dir/pgxc_multi_single.py -o

#set the primary postgresql.conf file
gs_guc set -Z datanode -D $primary_data_dir -c "most_available_sync = on"
gs_guc set -Z datanode -D $primary_data_dir -c "synchronous_commit = on"
gs_guc set -Z datanode -D $primary_data_dir -c "log_min_messages = DEBUG5"
gs_guc set -Z datanode -D $primary_data_dir -c "data_replicate_buffer_size=256MB"
gs_guc set -Z datanode -D $primary_data_dir -c "walsender_max_send_size=8MB"
gs_guc set -Z datanode -D $primary_data_dir -c "wal_receiver_buffer_size=64MB"
gs_guc set -Z datanode -D $primary_data_dir -c "shared_buffers=2GB"
gs_guc set -Z datanode -D $primary_data_dir -c "modify_initial_password = off"
gs_guc set -Z datanode -D $primary_data_dir -c "wal_sender_timeout = 120s"
gs_guc set -Z datanode -D $primary_data_dir -c "wal_receiver_timeout = 120s"
gs_guc set -Z datanode -D $primary_data_dir -c "max_replication_slots = 8"
gs_guc set -Z datanode -D $primary_data_dir -c "max_wal_senders = 8"
gs_guc set -Z datanode -D $primary_data_dir -c "replication_type = 1"
gs_guc set -Z datanode -D $primary_data_dir -c "enable_data_replicate = off"
gs_guc set -Z datanode -D $primary_data_dir -c "enable_incremental_checkpoint = off"

echo $node_num
for((i=1; i<=$node_num; i++))
do
	datanode_dir=$data_dir/datanode$i
	datanode_dir=$datanode_dir"_standby"
	echo $datanode_dir
	gs_guc set -Z datanode -D $datanode_dir -c "most_available_sync = on"
	gs_guc set -Z datanode -D $datanode_dir -c "synchronous_commit = on"
	gs_guc set -Z datanode -D $datanode_dir -c "log_min_messages = DEBUG5"
	gs_guc set -Z datanode -D $datanode_dir -c "data_replicate_buffer_size=256MB"
	gs_guc set -Z datanode -D $datanode_dir -c "walsender_max_send_size=8MB"
	gs_guc set -Z datanode -D $datanode_dir -c "wal_receiver_buffer_size=64MB"
	gs_guc set -Z datanode -D $datanode_dir -c "shared_buffers=2GB"
	gs_guc set -Z datanode -D $datanode_dir -c "modify_initial_password = off"
	gs_guc set -Z datanode -D $datanode_dir -c "wal_sender_timeout = 120s"
	gs_guc set -Z datanode -D $datanode_dir -c "wal_receiver_timeout = 120s"
	gs_guc set -Z datanode -D $datanode_dir -c "max_replication_slots = 8"
	gs_guc set -Z datanode -D $datanode_dir -c "max_wal_senders = 8"
	gs_guc set -Z datanode -D $datanode_dir -c "replication_type = 1"
	gs_guc set -Z datanode -D $datanode_dir -c "enable_data_replicate = off"
	gs_guc set -Z datanode -D $datanode_dir -c "enable_incremental_checkpoint = off"
done

#python $scripts_dir/pgxc_multi.py -o

sleep 2
#start the database
python $scripts_dir/pgxc_multi_single.py -s
