#!/bin/sh
# deploy primary-standby-dummystandby

source ./standby_env.sh

#stop the database
python $scripts_dir/pgxc_psd_single.py -o

#init the database
python $scripts_dir/pgxc_psd_single.py -c 1 -d 1

#wait database init, it always failed when CI Build
sleep 10

#build the standby
gs_ctl build -D $data_dir/datanode1_standby -Z single_node

#stop the database
python $scripts_dir/pgxc_psd_single.py -o

#set the primary postgresql.conf file
gs_guc set -Z datanode -D $primary_data_dir -c "most_available_sync = off"
gs_guc set -Z datanode -D $primary_data_dir -c "synchronous_commit = on"
gs_guc set -Z datanode -D $primary_data_dir -c "log_min_messages = DEBUG5"
gs_guc set -Z datanode -D $primary_data_dir -c "data_replicate_buffer_size=256MB"
gs_guc set -Z datanode -D $primary_data_dir -c "walsender_max_send_size=8MB"
gs_guc set -Z datanode -D $primary_data_dir -c "wal_receiver_buffer_size=64MB"
gs_guc set -Z datanode -D $primary_data_dir -c "shared_buffers=2GB"
gs_guc set -Z datanode -D $primary_data_dir -c "modify_initial_password = off"
gs_guc set -Z datanode -D $primary_data_dir -c "wal_sender_timeout = 120s"
gs_guc set -Z datanode -D $primary_data_dir -c "wal_receiver_timeout = 120s"
gs_guc set -Z datanode -D $primary_data_dir -c "enable_data_replicate = on"
gs_guc set -Z datanode -D $primary_data_dir -c "max_replication_slots = 4"
sed -i "/wal_level = hot_standby/c\wal_level = logical"        $data_dir/datanode1/postgresql.conf;
sed -i "/enable_slot_log/c\enable_slot_log = on"	     $data_dir/datanode1/postgresql.conf

#set the standby postgresql.conf file
gs_guc set -Z datanode -D $standby_data_dir -c "most_available_sync = off"
gs_guc set -Z datanode -D $standby_data_dir -c "synchronous_commit = on"
gs_guc set -Z datanode -D $standby_data_dir -c "log_min_messages = DEBUG5"
gs_guc set -Z datanode -D $standby_data_dir -c "data_replicate_buffer_size=256MB"
gs_guc set -Z datanode -D $standby_data_dir -c "walsender_max_send_size=8MB"
gs_guc set -Z datanode -D $standby_data_dir -c "wal_receiver_buffer_size=64MB"
gs_guc set -Z datanode -D $standby_data_dir -c "shared_buffers=2GB"
gs_guc set -Z datanode -D $standby_data_dir -c "modify_initial_password = off"
gs_guc set -Z datanode -D $standby_data_dir -c "wal_sender_timeout = 120s"
gs_guc set -Z datanode -D $standby_data_dir -c "wal_receiver_timeout = 120s"
gs_guc set -Z datanode -D $standby_data_dir -c "enable_data_replicate = on"
gs_guc set -Z datanode -D $standby_data_dir -c "max_replication_slots = 4"
sed -i "/wal_level = hot_standby/c\wal_level = logical"        $data_dir/datanode1_standby/postgresql.conf;
sed -i "/enable_slot_log/c\enable_slot_log = on"	     $data_dir/datanode1_standby/postgresql.conf

#set the dummystandby postgresql.conf file
gs_guc set -Z datanode -D $dummystandby_data_dir -c "most_available_sync = off"
gs_guc set -Z datanode -D $dummystandby_data_dir -c "synchronous_commit = on"
gs_guc set -Z datanode -D $dummystandby_data_dir -c "log_min_messages = DEBUG5"
gs_guc set -Z datanode -D $dummystandby_data_dir -c "data_replicate_buffer_size=256MB"
gs_guc set -Z datanode -D $dummystandby_data_dir -c "walsender_max_send_size=8MB"
gs_guc set -Z datanode -D $dummystandby_data_dir -c "wal_receiver_buffer_size=64MB"
gs_guc set -Z datanode -D $dummystandby_data_dir -c "shared_buffers=2GB"
gs_guc set -Z datanode -D $dummystandby_data_dir -c "modify_initial_password = off"
gs_guc set -Z datanode -D $dummystandby_data_dir -c "wal_sender_timeout = 120s"
gs_guc set -Z datanode -D $dummystandby_data_dir -c "wal_receiver_timeout = 120s"
gs_guc set -Z datanode -D $dummystandby_data_dir -c "max_replication_slots = 2"

#start the database
python $scripts_dir/pgxc_psd_single.py -s
