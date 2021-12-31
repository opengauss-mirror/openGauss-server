#!/bin/sh
# keep wal segment files when standby is offline using replication slot on primary

source ./standby_env.sh

function test_1()
{
check_instance_multi_standby

#create table slot1
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists mpp_slot1; CREATE TABLE mpp_slot1(id INT,name VARCHAR(15) NOT NULL);"


#set archive destination
archive_destination=$data_dir/archive_nas/dn1
#create archive slot
gsql -d $db -p $dn1_primary_port -c "set enable_slot_log = on; select * from pg_create_physical_replication_slot_extern('archive', false, 'NAS;$archive_destination;0;1');"

#check archive slot created
if [ $(gsql -d $db -p $dn1_primary_port -c "select * from pg_get_replication_slots();" | grep "archive" |wc -l) -eq 1 ]; then
	echo "success create archive slot"
else
	echo "$failed_keyword on create archive slot"
	exit 1
fi

#produce enough wal segment files
gsql -d $db -p $dn1_primary_port -c "INSERT INTO mpp_slot1 VALUES(311, 'sp');"
gsql -d $db -p $dn1_primary_port -c "INSERT INTO mpp_slot1 SELECT * FROM mpp_slot1;"
gsql -d $db -p $dn1_primary_port -c "INSERT INTO mpp_slot1 SELECT * FROM mpp_slot1;"
gsql -d $db -p $dn1_primary_port -c "INSERT INTO mpp_slot1 SELECT * FROM mpp_slot1;"
gsql -d $db -p $dn1_primary_port -c "INSERT INTO mpp_slot1 SELECT * FROM mpp_slot1;"
gsql -d $db -p $dn1_primary_port -c "INSERT INTO mpp_slot1 SELECT * FROM mpp_slot1;"
gsql -d $db -p $dn1_primary_port -c "INSERT INTO mpp_slot1 SELECT * FROM mpp_slot1;"
gsql -d $db -p $dn1_primary_port -c "INSERT INTO mpp_slot1 SELECT * FROM mpp_slot1;"
gsql -d $db -p $dn1_primary_port -c "INSERT INTO mpp_slot1 SELECT * FROM mpp_slot1;"
gsql -d $db -p $dn1_primary_port -c "INSERT INTO mpp_slot1 SELECT * FROM mpp_slot1;"
gsql -d $db -p $dn1_primary_port -c "INSERT INTO mpp_slot1 SELECT * FROM mpp_slot1;"
gsql -d $db -p $dn1_primary_port -c "INSERT INTO mpp_slot1 SELECT * FROM mpp_slot1;"
gsql -d $db -p $dn1_primary_port -c "INSERT INTO mpp_slot1 SELECT * FROM mpp_slot1;"
gsql -d $db -p $dn1_primary_port -c "INSERT INTO mpp_slot1 SELECT * FROM mpp_slot1;"
gsql -d $db -p $dn1_primary_port -c "INSERT INTO mpp_slot1 SELECT * FROM mpp_slot1;"
gsql -d $db -p $dn1_primary_port -c "INSERT INTO mpp_slot1 SELECT * FROM mpp_slot1;"

#wait archive
sleep 30

#check archive xlog
if [ $(ls $archive_destination/pg_xlog |wc -l) -gt 0 ]; then
	echo "success archive xlog"
else
	echo "$failed_keyword: failed archive xlog"
	exit 1
fi

#check global barrier
gsql -d $db -p $dn1_primary_port -c "select * from gs_get_global_barriers_status();"

#clean archive xlog
current_lsn=`gsql -d postgres -p 28891 -c "select * from pg_get_flush_lsn()" -t -A -X`
echo "current lsn is $local_lsn"
gsql -d $db -p $dn1_primary_port -c "select * from gs_set_obs_delete_location_with_slotname('$current_lsn', 'archive');"


sleep 5
#drop archive slot
gsql -d $db -p $dn1_primary_port -c "set enable_slot_log = on; select * from pg_drop_replication_slot('archive');"

#check archive slot created
if [ $(gsql -d $db -p $dn1_primary_port -c "select * from pg_get_replication_slots();" | grep "archive" |wc -l) -eq 0 ]; then
        echo "success drop archive slot"
else
        echo "$failed_keyword on drop archive slot"
        exit 1
fi


if [ $? -eq 0 ]; then
	echo "all of success"
else
	echo "$failed_keyword: archive slot failed."
	exit 1
fi
}

function tear_down()
{
sleep 1
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists mpp_slot1;"
rm -rf $data_dir/archive_nas
}

test_1
tear_down
