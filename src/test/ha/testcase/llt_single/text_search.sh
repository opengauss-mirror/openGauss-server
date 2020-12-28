#!/bin/sh

source ./standby_env.sh

function pre_work()
{
check_instance

#prepare
#check database
gsql -d $db -p $dn1_primary_port -c "SELECT * FROM pg_database;"

#copy user defined dict to tsearch_dir
tsearch_dir="$install_dir/share/postgresql/tsearch_data/"
cp $scripts_dir/data/user_define.gbk.xdb $tsearch_dir
if [ $? -eq 0 ]
then 
	echo "copy succeed $scripts_dir/data/user_define.gbk.xdb"
else 
	echo "copy $failed_keyword $scripts_dir/data/user_define.gbk.xdb"
	exit 1
fi

cp $scripts_dir/data/user_define.utf8.xdb $tsearch_dir
if [ $? -eq 0 ]
then 
	echo "copy succeed $scripts_dir/data/user_define.utf8.xdb"
else 
	echo "copy $failed_keyword $scripts_dir/data/user_define.utf8.xdb"
	exit 1
fi
#set guc parameter zhparser_extra_dicts
gs_guc set -Z datanode -D $data_dir/datanode1 -c zhparser_extra_dicts="'user_define.gbk.xdb,user_define.utf8.xdb'"
gs_guc set -Z datanode -D $data_dir/datanode1_standby -c zhparser_extra_dicts="'user_define.gbk.xdb,user_define.utf8.xdb'"
gs_guc set -Z datanode -D $data_dir/datanode1_dummystandby -c zhparser_extra_dicts="'user_define.gbk.xdb,user_define.utf8.xdb'"

#stop the coordinator
stop_primary
stop_standby
#start the coordinator
start_standby
start_primary

#test Chinese Text Search
gsql -d $db -p $dn1_primary_port -c "SHOW zhparser_extra_dicts;"
gsql -d $db -p $dn1_primary_port -c "SELECT to_tsvector('zhparser', '新词条');"

#set guc parameter zhparser_extra_dicts(delimiter:\t)
gs_guc set -Z datanode -D $data_dir/datanode1 -c zhparser_extra_dicts="'user_define.gbk.xdb\tuser_define.utf8.xdb'"
gs_guc set -Z datanode -D $data_dir/datanode1_standby -c zhparser_extra_dicts="'user_define.gbk.xdb\tuser_define.utf8.xdb'"
gs_guc set -Z datanode -D $data_dir/datanode1_dummystandby -c zhparser_extra_dicts="'user_define.gbk.xdb\tuser_define.utf8.xdb'"

#stop the coordinator
stop_primary
stop_standby
#start the coordinator
start_standby
start_primary

#test Chinese Text Search
gsql -d $db -p $dn1_primary_port -c "SHOW zhparser_extra_dicts;"
gsql -d $db -p $dn1_primary_port -c "SELECT to_tsvector('zhparser', '新词条');"

#set guc parameter zhparser_extra_dicts(xdb->txt)
gs_guc set -Z datanode -D $data_dir/datanode1 -c zhparser_extra_dicts="'user_define.gbk.xdb,user_define.utf8.xdb,user_define.utf8'"
gs_guc set -Z datanode -D $data_dir/datanode1_standby -c zhparser_extra_dicts="'user_define.gbk.xdb,user_define.utf8.xdb,user_define.utf8'"
gs_guc set -Z datanode -D $data_dir/datanode1_dummystandby -c zhparser_extra_dicts="'user_define.gbk.xdb,user_define.utf8.xdb,user_define.utf8'"

#stop the coordinator
stop_primary
stop_standby
#start the coordinator
start_standby
start_primary

#test Chinese Text Search
gsql -d $db -p $dn1_primary_port -c "SHOW zhparser_extra_dicts;"
gsql -d $db -p $dn1_primary_port -c "SELECT to_tsvector('zhparser', '新词条');"


#set guc parameter zhparser_extra_dicts(utf8->ascii)
gs_guc set -Z datanode -D $data_dir/datanode1 -c zhparser_extra_dicts="'user_define.gbk.xdb,user_define.utf8.xdb,user_define.ascii.xdb'"
gs_guc set -Z datanode -D $data_dir/datanode1_standby -c zhparser_extra_dicts="'user_define.gbk.xdb,user_define.utf8.xdb,user_define.ascii.xdb'"
gs_guc set -Z datanode -D $data_dir/datanode1_dummystandby -c zhparser_extra_dicts="'user_define.gbk.xdb,user_define.utf8.xdb,user_define.ascii.xdb'"

#stop the coordinator
stop_primary
stop_standby
#start the coordinator
start_standby
start_primary

#test Chinese Text Search
gsql -d $db -p $dn1_primary_port -c "SHOW zhparser_extra_dicts;"
gsql -d $db -p $dn1_primary_port -c "SELECT to_tsvector('zhparser', '新词条');"

#set guc parameter zhparser_extra_dicts, set zhparser_dict_in_memory = off
gs_guc set -Z datanode -D $data_dir/datanode1 -c zhparser_extra_dicts="'user_define.gbk.xdb,user_define.utf8.xdb'"
gs_guc set -Z datanode -D $data_dir/datanode1 -c zhparser_dict_in_memory="off"
gs_guc set -Z datanode -D $data_dir/datanode1_standby -c zhparser_extra_dicts="'user_define.gbk.xdb,user_define.utf8.xdb'"
gs_guc set -Z datanode -D $data_dir/datanode1_standby -c zhparser_dict_in_memory="off"
gs_guc set -Z datanode -D $data_dir/datanode1_dummystandby -c zhparser_extra_dicts="'user_define.gbk.xdb,user_define.utf8.xdb'"
gs_guc set -Z datanode -D $data_dir/datanode1_dummystandby -c zhparser_dict_in_memory="off"

#stop the coordinator
stop_primary
stop_standby
#start the coordinator
start_standby
start_primary

#test Chinese Text Search
gsql -d $db -p $dn1_primary_port -c "SHOW zhparser_dict_in_memory;"
gsql -d $db -p $dn1_primary_port -c "SHOW zhparser_extra_dicts;"
gsql -d $db -p $dn1_primary_port -c "SELECT to_tsvector('zhparser', '新词条');"
}

pre_work

