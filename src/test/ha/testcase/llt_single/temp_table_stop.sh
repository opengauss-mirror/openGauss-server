#!/bin/sh

source ./standby_env.sh

function temp_table_stop()
{
check_instance

echo "" > $scripts_dir/data/temp_stop.sql
echo "create temp table stop_test(a int);" >> $scripts_dir/data/temp_stop.sql
echo "create table normal_test(a int);" >> $scripts_dir/data/temp_stop.sql
echo "insert into normal_test select generate_series(1, 1000);" >> $scripts_dir/data/temp_stop.sql
echo "insert into stop_test select generate_series(1, 1000);" >> $scripts_dir/data/temp_stop.sql
echo "create view v_stop_test as select * from stop_test;" >> $scripts_dir/data/temp_stop.sql
echo "create function pg_temp.f_test_temp(a int) returns void" >> $scripts_dir/data/temp_stop.sql
echo "as \$\$" >> $scripts_dir/data/temp_stop.sql
echo "begin" >> $scripts_dir/data/temp_stop.sql
echo "dbms_output.put_line('abc');" >> $scripts_dir/data/temp_stop.sql
echo "end; \$\$ language plpgsql;" >> $scripts_dir/data/temp_stop.sql
echo "\! gs_ctl stop -D $data_dir/datanode1" >> $scripts_dir/data/temp_stop.sql
echo "select * from stop_test;" >> $scripts_dir/data/temp_stop.sql
echo "select * from v_stop_test;" >> $scripts_dir/data/temp_stop.sql
echo "select * from pg_temp.f_test_temp(1);" >> $scripts_dir/data/temp_stop.sql

gsql -d $db -p $dn1_primary_port -f $scripts_dir/data/temp_stop.sql
}

function tear_down()
{
#start primary
start_primary
gs_clean -r -a -p $dn1_primary_port -v
gsql -d $db -p $dn1_primary_port -c "select * from pg_namespace"

#delete temp_stop.sql file
rm $scripts_dir/data/temp_stop.sql

gsql -d $db -p $dn1_primary_port -c "drop table if exists normal_test;"
}

temp_table_stop
tear_down