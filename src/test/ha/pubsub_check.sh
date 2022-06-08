#!/bin/sh
source ./util.sh

pub_datadir="$data_dir/pub"
pub_port=8988
conn_port=8989
sub_datadir="$data_dir/sub"
sub_port=8990

function test_1()
{
  echo "init database and start"
  gs_initdb -D $pub_datadir --nodename='sgnode' -w Gauss@123
  gs_initdb -D $sub_datadir --nodename='sgnode' -w Gauss@123

  gs_guc set -D $pub_datadir -c "wal_level = logical"
  gs_guc set -D $sub_datadir -c "wal_level = logical"

  gs_guc generate -S Gauss@123 -D $GAUSSHOME/bin -o subscription
  sed -i '$ahost replication '"$username"' 127.0.0.1/32 sha256' $pub_datadir/pg_hba.conf

  gaussdb -D $pub_datadir -p $pub_port &
  gaussdb -D $sub_datadir -p $sub_port &

  sleep 10

  echo "create tables and insert data"
  gsql -d postgres -p $pub_port -c "create table t1 (a int, b int); insert into t1 values (1,1),(2,2),(3,3); create table t2(a int); insert into t2 values (1),(2),(3);"
  gsql -d postgres -p $sub_port -c "create table t1 (a int, b int); create table t2(a int);"

  echo "create publication for t1"
  gsql -d postgres -p $pub_port -c "create publication pub1 for table t1;"

  echo "create subscription for pub1 and check initial data"
  gsql -d postgres -p $sub_port -c "create subscription sub1 connection 'host=127.0.0.1 port=$conn_port user=$username dbname=postgres password=Gauss@123' publication pub1;"
  sleep 2
  if [ $(gsql -d postgres -p $sub_port -c "select count(*) from t1;" | grep `expr 1 \* 3` |wc -l) -eq 1 ]; then
    echo "initial data on t1 synchronize success"
  else
    echo "initial data on t1 synchronize $failed_keyword"
    exit 1
  fi

  echo "alter pub1 add t2 and refresh sub"
  gsql -d postgres -p $pub_port -c "alter publication pub1 add table t2;"
  gsql -d postgres -p $sub_port -c "alter subscription sub1 refresh publication;"
  sleep 2
  if [ $(gsql -d postgres -p $sub_port -c "select count(*) from t2;" | grep `expr 1 \* 3` |wc -l) -eq 1 ]; then
    echo "initial data on t2 synchronize success"
  else
    echo "initial data on t2 synchronize $failed_keyword"
    exit 1
  fi

  echo "test incremental data"
  gsql -d postgres -p $pub_port -c "insert into t2 values (4);"
  sleep 2
  if [ $(gsql -d postgres -p $sub_port -c "select count(*) from t2;" | grep `expr 1 \* 4` |wc -l) -eq 1 ]; then
    echo "incremental data on t2 synchronize success"
  else
    echo "incremental data on t2 synchronize $failed_keyword"
    exit 1
  fi
}

function tear_down() {
  ps -ef | grep -w $pub_datadir | grep -v grep | awk '{print $2}' | xargs kill -9
  ps -ef | grep -w $sub_datadir | grep -v grep | awk '{print $2}' | xargs kill -9
}

test_1 > ./results/pubsub_check.log 2>&1
tear_down >> ./results/pubsub_check.log 2>&1
echo "publication and subscription test ok."