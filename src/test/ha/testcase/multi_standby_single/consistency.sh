#!/bin/sh

source ./util.sh

function test_1()
{
  set_default

  echo "check 1-sync slaves"
  check_synchronous_commit "datanode1" 1

  echo "create table consistency_t1 (a int primary key, b int);
insert into consistency_t1 values (1, 1);
\parallel on 2
begin
update consistency_t1 set b = b + 1 where a = 1;
perform pg_sleep(2);
end;
/
begin
perform pg_sleep(1);
perform * from consistency_t1 where a = 1 for key share;
perform pg_sleep(3);
end;
/
\parallel off" > consistency_tmp.sql

  gsql -d $db -p $dn1_primary_port -f consistency_tmp.sql

  rm consistency_tmp.sql

  if [ $(gsql -d $db -p $dn1_standby_port -c "select count(*) from consistency_t1 where a = 1;" | grep 0 |wc -l) -eq 0 ]; then
    echo "consistency check success on dn1_standby"
  else
    echo "consistency check failed on dn1_standby"
    exit 1
  fi

}

function tear_down() {
  sleep 1
  set_default
  gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists consistency_t1;"
}

test_1
tear_down
