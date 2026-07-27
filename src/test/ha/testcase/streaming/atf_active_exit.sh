#!/bin/sh
source ./util.sh

atf_timeout=2
start_message="[ATF] recovery stage started"
finish_message="[ATF] recovery stage completed"

function cleanup()
{
  kill_primary
  if ps -ef | grep -w "$primary_data_dir" | grep -v grep >/dev/null; then
    echo "$failed_keyword, primary process is still running after cleanup"
  fi
  gs_guc set -Z datanode -D "$primary_data_dir" \
    -c "atf_task_counter_timeout_sec=0" || \
    echo "$failed_keyword, reset ATF timeout failed"
}

function fail()
{
  echo "$failed_keyword, $1"
  exit 1
}

trap cleanup EXIT

function test_atf_active_exit()
{
  kill_cluster
  gs_guc set -Z datanode -D "$primary_data_dir" \
    -c "atf_task_counter_timeout_sec=$atf_timeout" || fail "set ATF timeout failed"

  start_primary
  last_log=`ls -tr "$primary_data_dir"/pg_log/postgresql-* | tail -1`

  grep -F "$start_message" "$last_log" >/dev/null || fail "ATF worker did not start"

  found=0
  for i in `seq 1 10`
  do
    if grep -F "$finish_message" "$last_log" | grep -F "reason: timeout" >/dev/null; then
      found=1
      break
    fi
    sleep 1
  done

  if [ "$found" -ne 1 ]; then
    fail "ATF stage did not finish without a client session"
  fi

  gsql -d postgres -p "$dn1_primary_port" -c "select 1;" >/dev/null || \
    fail "gsql was affected after ATF completion"
}

test_atf_active_exit
