#!/bin/sh

source ./util.sh

function test_gs_stat_walsender()
{
    echo -e '\n------------------------------------------------------------\n'
    # test default
    sqls=(
        'select is_enable_stat,send_times,first_send_time,last_send_time,last_reset_time,avg_send_interval,since_last_send_interval from gs_stat_walsender() limit 1;'
        'select is_enable_stat from gs_stat_walsender() limit 1;'
        'select is_enable_stat from gs_stat_walsender(1) limit 1;' # enable
    )

    expecteds=(
        'f|0|||||'
        'f'
        't'
    )

    sql_num=${#sqls[@]}
    exp_num=${#expecteds[@]}
    if test $sql_num != $exp_num
    then
        echo "sqls array size($sql_num) not equals expecteds size($exp_num) ... $failed_keyword"
    fi
    
    for (( i=0; i<$sql_num; i++ ))
    do
        output=$(gsql -d $db -p $dn1_primary_port -Atc "${sqls[$i]}")
        echo "${sqls[$i]}"
        echo "expected:"
        echo "${expecteds[$i]}"
        echo "output:"
        echo "${output}"

        if [ "$output" != "${expecteds[$i]}" ]
        then
            echo "case-se$i: $failed_keyword"
        else
            echo "case-se$i: success"
        fi
        echo ''
    done

    # prepare data
    data_sql="create table walstat_t1(a int, b varchar(100), c text);
            insert into walstat_t1 values(1, 'hello', 'test'), (1, 'hello', 'test');
            insert into walstat_t1 values(1, 'hello', 'test'), (1, 'hello', 'test');
            insert into walstat_t1 values(1, 'hello', 'test'), (1, 'hello', 'test');"

    gsql -d $db -p $dn1_primary_port -c "$data_sql"
    echo ''
    sleep 5

    sqls=(
        'select send_times from gs_stat_walsender() limit 1;'
        'select first_send_time as has_value from gs_stat_walsender() limit 1;'
        'select last_send_time as has_value from gs_stat_walsender() limit 1;'
        'select avg_send_interval as has_value from gs_stat_walsender() limit 1;'
        'select since_last_send_interval as has_value from gs_stat_walsender() limit 1;'
        'select is_enable_stat from gs_stat_walsender(-1) limit 1;'
        'select last_reset_time from gs_stat_walsender(0) limit 1;'
        'select cur_time from gs_stat_walsender() limit 1;'
        'select channel from gs_stat_walsender() limit 1;'
    )

    unexpecteds=(
        '0'
        '0'
        '0'
        '0'
        '0'
        't'
        ''
        ''
        ''
    )
    sql_num=${#sqls[@]}
    
    for (( i=0; i<$sql_num; i++ ))
    do
        output=$(gsql -d $db -p $dn1_primary_port -Atc "${sqls[$i]}")
        echo "${sqls[$i]}"
        echo "unexpected:"
        echo "${unexpecteds[$i]}"
        echo "output:"
        echo "${output}"

        if [ "$output" = "${unexpecteds[$i]}" ]
        then
            echo "case-su$i: $failed_keyword"
        else
            echo "case-su$i: success"
        fi
        echo ''
    done

    # test count
    sql='select count(1) from gs_stat_walsender(2);'
    expected='4'
    output=$(gsql -d $db -p $dn1_primary_port -Atc "${sql}")
    echo "${sql}"
    echo "expected:"
    echo "${expected}"
    echo "output:"
    echo "${output}"
    if [ "$output" != "${expected}" ]
    then
        echo "case-sx: $failed_keyword"
    else
        echo "case-sx: success"
    fi

    # just print stats
    gsql -d $db -p $dn1_primary_port -Atc "select * from gs_stat_walsender();"

    # test disable
    sql='select is_enable_stat,send_times,first_send_time,last_send_time,last_reset_time,avg_send_interval,since_last_send_interval from gs_stat_walsender(-1)'
    echo $sql
    output_s1=$(gsql -d $db -p $dn1_primary_port -Atc "$sql")
    echo 'output_s1:'
    echo $output_s1
    echo ''
    sql='select is_enable_stat,send_times,first_send_time,last_send_time,last_reset_time,avg_send_interval,since_last_send_interval from gs_stat_walsender()'
    echo $sql
    output_s2=$(gsql -d $db -p $dn1_primary_port -Atc "$sql")
    echo 'output_s2:'
    echo $output_s2
    if [ "$output_s1" = "$output_s2" ]
    then
        echo "disable gs_stat_walsender: success"
    else
        echo "disable gs_stat_walsender: $failed_keyword"
    fi
    echo ''

    # test reset
    echo 'case reset receiver, writer:'
    sql='select send_times,first_send_time,last_send_time,avg_send_interval,since_last_send_interval from gs_stat_walsender(0)'
    expected='0||||
0||||
0||||
0||||'
    output=$(gsql -d $db -p $dn1_primary_port -Atc "$sql")
    echo $sql
    echo 'expected:'
    echo $expected
    echo 'output:'
    echo $output
    if [ "$output" = "$expected" ]
    then
        echo "reset gs_stat_walsender: success"
    else
        echo "reset gs_stat_walsender: $failed_keyword"
    fi
    echo ''

    # test should error
    sql='select gs_stat_walsender(666);'
    expected="ERROR:  illegal value \"666\" for parameter \"operation\".
HINT:  -1: disable, 0: reset, 1: enable, 2: get
CONTEXT:  referenced column: gs_stat_walsender"
    output=$(gsql -d $db -p $dn1_standby_port -c "$sql" 2>&1)
    echo "$sql"
    echo 'expected:'
    echo "$expected"
    echo 'output:'
    echo "$output"
    if [ "$output" = "$expected" ]
    then
        echo "gs_stat_walsender check params: success"
    else
        echo "gs_stat_walsender check params: $failed_keyword"
    fi
    echo ''

    echo -e '\n------------------------------------------------------------\n'
}

function test_gs_stat_walreceiver_and_walwriter()
{
    echo -e '\n------------------------------------------------------------\n'

    # test default
    sqls=(
        # receiver
        'select count(1) from gs_stat_walreceiver();'
        'select is_enable_stat from gs_stat_walreceiver();'
        'select buffer_full_times from gs_stat_walreceiver(2);'
        'select wake_writer_times from gs_stat_walreceiver();'
        'select first_wake_time from gs_stat_walreceiver(2);'
        'select last_wake_time from gs_stat_walreceiver();'
        'select last_reset_time from gs_stat_walreceiver();'
        'select since_last_wake_interval from gs_stat_walreceiver();'
        'select is_enable_stat from gs_stat_walreceiver(1);' # enable
        # writer
        'select count(1) from gs_stat_walrecvwriter();'
        'select is_enable_stat from gs_stat_walrecvwriter(2);'
        'select total_write_bytes from gs_stat_walrecvwriter();'
        'select write_times from gs_stat_walrecvwriter();'
        'select total_write_time from gs_stat_walrecvwriter();'
        'select avg_write_time from gs_stat_walrecvwriter();'
        'select avg_write_bytes from gs_stat_walrecvwriter(2);'
        'select total_sync_bytes from gs_stat_walrecvwriter();'
        'select sync_times from gs_stat_walrecvwriter();'
        'select total_sync_time from gs_stat_walrecvwriter(2);'
        'select avg_sync_time from gs_stat_walrecvwriter();'
        'select avg_sync_bytes from gs_stat_walrecvwriter(2);'
        'select is_enable_stat from gs_stat_walrecvwriter(0);'
        'select is_enable_stat from gs_stat_walrecvwriter(1);' # enable
    )

    expecteds=(
        # receiver
        '1'
        'f'
        '0'
        '0'
        ''
        ''
        ''
        ''
        't'
        # writer
        '1'
        'f'
        '0'
        '0'
        '0'
        ''
        ''
        '0'
        '0'
        '0'
        ''
        ''
        'f'
        't'
    )

    sql_num=${#sqls[@]}
    exp_num=${#expecteds[@]}
    if test $sql_num != $exp_num
    then
        echo "test_gs_stat_walreceiver_and_walwriter sqls array size($sql_num) not equals expecteds size($exp_num) ... $failed_keyword"
    fi
    
    for (( i=0; i<$sql_num; i++ ))
    do
        output=$(gsql -d $db -p $dn1_standby_port -Atc "${sqls[$i]}")
        echo "${sqls[$i]}"
        echo "expected:"
        echo "${expecteds[$i]}"
        echo "output:"
        echo "${output}"

        if [ "$output" != "${expecteds[$i]}" ]
        then
            echo "case-re$i: $failed_keyword"
        else
            echo "case-re$i: success"
        fi
        echo ''
    done

    # prepare data
    data_sql="create table if not exists walstat_t1(a int, b varchar(100), c text);
            insert into walstat_t1 values(1, 'hello', 'test'), (1, 'hello', 'test');
            insert into walstat_t1 values(1, 'hello', 'test'), (1, 'hello', 'test');
            insert into walstat_t1 values(1, 'hello', 'test'), (1, 'hello', 'test');"

    gsql -d $db -p $dn1_primary_port -c "$data_sql"
    echo ''
    sleep 3
    # repeat once
    gsql -d $db -p $dn1_primary_port -c "$data_sql"
    echo ''
    sleep 3

    # test should have values
    sqls=(
        # receiver
        'select is_enable_stat from gs_stat_walreceiver();'
        'select buffer_current_size from gs_stat_walreceiver(2);'
        'select buffer_full_times from gs_stat_walreceiver(2);'
        'select wake_writer_times from gs_stat_walreceiver(1);'
        'select avg_wake_interval from gs_stat_walreceiver();'
        'select since_last_wake_interval from gs_stat_walreceiver(2);'
        'select first_wake_time from gs_stat_walreceiver();'
        'select last_wake_time from gs_stat_walreceiver(1);'
        # writer
        'select is_enable_stat from gs_stat_walrecvwriter(2);'
        'select total_write_bytes from gs_stat_walrecvwriter();'
        'select write_times from gs_stat_walrecvwriter(1);'
        'select total_write_time from gs_stat_walrecvwriter(2);'
        'select avg_write_time from gs_stat_walrecvwriter();'
        'select avg_write_bytes from gs_stat_walrecvwriter(2);'
        'select total_sync_bytes from gs_stat_walrecvwriter();'
        'select sync_times from gs_stat_walrecvwriter(1);'
        'select total_sync_time from gs_stat_walrecvwriter(2);'
        'select avg_sync_time from gs_stat_walrecvwriter();'
        'select avg_sync_bytes from gs_stat_walrecvwriter(2);'
        'select cur_time from gs_stat_walrecvwriter(2);'
    )

    unexpecteds=(
        # receiver
        'f'
        ''
        ''
        '0'
        '0'
        '0'
        ''
        ''
        # writer
        'f'
        '0'
        '0'
        '0'
        '0'
        '0'
        '0'
        '0'
        '0'
        '0'
        '0'
        ''
    )
    sql_num=${#sqls[@]}
    unexp_num=${#unexpecteds[@]}
    if test $sql_num != $unexp_num
    then
        echo "sqls array size($sql_num) not equals unexpecteds size($unexp_num) ... $failed_keyword"
    fi
    
    for (( i=0; i<$sql_num; i++ ))
    do
        output=$(gsql -d $db -p $dn1_standby_port -Atc "${sqls[$i]}")
        echo "${sqls[$i]}"
        echo "unexpected:"
        echo "${unexpecteds[$i]}"
        echo "output:"
        echo "${output}"

        if [ "$output" = "${unexpecteds[$i]}" ]
        then
            echo "case-ru$i: $failed_keyword"
        else
            echo "case-ru$i: success"
        fi
        echo ''
    done

    # test disable receiver, writer
    echo 'case: disable receiver, writer'
    sql='select is_enable_stat, buffer_full_times, wake_writer_times, avg_wake_interval,first_wake_time,last_wake_time,last_reset_time from gs_stat_walreceiver(-1);'
    echo $sql
    output_r1=$(gsql -d $db -p $dn1_standby_port -Atc "$sql")
    echo 'output_r1:'
    echo $output_r1
    echo ''
    sql='select is_enable_stat,total_write_bytes,write_times,total_write_time,avg_write_time,avg_write_bytes,total_sync_bytes,sync_times,total_sync_time,avg_sync_time,avg_sync_bytes,last_reset_time from gs_stat_walrecvwriter(-1);'
    echo $sql
    output_w1=$(gsql -d $db -p $dn1_standby_port -Atc "$sql")
    echo 'output_w1:'
    echo $output_w1
    echo ''

    gsql -d $db -p $dn1_primary_port -c "$data_sql"
    sleep 3
    echo ''

    sql='select is_enable_stat, buffer_full_times, wake_writer_times, avg_wake_interval,first_wake_time,last_wake_time,last_reset_time from gs_stat_walreceiver();'
    echo $sql
    output_r2=$(gsql -d $db -p $dn1_standby_port -Atc "$sql")
    echo 'output_r2:'
    echo $output_r2
    echo ''
    sql='select is_enable_stat,total_write_bytes,write_times,total_write_time,avg_write_time,avg_write_bytes,total_sync_bytes,sync_times,total_sync_time,avg_sync_time,avg_sync_bytes,last_reset_time from gs_stat_walrecvwriter();'
    echo $sql
    output_w2=$(gsql -d $db -p $dn1_standby_port -Atc "$sql")
    echo 'output_w2:'
    echo $output_w2
    echo ''

    if [ "$output_r1" = "$output_r2" ]
    then
        echo "disable gs_stat_walreceiver: success"
    else
        echo "disable gs_stat_walreceiver: $failed_keyword"
    fi

    if [ "$output_w1" = "$output_w2" ]
    then
        echo "disable gs_stat_walrecvwriter: success"
    else
        echo "disable gs_stat_walrecvwriter: $failed_keyword"
    fi
    echo ''

    # test reset
    echo 'case reset receiver, writer:'
    sql='select buffer_full_times, wake_writer_times, avg_wake_interval,first_wake_time,last_wake_time from gs_stat_walreceiver(0);'
    expected='0|0|||'
    output=$(gsql -d $db -p $dn1_standby_port -Atc "$sql")
    echo $sql
    echo 'expected:'
    echo $expected
    echo 'output:'
    echo $output
    if [ "$output" = "$expected" ]
    then
        echo "reset gs_stat_walreceiver: success"
    else
        echo "reset gs_stat_walreceiver: $failed_keyword"
    fi

    sql='select total_write_bytes,write_times,total_write_time,avg_write_time,avg_write_bytes,total_sync_bytes,sync_times,total_sync_time,avg_sync_time,avg_sync_bytes from gs_stat_walrecvwriter(0);'
    expected='0|0|0|||0|0|0||'
    output=$(gsql -d $db -p $dn1_standby_port -Atc "$sql")
    echo $sql
    echo 'expected:'
    echo $expected
    echo 'output:'
    echo $output
    if [ "$output" = "$expected" ]
    then
        echo "reset gs_stat_walrecvwriter: success"
    else
        echo "reset gs_stat_walrecvwriter: $failed_keyword"
    fi
    echo ''

    # test should error
    sql='select gs_stat_walreceiver(3);'
    expected="ERROR:  illegal value \"3\" for parameter \"operation\".
HINT:  -1: disable, 0: reset, 1: enable, 2: get
CONTEXT:  referenced column: gs_stat_walreceiver"
    output=$(gsql -d $db -p $dn1_standby_port -c "$sql" 2>&1)
    echo "$sql"
    echo 'expected:'
    echo "$expected"
    echo 'output:'
    echo "$output"
    if [ "$output" = "$expected" ]
    then
        echo "gs_stat_walreceiver check params: success"
    else
        echo "gs_stat_walreceiver check params: $failed_keyword"
    fi
    echo ''

    sql='select gs_stat_walrecvwriter(-100);'
    expected="ERROR:  illegal value \"-100\" for parameter \"operation\".
HINT:  -1: disable, 0: reset, 1: enable, 2: get
CONTEXT:  referenced column: gs_stat_walrecvwriter"
    output=$(gsql -d $db -p $dn1_standby_port -c "$sql" 2>&1)
    echo "$sql"
    echo 'expected:'
    echo "$expected"
    echo 'output:'
    echo "$output"
    if [ "$output" = "$expected" ]
    then
        echo "gs_stat_walrecvwriter check params: success"
    else
        echo "gs_stat_walrecvwriter check params: $failed_keyword"
    fi

    echo -e '\n------------------------------------------------------------\n'
}

function tear_down()
{
    echo 'tear down'
    gsql -d $db -p $dn1_primary_port -c "drop table if exists walstat_t1;"
    gsql -d $db -p $dn1_primary_port -c "select gs_stat_walsender(-1);"
    gsql -d $db -p $dn1_primary_port -c "select gs_stat_walsender(0);"
    gsql -d $db -p $dn1_standby_port -c "select gs_stat_walreceiver(-1);"
    gsql -d $db -p $dn1_standby_port -c "select gs_stat_walreceiver(0);"
    gsql -d $db -p $dn1_standby_port -c "select gs_stat_walrecvwriter(-1);"
    gsql -d $db -p $dn1_standby_port -c "select gs_stat_walrecvwriter(0);"
}

test_gs_stat_walsender
test_gs_stat_walreceiver_and_walwriter
tear_down
