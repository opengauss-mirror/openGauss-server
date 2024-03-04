#!/bin/bash

source $1/env_utils.sh $1 $2 $3
case_db="ddl_repldb"
subscription_dir=$1
base_port=$2
sql_dir="${subscription_dir}/testcase/ddl_replication_sql/${dbcompatibility}"
results_dir="${subscription_dir}/results"
dump_result_dir="${results_dir}/dump_results"
mkdir -p $dump_result_dir

function case_setup() {
    local case_use_db=$1
    echo "create $case_use_db database and tables"

    exec_sql_with_user $db $pub_node1_port "CREATE DATABASE $case_use_db"
    exec_sql_with_user $db $sub_node1_port "CREATE DATABASE $case_use_db"

    exec_sql $case_use_db $pub_node1_port "ALTER DATABASE $case_use_db SET dolphin.sql_mode='sql_mode_full_group,pipes_as_concat,ansi_quotes,no_zero_date,pad_char_to_full_length'"
    exec_sql $case_use_db $sub_node1_port "ALTER DATABASE $case_use_db SET dolphin.sql_mode='sql_mode_full_group,pipes_as_concat,ansi_quotes,no_zero_date,pad_char_to_full_length'"

    if [ -f "${case_use_db}.setup" ]; then
        echo "execute setup ${case_use_db}.setup"
        sh ${case_use_db}.setup $subscription_dir $base_port $case_use_db
    fi

    # Setup logical replication
    echo "create publication and subscription"
    let ppp=pub_node1_port+1
    publisher_connstr="port=$ppp host=$g_local_ip dbname=$case_use_db user=$username password=$passwd"
    exec_sql $case_use_db $pub_node1_port "CREATE PUBLICATION ${case_use_db}_pub FOR ALL TABLES with (ddl='all');"

    exec_sql $case_use_db $sub_node1_port "CREATE SUBSCRIPTION ${case_use_db}_sub CONNECTION '$publisher_connstr' PUBLICATION ${case_use_db}_pub"

    wait_for_subscription_sync $case_use_db $sub_node1_port "${case_use_db}_sub"
}

function case_teardown() {
    local case_use_db=$1

    exec_sql $case_use_db $sub_node1_port "DROP SUBSCRIPTION IF EXISTS ${case_use_db}_sub"
    exec_sql $case_use_db $pub_node1_port "DROP PUBLICATION IF EXISTS ${case_use_db}_pub"

    exec_sql $db $sub_node1_port "DROP DATABASE $case_use_db"
    exec_sql $db $pub_node1_port "DROP DATABASE $case_use_db"

    echo "$case_use_db tear down"
}

function run_test() {
    local case_dir=$1
    local totalerr=0

    cd $case_dir
    for testcase in $(ls -1 *.sql)
    do
        echo "run $testcase"
        local err=0
        cur_case=${testcase%%.sql}

        case_setup $cur_case

        exec_sql_file $cur_case $pub_node1_port "$case_dir/$testcase"

        wait_for_catchup $cur_case $pub_node1_port "${cur_case}_sub"

        if [ -f "${cur_case}.teardown" ]; then
            echo "execute teardown ${cur_case}.teardown"
            sh ${cur_case}.teardown $subscription_dir $base_port $cur_case
        fi

        exec_dump_db $cur_case $pub_node1_port "$dump_result_dir/$cur_case.dump.pub"
        exec_dump_db $cur_case $sub_node1_port "$dump_result_dir/$cur_case.dump.sub"

        diff $dump_result_dir/$cur_case.dump.pub $dump_result_dir/$cur_case.dump.sub > ${results_dir}/$cur_case.dump.diff

        if [ -s ${results_dir}/$cur_case.dump.diff ]; then
            if [ -f $sql_dir/acceptable_diff/$cur_case.diff ]; then
                diff ${results_dir}/$cur_case.dump.diff $sql_dir/acceptable_diff/$cur_case.diff > ${results_dir}/$cur_case.acceptable.diff
                if [ -s ${results_dir}/$cur_case.acceptable.diff ]; then
                    err=1
                else
                    rm ${results_dir}/$cur_case.acceptable.diff
                    rm ${results_dir}/$cur_case.dump.diff 
                    err=0
                fi
            else
                err=1
            fi

            if [ $err -eq 1 ];then
                echo "$cur_case dump compare failed, see ${results_dir}/$cur_case.dump.diff"
                totalerr=1
            fi
        else
            rm ${results_dir}/$cur_case.dump.diff 
        fi

        if [ $err -eq 0 ]; then
            case_teardown $cur_case
        fi

    done

    if [ $totalerr -eq 1 ];then
        echo "$failed_keyword when check incremental data of new table is replicated"
    fi

}

exec_sql $db $sub_node1_port "CREATE USER ddl_test_user PASSWORD '$passwd'"
exec_sql $db $pub_node1_port "CREATE USER ddl_test_user PASSWORD '$passwd'"
exec_sql $db $sub_node1_port "ALTER ROLE ddl_test_user sysadmin"
exec_sql $db $pub_node1_port "ALTER ROLE ddl_test_user sysadmin"

export test_username="ddl_test_user"

run_test $sql_dir