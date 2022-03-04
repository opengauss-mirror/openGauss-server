#!/bin/sh
# run all the test case of ipv6

source ./ipv6_env.sh
test -f regression.diffs.ipv6check && rm regression.diffs.ipv6check
rm -rf results
mkdir -p results

function test_1()
{
    testcase_name="single"
    rm -rf ./results/result_${testcase_name}.log
    node_num_all=1
    
    #stop all exists database
    kill_all 3 >> ./results/tmp_${testcase_name}.log 2>&1
    
    printf "init the database...\n"
    python create_server.py -d $node_num_all >> ./results/tmp_${testcase_name}.log 2>&1

    printf "setup hba for ip...\n"
    setup_hba $node_num_all
    
    printf "start the database...\n"
    start_normal >> ./results/result_${testcase_name}.log

    create_test_user   >> ./results/result_${testcase_name}.log
    gsql_test >> ./results/result_${testcase_name}.log
    jdbc_test >> ./results/result_${testcase_name}.log
    
    if [ $( grep "$failed_keyword" ./results/result_${testcase_name}.log | wc -l ) -eq 0 ]; then
        printf "===================================\n"
        printf "%s tests passed.\n" ${testcase_name}
        printf "===================================\n\n"
    else
        echo  "${testcase_name} tests .... FAILED"  >> regression.diffs.ipv6check
        printf "===================================\n"
        printf  "%s tests .... FAILED.\n" ${testcase_name}
        printf "===================================\n\n"
    fi
}

function test_2()
{
    testcase_name="primary_standby"
    rm -rf ./results/result_${testcase_name}.log
    node_num_all=2
    
    #stop all exists database
    kill_all 3 >> ./results/tmp_${testcase_name}.log 2>&1
    
    printf "init the database...\n"
    python create_server.py -d $node_num_all >> ./results/tmp_${testcase_name}.log 2>&1
 
    printf "setup hba for ip...\n"
    setup_hba $node_num_all
  
    printf "start the primary database...\n"
    start_primary >> ./results/result_${testcase_name}.log

    printf "build the standby...\n"
    $bin_dir/gs_ctl build -Z single_node -D $data_dir/datanode2 -b full >> ./results/tmp_${testcase_name}.log 2>&1
    check_standby_startup >> ./results/result_${testcase_name}.log >> ./results/tmp_${testcase_name}.log 2>&1
        
    printf "check result...\n"
    check_instance_primary_standby >> ./results/result_${testcase_name}.log
    
    create_test_user   >> ./results/result_${testcase_name}.log
    gsql_test >> ./results/result_${testcase_name}.log
    gsql_standby_test >> ./results/result_${testcase_name}.log
    jdbc_test >> ./results/result_${testcase_name}.log
    
    if [ $( grep "$failed_keyword" ./results/result_${testcase_name}.log | wc -l ) -eq 0 ]; then
        printf "===================================\n"
        printf "%s tests passed.\n" ${testcase_name}
        printf "===================================\n\n"
    else
        echo  "${testcase_name} tests .... FAILED"  >> regression.diffs.ipv6check
        printf "===================================\n"
        printf  "%s tests .... FAILED.\n" ${testcase_name}
        printf "===================================\n\n"
    fi
}

function test_3()
{
    testcase_name="muti_standby"
    rm -rf ./results/result_${testcase_name}.log
    node_num_all=3
    
    #stop all exists database
    kill_all 3 >> ./results/tmp_${testcase_name}.log 2>&1
    
    printf "init the database...\n"
    python create_server.py -d $node_num_all >> ./results/tmp_${testcase_name}.log 2>&1

    printf "setup hba for ip...\n"
    setup_hba $node_num_all
    
    printf "start the primary database...\n"
    start_primary >> ./results/result_${testcase_name}.log

    printf "build the muti standby...\n"
    $bin_dir/gs_ctl build -Z single_node -D $data_dir/datanode2 -b full >> ./results/tmp_${testcase_name}.log 2>&1
    $bin_dir/gs_ctl build -Z single_node -D $data_dir/datanode3 -b full >> ./results/tmp_${testcase_name}.log 2>&1
    check_multi_standby_startup  $node_num_all >> ./results/result_${testcase_name}.log
        
    printf "check result...\n"
    check_instance_multi_standby >> ./results/result_${testcase_name}.log

    create_test_user   >> ./results/result_${testcase_name}.log
    gsql_test >> ./results/result_${testcase_name}.log
    gsql_standby_test >> ./results/result_${testcase_name}.log
    jdbc_test >> ./results/result_${testcase_name}.log
    
    if [ $( grep "$failed_keyword" ./results/result_${testcase_name}.log | wc -l ) -eq 0 ]; then
        printf "===================================\n"
        printf "%s tests passed.\n" ${testcase_name}
        printf "===================================\n\n"
    else
        echo  "${testcase_name} tests .... FAILED"  >> regression.diffs.ipv6check
        printf "===================================\n"
        printf  "%s tests .... FAILED.\n" ${testcase_name}
        printf "===================================\n\n"
    fi
}

function test_4()
{
    testcase_name="casecade_standby"
    rm -rf ./results/result_${testcase_name}.log
    node_num_all=3
    
    #stop all exists database
    kill_all 3 >> ./results/tmp_${testcase_name}.log 2>&1
    
    printf "init the database...\n"
    python create_server.py -d $node_num_all >> ./results/tmp_${testcase_name}.log 2>&1

    printf "setup hba for ip...\n"
    setup_hba $node_num_all
    
    printf "start the primary database...\n"
    start_primary >> ./results/result_${testcase_name}.log
    
    printf "build the standby...\n"
    $bin_dir/gs_ctl build -Z single_node -D $data_dir/datanode2 -b full >> ./results/tmp_${testcase_name}.log 2>&1
    check_standby_startup  >> ./results/result_${testcase_name}.log
    
    printf "build the casecade standby...\n"
    $bin_dir/gs_ctl build -Z single_node -M cascade_standby  -D $data_dir/datanode3 -b standby_full >> ./results/tmp_${testcase_name}.log 2>&1
    check_casecade_standby_startup >> ./results/result_${testcase_name}.log
    
    printf "check result...\n"
    check_instance_casecade_standby >> ./results/result_${testcase_name}.log
    
    create_test_user   >> ./results/result_${testcase_name}.log
    gsql_test >> ./results/result_${testcase_name}.log
    gsql_standby_test >> ./results/result_${testcase_name}.log
    jdbc_test >> ./results/result_${testcase_name}.log
    
    if [ $( grep "$failed_keyword" ./results/result_${testcase_name}.log | wc -l ) -eq 0 ]; then
        printf "===================================\n"
        printf "%s tests passed.\n" ${testcase_name}
        printf "===================================\n\n"
    else
        echo  "${testcase_name} tests .... FAILED"  >> regression.diffs.ipv6check
        printf "===================================\n"
        printf  "%s tests .... FAILED.\n" ${testcase_name}
        printf "===================================\n\n"
    fi
}

function test_5()
{
    testcase_name="listen_thread_pool"
    rm -rf ./results/result_${testcase_name}.log
    node_num_all=3
    
    #stop all exists database
    kill_all 3 >> ./results/tmp_${testcase_name}.log 2>&1
    
    printf "init the database...\n"
    python create_server.py -d $node_num_all >> ./results/tmp_${testcase_name}.log 2>&1
    
    printf "set listen address and enalble thread pool...\n"
    set_node_conf 1 >> ./results/result_${testcase_name}.log
    set_node_conf 2 >> ./results/result_${testcase_name}.log
    set_node_conf 3 >> ./results/result_${testcase_name}.log

    printf "setup hba for ip...\n"
    setup_hba $node_num_all
    
    printf "start the primary database...\n"
    start_primary >> ./results/result_${testcase_name}.log

    printf "build the muti standby...\n"
    $bin_dir/gs_ctl build -Z single_node -D $data_dir/datanode2 -b full >> ./results/tmp_${testcase_name}.log 2>&1
    $bin_dir/gs_ctl build -Z single_node -D $data_dir/datanode3 -b full >> ./results/tmp_${testcase_name}.log 2>&1
    check_multi_standby_startup $node_num_all >> ./results/result_${testcase_name}.log
    
    printf "check result...\n"
    check_instance_multi_standby >> ./results/result_${testcase_name}.log
    
    create_test_user   >> ./results/result_${testcase_name}.log
    gsql_test >> ./results/result_${testcase_name}.log
    gsql_standby_test >> ./results/result_${testcase_name}.log
    jdbc_test >> ./results/result_${testcase_name}.log
    
    if [ $( grep "$failed_keyword" ./results/result_${testcase_name}.log | wc -l ) -eq 0 ]; then
        printf "===================================\n"
        printf "%s tests passed.\n" ${testcase_name}
        printf "===================================\n\n"
    else
        echo  "${testcase_name} tests .... FAILED"  >> regression.diffs.ipv6check
        printf "===================================\n"
        printf  "%s tests .... FAILED.\n" ${testcase_name}
        printf "===================================\n\n"
    fi
}

function test_6()
{
    testcase_name="check_disable_conn"
    rm -rf ./results/result_${testcase_name}.log
    node_num_all=3
    
    #stop all exists database
    kill_all 3 >> ./results/tmp_${testcase_name}.log 2>&1
    
    printf "init the database...\n"
    python create_server.py -d $node_num_all >> ./results/tmp_${testcase_name}.log 2>&1

    printf "set listen address...\n"
    set_listen_address 1
    set_listen_address 2
    set_listen_address 3

    printf "setup hba for ip...\n"
    setup_hba $node_num_all
    
    printf "start the primary database...\n"
    start_primary >> ./results/result_${testcase_name}.log
    
    printf "build the muti standby...\n"
    $bin_dir/gs_ctl build -Z single_node -D $data_dir/datanode2 -b full >> ./results/tmp_${testcase_name}.log 2>&1
    $bin_dir/gs_ctl build -Z single_node -D $data_dir/datanode3 -b full >> ./results/tmp_${testcase_name}.log 2>&1
    check_multi_standby_startup $node_num_all >> ./results/result_${testcase_name}.log
        
    printf "check result...\n"
    check_instance_multi_standby >> ./results/result_${testcase_name}.log

    #local port standby
    port=$(($g_base_standby_port + 4))
    if  [ $( $bin_dir/gsql -d $db -p $dn1_standby_port -c "select read_disable_conn_file();" | grep $port |  wc -l ) -eq 0 ]; then
        echo "$failed_keyword when $testcase_name" >> ./results/result_${testcase_name}.log
    fi

    #set disable_conn = remote port ( master port)
    port2=$(($g_base_standby_port + 1))
    $bin_dir/gsql -d $db -p $dn1_standby_port -c "select disable_conn('specify_connection','"$g_local_ip"','"$port2"');"
    if  [ $( $bin_dir/gsql -d $db -p $dn1_standby_port -c "select read_disable_conn_file();" | grep $port2 |  wc -l ) -eq 0 ]; then
        echo "$failed_keyword when $testcase_name" >> ./results/result_${testcase_name}.log
    fi
    #check standby is connected to primary
    sleep 2
    check_instance_multi_standby >> ./results/result_${testcase_name}.log

    #set disable_conn != remote port
    port2=$(($g_base_standby_port + 13))
    $bin_dir/gsql -d $db -p $dn1_standby_port -c "select disable_conn('specify_connection','"$g_local_ip"','"$port2"');"
    if  [ $( $bin_dir/gsql -d $db -p $dn1_standby_port -c "select read_disable_conn_file();" | grep $port2 |  wc -l ) -eq 0 ]; then
        echo "$failed_keyword when $testcase_name" >> ./results/result_${testcase_name}.log
    fi
    #check standby is disconnected from primary
    sleep 2
    if [ $(query_standby | grep -E $setup_keyword | wc -l) -eq 1 ]; then
        echo "$failed_keyword when $testcase_name" >> ./results/result_${testcase_name}.log
    fi

    #set disable_conn = remote port ( master port) again
    port=$(($g_base_standby_port + 1))
    $bin_dir/gsql -d $db -p $dn1_standby_port -c "select disable_conn('specify_connection','"$g_local_ip"','"$port"');"
    sleep 5
    #check standby is connected to primary        
    check_instance_multi_standby >> ./results/result_${testcase_name}.log

    
    if [ $( grep "$failed_keyword" ./results/result_${testcase_name}.log | wc -l ) -eq 0 ]; then
        printf "===================================\n"
        printf "%s tests passed.\n" ${testcase_name}
        printf "===================================\n\n"
    else
        echo  "${testcase_name} tests .... FAILED"  >> regression.diffs.ipv6check
        printf "===================================\n"
        printf  "%s tests .... FAILED.\n" ${testcase_name}
        printf "===================================\n\n"
    fi
}

function test_7()
{
    testcase_name="change_replconninfo"
    rm -rf ./results/result_${testcase_name}.log
    node_num_all=2

    #stop all exists database
    kill_all 3 >> ./results/tmp_${testcase_name}.log 2>&1

    printf "init the database...\n"
    python create_server.py -d $node_num_all >> ./results/tmp_${testcase_name}.log 2>&1

    printf "setup hba for ip...\n"
    setup_hba $node_num_all

    printf "start the primary database...\n"
    start_primary >> ./results/result_${testcase_name}.log

    printf "build the standby...\n"
    $bin_dir/gs_ctl build -Z single_node -D $data_dir/datanode2 -b full >> ./results/tmp_${testcase_name}.log 2>&1
    check_standby_startup >> ./results/result_${testcase_name}.log >> ./results/tmp_${testcase_name}.log 2>&1

    printf "check result...\n"
    check_instance_primary_standby >> ./results/result_${testcase_name}.log

    primary_ha_port=$(($g_base_standby_port + 1))
    tmp_primary_ha_port=$(($primary_ha_port - 1))
    sed -i "s/$primary_ha_port/$tmp_primary_ha_port/g" ${primary_data_dir}/postgresql.conf
    sed -i "s/$primary_ha_port/$tmp_primary_ha_port/g" ${standby_data_dir}/postgresql.conf
    $bin_dir/gs_ctl reload -D ${primary_data_dir}
    $bin_dir/gs_ctl reload -D ${standby_data_dir}
    sleep 5

    printf "check ha result...\n"
    check_instance_primary_standby >> ./results/result_${testcase_name}.log

    create_test_user   >> ./results/result_${testcase_name}.log
    gsql_test >> ./results/result_${testcase_name}.log
    gsql_standby_test >> ./results/result_${testcase_name}.log
    jdbc_test >> ./results/result_${testcase_name}.log

    if [ $( grep "$failed_keyword" ./results/result_${testcase_name}.log | wc -l ) -eq 0 ]; then
        printf "===================================\n"
        printf "%s tests passed.\n" ${testcase_name}
        printf "===================================\n\n"
    else
        echo  "${testcase_name} tests .... FAILED"  >> regression.diffs.ipv6check
        printf "===================================\n"
        printf  "%s tests .... FAILED.\n" ${testcase_name}
        printf "===================================\n\n"
    fi
}

function tear_down()
{
    node_num_all=4
    
    #stop all exists database
    kill_all $node_num_all
}
server_type=$1

case "$server_type" in
    ipv6_all)
        test_1
        test_2
        test_3
        test_4
        #test_5
        test_6
        test_7
        ;;
    normal)
        test_1
        ;;
    primary_standby)
        test_2
        test_7
        ;;
    muti_standby)
        test_3
        #test_5
        test_6
        ;;
    casecade_standby)
        test_4
        ;;
    *)
        echo "Internal Error: server_type option processing error: $server_type"
        echo "please input right paramenter values ipv6_all, normal, primary_standby, muti_standby or casecade_standby"
        exit 1
esac

#tear_down

printf "===================================\n"
printf "all tests finished.\n"
printf "===================================\n"



