#!/bin/sh
# run all the test case of mutilip

source ./mutilip_env.sh
rm -rf results
mkdir -p results
#set -e

function create_instance()
{
    testcase_name="create_instance"
    rm -rf ./results/result_${testcase_name}.log
    node_num_all=2
    
    #stop all exists database
    kill_all 2 >> ./results/tmp_${testcase_name}.log 2>&1
    
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
}

function mutilip_test()
{
    while read line
    do
            line_content=`eval echo "$line" | sed -e 's/\r//g' | sed -e 's/\!/\&/g'`
            echo "trying $line_content"
            ./testlibpq "$line_content"
            echo ""
    done < mutilip.in >results/mutilip.out 2>&1
    
    cp results/mutilip.out results/mutilip.out.org
    sed -i 's/".*"//g' results/mutilip.out
    sed -i "s/$host_name/host_name/g" results/mutilip.out
    if [[ "$g_local_ip" = "127.0.0.1" ]]; then
        expect_file_name=expected_127.out
    else
        expect_file_name=expected_ipv4.out
    	sed -i "s/$g_local_ip/local_ip/g" results/mutilip.out
    fi
    
    if diff -c $expect_file_name results/mutilip.out >results/mutilip.diff; then
            echo "========================================"
            echo "All tests passed"
            exit 0
    else
            echo "========================================"
            echo "FAILED: the test result differs from the expected output"
            echo
            echo "Review the difference in results/mutilip.diff"
            echo "========================================"
            exit 1
    fi
}

create_instance
mutilip_test
kill_all 2





