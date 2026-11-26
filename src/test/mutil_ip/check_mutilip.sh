#!/bin/sh
# Copyright (c) 2026 Huawei Technologies Co., Ltd.
# openGauss-server is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#          http://license.coscl.org.cn/MulanPSL2
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.

# run all the test case of mutilip

source ./mutilip_env.sh
rm -rf results
mkdir -p results
#set -e

node_num_all=3
exit_code=0

function create_instance()
{
    testcase_name="create_instance"
    rm -rf ./results/result_${testcase_name}.log
    
    #stop all exists database
    kill_all ${node_num_all} >> ./results/tmp_${testcase_name}.log 2>&1
    
    printf "init the database...\n"
    python create_server.py -d $node_num_all >> ./results/tmp_${testcase_name}.log 2>&1
 
    printf "setup hba for ip...\n"
    setup_hba $node_num_all
  
    printf "start the primary database...\n"
    start_primary >> ./results/result_${testcase_name}.log

    printf "build the standby...\n"
    for i in `seq 2 ${node_num_all}`
    do
    $bin_dir/gs_ctl build -Z single_node -D $data_dir/datanode$i -b full >> ./results/tmp_${testcase_name}.log 2>&1
    done
    check_standby_startup >> ./results/result_${testcase_name}.log >> ./results/tmp_${testcase_name}.log 2>&1
        
    printf "check result...\n"
    check_instance_primary_standby >> ./results/result_${testcase_name}.log

    create_test_user   >> ./results/result_${testcase_name}.log
}

function mutilip_test()
{
    echo "========================================"
    echo "mutilip_test[target_session_attrs] checking"
    outfile="results/mutilip.out"
    while read line
    do
            line_content=`eval echo "$line" | sed -e 's/\r//g' | sed -e 's/\!/\&/g'`
            echo "trying $line_content"
            ./testlibpq "$line_content"
            echo ""
    done < mutilip.in >results/mutilip.out 2>&1
    
    cp ${outfile} ${outfile}.org
    sed -i 's/"[^"]*"//g' ${outfile}
    sed -i "s/$host_name/host_name/g" ${outfile}
    sed -i 's/connect_timeout=./connect_timeout=/g' ${outfile}
    if [[ "$g_local_ip" = "127.0.0.1" ]]; then
        expect_file_name=expected_127.out
    else
        expect_file_name=expected_ipv4.out
    	sed -i "s/$g_local_ip/local_ip/g" ${outfile}
    fi
    
    if diff -c $expect_file_name ${outfile} >results/mutilip.diff; then
            echo "========================================"
            echo "All tests passed"
    else
            echo "========================================"
            echo "FAILED: the test result differs from the expected output"
            echo
            echo "Review the difference in results/mutilip.diff"
            echo "========================================"
    fi
}

create_instance

source ./check_balance.sh
if [ $# -eq 1 ] && [ $1'x' == 'balancex' ];then
  check_balance
else
  mutilip_test
  check_balance
fi

kill_all ${node_num_all}
