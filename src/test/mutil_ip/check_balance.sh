#!/bin/bash
# Copyright (c) 2026 Huawei Technologies Co., Ltd.
# openGauss-server is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#          http://license.coscl.org.cn/MulanPSL2
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.

# auth:wrq
# date:20251231
source ./mutilip_env.sh
export connect_timeout=1

function loop_check() {
  port_list=""
  err_msg=""
  for i in `seq 10`; do
    output=$(./testlibpq_balance "$@" 2>&1 )
    if [ $? -eq 0 ]; then
      port_list+="$output "
    else
      err_msg=$output
    fi
  done
  if [ -z "$port_list" ]; then
    echo $err_msg
  else
    port_list=$(echo $port_list |xargs -n1 |sort -u |xargs)
    echo $port_list
  fi
}

function check_balance() {
  echo "========================================"
  echo "mutilip_balance_test[load_balance_host] checking"
  outfile="results/mutilip_balance.out"
  while read line
    do
            line_content=`eval echo "$line" | sed -e 's/\r//g' | sed -e 's/\!/\&/g'`
            echo "trying $line_content"
            loop_check "$line_content"
            echo ""
    done < mutilip_balance.in >${outfile} 2>&1

    cp ${outfile} ${outfile}.org
    sed -i 's/"[^"]*"//g' ${outfile}
    sed -i 's/\(.*\)server is not in hot standby mode\(.*\)/\1\2 server is not in hot standby mode/' ${outfile}
    sed -i '/Connection/s/ \{2,\}/ /g' ${outfile}
    sed -i "s/$host_name/host_name/g" ${outfile}
    sed -i 's/connect_timeout=./connect_timeout=/g' ${outfile}
    if [[ "$g_local_ip" = "127.0.0.1" ]]; then
        expect_file_name=expected_127_b.out
    else
        expect_file_name=expected_ipv4_b.out
        sed -i "s/$g_local_ip/local_ip/g" ${outfile}
    fi

    if diff -c ${expect_file_name} ${outfile} >results/mutilip_balance.diff; then
            echo "========================================"
            echo "All tests passed"
    else
            echo "========================================"
            echo "FAILED: the test result differs from the expected output"
            echo
            echo "Review the difference in results/mutilip_balance.diff"
            echo "========================================"
    fi

}

