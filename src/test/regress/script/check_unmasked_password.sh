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


# example: ./check_unmasked_password.sh "password"

PASSWORD=$1
DB_LOG_PATH=$2

TMP=$(ls -t $DB_LOG_PATH/postgresql-* | head -1);
DB_LOG_FILE_NAME=${TMP##*/};

check_log_file()
{
    if test -f "$DB_LOG_PATH/$DB_LOG_FILE_NAME";
    then
        LINES_LOGGED=$(grep -wFc "$PASSWORD" "$DB_LOG_PATH/$DB_LOG_FILE_NAME"); # -c for counting lines
        if [[ $LINES_LOGGED != 0 ]];
        then
          echo "PASSWORD IN LOG FILE: FAILED (raw password ($PASSWORD) found in log file)";
        else
          echo "NO PASSWORD IN LOG FILE: OK";
        fi
    else
        echo "FILE $DB_LOG_PATH/$DB_LOG_FILE_NAME NOT FOUND";
    fi
}

check_log_file
exit 0;


