#!/bin/bash
#Copyright (c) 2020 Huawei Technologies Co.,Ltd.
#
#openGauss is licensed under Mulan PSL v2.
#You can use this software according to the terms and conditions of the Mulan PSL v2.
#You may obtain a copy of Mulan PSL v2 at:
#
#          http://license.coscl.org.cn/MulanPSL2
#
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.
#-------------------------------------------------------------------------
#
# run_drop_cache.sh
#    script to drop system caches
#
# IDENTIFICATION
#    src/bin/scripts/run_drop_cache.sh
#
#-------------------------------------------------------------------------
set -e

SHELL_FOLDER=$(dirname $(readlink -f "$0"))

if [ ! -f "$SHELL_FOLDER"/drop_caches.sh ] ; then
    echo "ERROR: Can not found $SHELL_FOLDER/drop_caches.sh"
    exit 1
fi

echo "drop cache start"
if [ $(ps -ef |grep -E '\<drop_caches.sh\>' |grep -v grep |wc -l) -eq 0 ]; then nohup sh $SHELL_FOLDER/drop_caches.sh >>/dev/null 2>&1 & fi

echo "configure the drop_caches.sh to crontab"
tempfile=$(mktemp crontab.XXXXXX)
crontab -l > $tempfile
sed -i '/\/drop_caches.sh/d' $tempfile
echo "*/1 * * * * if [ \$(ps -ef |grep -E '\<drop_caches.sh\>' |grep -v grep |wc -l) -eq 0 ]; then nohup sh $SHELL_FOLDER/drop_caches.sh >>/dev/null 2>&1 & fi" >> $tempfile
crontab $tempfile
rm -f $tempfile

echo "drop cache finish"
