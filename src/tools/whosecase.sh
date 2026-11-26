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



if [ "${GAUSSDBTRUNK}" = "" ]; 
then
	echo "GAUSSDBTRUNK is empty!";	
	exit;
fi


CASEDB="${GAUSSDBTRUNK}/Code/src/tools/casedb"
TARGETCASE=$1
CASEFOUND=0

if [ "${TARGETCASE}" = "" ]; 
then
	echo "Which testcase?";
	exit;
fi

OLDIFS=$IFS
IFS=";"
while read testcase group owner	uid 
do
if	[ "$1" == "$testcase" ]; then
echo -e "Case\tGroup\tID\tOwner\t
${testcase}\t${group}\t${uid}\t${owner}"
CASEFOUND=1;
break;
fi
done < $CASEDB
IFS=$OLDIFS

if [ $CASEFOUND -eq 0 ]; 
then
	echo "$TARGETCASE not found!";
fi
