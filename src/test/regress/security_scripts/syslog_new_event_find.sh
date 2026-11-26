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



# example: ./syslog_new_event_find.sh postgres gs_auditing_policy audit_access_dml_policy select 1

PGDB=$1
PGPORT=$2
PGPOLICY=$3
POL_NAME=$4
QUERY=$5
EXPECTED=$6

GSQL_OPTS="-t -d $PGDB -p $PGPORT -U policy_officer -W 1q@W3e4r"
#echo "connection str: " $GSQL_OPTS
GET_ID_QUERY="select oid from $PGPOLICY where polname = lower('$POL_NAME');"
POL_ID=`gsql $GSQL_OPTS -c "$GET_ID_QUERY" | awk NF | xargs`
if [ "$POL_ID" == "" ]; then echo "no policy found by name '$POL_NAME'" ; exit 1 ; fi

if [ "$PGPOLICY" == "gs_security_policy" ]; then 
    if [ $(sudo grep PGACCESSCONROL /var/log/localmessages | grep "$POL_ID" | grep -v "\[policy_officer\]" | grep gsql | grep "$QUERY" | tail -"$EXPECTED" | wc -l) -eq "$EXPECTED" ]; then echo "Yay"; exit 0 ; else echo "Nay"; fi
elif [ "$PGPOLICY" == "gs_masking_policy" ]; then
    if [ $(sudo grep PGMASKING /var/log/localmessages | grep "$POL_ID" | grep -v "\[policy_officer\]" | tail -"$EXPECTED" | grep "$QUERY" | wc -l) -eq "$EXPECTED" ]; then echo "Yay"; exit 0 ; else echo "Nay"; fi
elif [ "$PGPOLICY" == "gs_rls_policy" ]; then
    if [ $(sudo grep PGRLS /var/log/localmessages | tail -"$EXPECTED" | grep "$QUERY" | wc -l) -eq "$EXPECTED" ]; then echo "Yay"; exit 0 ; else echo "Nay"; fi
else
    if [ $(sudo grep PGAUDIT /var/log/localmessages | grep "$POL_ID" | grep gsql | grep "$QUERY" | tail -"$EXPECTED" | wc -l) -eq "$EXPECTED" ]; then echo "Yay"; exit 0 ; else echo "Nay"; fi
fi

echo "logs found: (without expected query: \"$QUERY\")"
grep PGAUDIT /var/log/localmessages | grep "$POL_ID" | grep -v "\[policy_officer\]" | tail -"$EXPECTED"
echo ""
