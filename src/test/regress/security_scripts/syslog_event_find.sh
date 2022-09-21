#!/bin/bash

PGPORT=$1
POL_NAME=$2
QUERY=$3
EXPECTED=$4

GSQL_OPTS="-t -d regression -p $PGPORT -U policy_officer -W 1q@W3e4r"
GET_ID_QUERY="select oid from gs_auditing_policy where polname = lower('$POL_NAME');"
POL_ID=`gsql $GSQL_OPTS -c "$GET_ID_QUERY" | awk NF | xargs`
if [ "$POL_ID" == "" ]; then echo "no policy found by name '$POL_NAME'" ; exit 1 ; fi
if [ $(sudo grep PGAUDIT /var/log/localmessages | grep "$POL_ID" | grep -v "\[policy_officer\]" | tail -"$EXPECTED" | grep "$QUERY" | wc -l) -eq "$EXPECTED" ]; then echo "Yay"; exit 0 ; else echo "Nay"; fi
echo "logs found: (without expected query: \"$QUERY\")"
grep PGAUDIT /var/log/localmessages | grep "$POL_ID" | grep -v "\[policy_officer\]" | tail -"$EXPECTED"
echo ""

