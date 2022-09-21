#!/bin/bash

FILE=$1

sed -ir 's/roles\[1[6-9][[:digit:]][[:digit:]][[:digit:]],1[6-9][[:digit:]][[:digit:]][[:digit:]]/roles\[roid1,roid2/g' $FILE
if [ $? -ne 0 ]; then
    echo "remove oid1 failed"
fi

sed -ir 's/roles\[1[6-9][[:digit:]][[:digit:]][[:digit:]]/roles\[roid1/g' $FILE
if [ $? -ne 0 ]; then
    echo "remove oid2 failed"
fi

sed -i -r 's/xid=[0-9]+/xid=XIDNUM/g' $FILE
if [ $? -ne 0 ]; then
    echo "remove xid failed"
fi
