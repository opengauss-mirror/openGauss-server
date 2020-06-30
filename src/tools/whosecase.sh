#!/bin/bash


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
