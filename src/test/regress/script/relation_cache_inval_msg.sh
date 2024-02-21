#!/bin/bash

type=$1
execfile=$2
port=$3
log=$4
temp=$5

file=$(realpath $0)

if [ "$type" = "start" ] ; then
    for ((i=1; i<=10; i++))
    do
        sh $file t_rcim1 $execfile $port $log &
    done
    for ((i=1; i<=5; i++))
    do
        sh $file spt_rcim1_add $execfile $port $log $i &
        sh $file spt_rcim1_insert $execfile $port $log &
    done
    wait
elif [ "$type" = "t_rcim1" ] ; then
    for ((i=1; i<=10; i++))
    do
        $execfile -p $port -d regression -c "insert into t_rcim1 values ($i, ('$i day') + '2000-1-1'::timestamp);" >> $log 2>&1
    done
    
elif [ "$type" = "spt_rcim1_add" ] ; then
    for ((i=1; i<=10; i++))
    do
        $execfile -p $port -d regression -c "alter table spt_rcim1 modify partition p1 add subpartition sp1${i}_${temp} values less than('$i');" >> $log 2>&1
    done
    
elif [ "$type" = "spt_rcim1_insert" ] ; then
    for ((i=1; i<=10; i++))
    do
        for ((j=1; j<=3; j++))
        do
            $execfile -p $port -d regression -c "insert into spt_rcim1 values ($i$j, '$i');" >> $log 2>&1
        done
    done

fi

exit 0
