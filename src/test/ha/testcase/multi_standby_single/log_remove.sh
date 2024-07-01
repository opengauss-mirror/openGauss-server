#!/bin/sh

:<<!
 This use case is used to check the cleanup function of the service-side
 tool logs, with a maximum of 50 files retained and a maximum size of 16MB
 per file. It is not only applicable to gs_ctl, but also to other tools
 such as gs_restore, gs_dumpall, gs_guc, gs_dump, gs_clean, or gs_cgroup.
!

source ./util.sh

function gs_ctl_log_remove()
{
    #1. set GAUSSLOG echonvironment variable
    export GAUSSLOG=$GAUSSHOME/logs

    #2. query database
    gs_ctl query -D $primary_data_dir
    if [ "$?" -ne 0 ]; then
        echo "gs_ctl query error: $failed_keyword"
    fi

    #3. check the log count
    count=`ls -l $GAUSSLOG/bin/gs_ctl | grep log | wc -l`
    if [ $count -ne 1 ]; then
        echo "gs_ctl create logfile error: $failed_keyword"
    fi

    #4. set current file full
    curr=`ls -l $GAUSSLOG/bin/gs_ctl | grep current | awk '{print $9}'`
    dd if=/dev/zero of=$GAUSSLOG/bin/gs_ctl/$curr bs=16M count=1

    #5. create 50 log files
    date=`echo $(date "+%Y-%m-%d")`
    for i in `seq 1 50`
    do
        time=`echo $i | awk '{printf("%06d\n", $0)}'`
        name="gs_ctl-""$date"_"$time".log
        dd if=/dev/zero of=$GAUSSLOG/bin/gs_ctl/$name bs=16M count=1
    done

    #6. query database again
    gs_ctl query -D $primary_data_dir
    if [ "$?" -ne 0 ]; then
        echo "gs_ctl query error: $failed_keyword"
    fi

    #7. check the remain log count
    count=`ls -l $GAUSSLOG/bin/gs_ctl | grep log | wc -l`
    if [ $count -ne 50 ]; then
        echo "gs_ctl remove log error: $failed_keyword"
    fi

    #8. query concurrently, to trigger gs_ctl.lock fail
    for i in `seq 1 10`
    do
        gs_ctl query -D $primary_data_dir &
    done
}

function tear_down()
{
    tms=0
    #1. wait all gs_ctl query done
    while true
    do
        count=`ps ux | grep "gs_ctl query" | grep -v grep | wc -l`
        if [ $count -ne 0 -a $tms -lt 30 ]; then
            let $tms+1
            sleep 1
        else
            break
        fi
    done

    #2. remove remain logfile
    rm -rf $GAUSSLOG/bin
    if [ "$?" -ne 0 ]; then
        echo "remove file error: $failed_keyword"
    fi
}

gs_ctl_log_remove
tear_down
