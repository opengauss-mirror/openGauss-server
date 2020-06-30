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
# looptest.sh
#
# IDENTIFICATION
#    src/bin/scripts/looptest.sh
#
#-------------------------------------------------------------------------

set -e

function cleanptipc()
{
  aas=$(ipcs -s | grep ^0x | awk '{ print $2 }')
  aam=$(ipcs -m | grep ^0x | awk '{ print $2 }')

  for bb in $aas
  do
    ipcrm -s $bb
  done

  for bb in $aam
  do
    ipcrm -m $bb
  done

  ipcs
}

function dumpgstack()
{
  gtmproc=$(ps -f -u $currentuser | grep gs_gtm | grep tmp_check | grep -v grep | awk '{ print $2 }')
  gaussdbproc=$(ps -f -u $currentuser | grep gaussdb | grep tmp_check | grep -v grep | awk '{ print $2 }')
  DT=$(date +%F_%T | sed 's/-//g' | sed 's/://g')

  ps -ef | grep ^$currentuser | grep -P "(gs_gtm|gaussdb)" > $logloc/allstack$DT
  gstack $gtmproc >> allstack$DT
  for bb in $gaussdbproc
  do
    gstack $bb >> $logloc/allstack$DT
  done
}

typeset -i waitcount=20
typeset -i runningproc=1
typeset -i count=0

typeset -i totalcount=0
typeset -i killflag=0

currentuser=$(whoami)

startts=$(date +%F_%T | sed 's/-//g' | sed 's/://g')

logloc=$PWD/loopTest.log
logfile=$PWD/loopTest.log/loopTest$startts

mkdir -p $PWD/loopTest.log

cleanptipc

if [ -f $logloc ]
then
  rm -f $logloc
fi

while [ 1 ]
do
  count=0
  runningproc=1

  totalcount=${totalcount}+1
  echo "test started at $(date +%F:%T)" >> $logfile

  make fastcheck p=12121 &
  sleep 300

  while [ $runningproc -gt 0 ]
  do
    runningproc=$(ps -f -u $currentuser | grep tmp_check | grep gaussdb | grep -v grep | wc -l)

    echo $count
    if [ $count -gt $waitcount ]
    then
       dumpgstack
       echo "***** Test $totalcount has been killed at $(date +%F:%T)" >> $logfile
       echo ""
       killall -9 gaussdb gs_gtm pg_regress
       killflag=1
       sleep 30
       runningproc=$(ps -f -u $currentuser | grep tmp_check | grep gaussdb | grep -v grep | wc -l)
    else
      sleep 10
      count=${count}+1
    fi
  done

  if [ $killflag -ne 0 ]
  then
    killflag=0
  else
     echo "Test $totalcount finished at $(date +%F:%T)" >> $logfile
     echo ""
  fi

  sleep 3
done

