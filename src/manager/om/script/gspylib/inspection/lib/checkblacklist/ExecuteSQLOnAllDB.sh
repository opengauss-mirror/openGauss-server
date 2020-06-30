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
# ExecuteSQLOnAllDB.sh
#
# IDENTIFICATION
#    src/manager/om/script/gspylib/inspection/lib/checkblacklist/ExecuteSQLOnAllDB.sh
#
#-------------------------------------------------------------------------

GSPORT=''
SQLFILE=''

function usage()
{
	echo "***********************************************************************"
	echo "*                   ExecuteSQLOnAllDB.sh usage                        *"
	echo "* two indispensable paramater as following                            *"
	echo "* -p: coordinator port number                                         *"
	echo "* -f: sql file to execute                                             *"
	echo "* example:  ./ExecuteSQLOnAllDB.sh -p 25308 -f blacklist_check.sql    *"
	echo "***********************************************************************"
}

function parse_para()
{
	while getopts "p:f:h" opt
	do
		case $opt in
			p)
				if [ -z $GSPORT ]; then
					let GSPORT=$OPTARG
				else
					echo "GSPORT: "$GSPORT
					echo "SQLFILE: "$SQLFILE
					echo "ERROR: duplicate port number"
					usage
					exit 1
				fi
				;;
			f)
				if [ -z $SQLFILE ]; then
					SQLFILE=$OPTARG
				else
					echo "GSPORT: "$GSPORT
					echo "SQLFILE: "$SQLFILE
					echo "ERROR: duplicate sql file"
					usage
					exit 1
				fi
				;;
			h)
				usage
				exit 1
				;;
			?)
				echo "ERROR: unkonw argument"
				echo "GSPORT: "$GSPORT
				echo "SQLFILE: "$SQLFILE
				usage
				exit 1
				;;
		esac
	done

	if [[ -z $SQLFILE || -z $GSPORT ]]; then
		echo "GSPORT: "$GSPORT
		echo "SQLFILE: "$SQLFILE
		echo "ERROR: must designate -p and -f"
		usage
		exit 1
	fi
}

parse_para $*
	
echo "GSPORT: "$GSPORT
echo "SQLFILE: "$SQLFILE

for db in $(gsql -d postgres -p $GSPORT -c "select datname||'  GAUSSDB' from pg_database where datname != 'template0'" | grep GAUSSDB | awk '{print $1}')
do
	echo "****************************************Blacklist Check for DataBase: "$db"**************************************************"
        if [ ! -d ./log ]; then mkdir ./log ;fi
        sql_log=$(pwd)/log/sqlLog.log
        gsql -d $db -p $GSPORT -r -P pager=off -f $SQLFILE >$sql_log
        cat $sql_log
done

