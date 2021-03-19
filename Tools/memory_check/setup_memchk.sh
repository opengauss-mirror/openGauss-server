#!/bin/bash
# ***********************************************************************
# Copyright: (c) Huawei Technologies Co., Ltd. 2019. All rights reserved.
# script for memcheck setup 
# version: 1.0.0
# change log:
# ***********************************************************************
set -e

SUFFIX="$(date +%Y-%m-%d-%H-%M-%S | tr -d '\n\r')"
CWD="$(pwd | tr -d '\n\r')"

alter_env_file=""
env_file="$HOME/.bashrc"
os_ver="Suse11SP1"

user="$(whoami | tr -d '\r\n')"

# commence/restore
action="commence"

# jeprof/asan
mode="asan"

version="V100R008C10"

revision=""
jeprofpath=""

mem_check_dir="$(dirname $0)"
TOP_DIR="$(cd $mem_check_dir/../../; pwd)"

INST_DIR=$TOP_DIR/mppdb_temp_install
if [ ! -z $GAUSSHOME ]; then
    INST_DIR=$GAUSSHOME
fi

PORT=22200
if [ ! -z $LLTPORT ]; then
    PORT=$LLTPORT
fi

gcc_version="5.4"

help()
{
    echo "$0 [-t action|-m mode|-u user|-f user-profile|-v version|-r revision|-p jeprof] [/path/to/gaussdb/package]"
    echo " "
    echo "   -t action			       setup or restore memory check and start cluster for you"
    echo "      action can be:"
    echo "          commence           set up memory check tools and start cluster, and then you can run jobs"
    echo "          restore            restore cluster to the state before you set up mem check tools"
    echo "          llt-mem-check      1) run <Gauss200-repo-top-dir>/Code/configure with '--enable-memory-check'"
    echo "                             2) make && make install"
    echo "                             3) make fastcheck"
    echo "          gen-report         generate report for memory check of mode 'asan' or 'jeprof'"
    echo "   -m mode                   what kind of mem check you will do"
    echo "      mode can be:"
    echo "          jeprof             enaable jemallc profiling and start cluster, and then you can run jobs"
    echo "          asan               enaable address sanitizer and start cluster"
    echo "   -u user                   current user name"
    echo "   -v version                current mppdb version, e.g. V100R007C10"
    echo "   -r revision               current mppdb revision, e.g. a4a4edc7"
    echo "   -f user-profile           alternative user profile, e.g. /opt/huawei/Bigdata/mppdb/.mppdb_profile"

    echo "   -p jeprof                 jeprof file path"
    echo " "

    echo "Example:"
    echo " "
}

# environ variables
setup_environ()
{
    # SUSE
    if [ $(cat /etc/issue | grep 'SUSE' | wc -l) -ge 1 ]; then

        export SUSE11_HOME="$(cd ../buildtools/suse11_sp1_x86_64/; pwd)"
        export CC=$SUSE11_HOME/gcc$gcc_version/gcc/bin/gcc
        export CXX=$SUSE11_HOME/gcc$gcc_version/gcc/bin/g++
        export JAVA_HOME=$SUSE11_HOME/jdk8/jdk1.8.0_77
        export JRE_HOME=$JAVA_HOME/jre
        export PATH=$JAVA_HOME/bin:$JRE_HOME/bin:$PATH
        LD_LIBRARY_PATH=$JRE_HOME/lib/amd64/server:$LD_LIBRARY_PATH
        LD_LIBRARY_PATH=$SUSE11_HOME/gcc$gcc_version/gcc/lib64:$SUSE11_HOME/gcc$gcc_version/isl/lib:$LD_LIBRARY_PATH
        export LD_LIBRARY_PATH=$SUSE11_HOME/gcc$gcc_version/mpc/lib:$SUSE11_HOME/gcc$gcc_version/mpfr/lib/:$SUSE11_HOME/gcc$gcc_version/gmp/lib:$LD_LIBRARY_PATH
        export PATH=$SUSE11_HOME/gcc$gcc_version/gcc/bin:$PATH

    # Redhat
    elif [ $(cat /etc/issue | grep 'Red Hat'|wc -l) -ge 1 ]; then

        export RHEL64_HOME=$(cd ../buildtools/redhat6.4_x86_64/; pwd)
        export CC=$RHEL64_HOME/gcc$gcc_version/gcc/bin/gcc
        export CXX=$RHEL64_HOME/gcc$gcc_version/gcc/bin/g++
        export JAVA_HOME=$RHEL64_HOME/jdk8/jdk1.8.0_77
        export JRE_HOME=$JAVA_HOME/jre
        export PATH=$JAVA_HOME/bin:$JRE_HOME/bin:$PATH
        LD_LIBRARY_PATH=$JRE_HOME/lib/amd64/server:$LD_LIBRARY_PATH
        LD_LIBRARY_PATH=$RHEL64_HOME/gcc$gcc_version/gcc/lib64:$RHEL64_HOME/gcc$gcc_version/isl/lib:$LD_LIBRARY_PATH
        export LD_LIBRARY_PATH=$RHEL64_HOME/gcc$gcc_version/mpc/lib:$RHEL64_HOME/gcc$gcc_version/mpfr/lib/:$RHEL64_HOME/gcc$gcc_version/gmp/lib:$LD_LIBRARY_PATH
        export PATH=$RHEL64_HOME/gcc$gcc_version/gcc/bin:$PATH

    fi

    # to suppress some address-sanitizer false negative errors
    ulimit -v unlimited
}


check_cluster()
{
    i=0
    while [ 1 ]; do
        sta=''
        cm_ctl query | grep 'cluster_state' | grep 'Normal' > /dev/null 2>&1
        if [ $? -eq 0 ]; then
            sta="Y$sta"
        fi

        cm_ctl query | grep 'balanced' | grep 'Yes' > /dev/null 2>&1
        if [ $? -eq 0 ]; then
            sta="Y$sta"
        else
            #cluster_state: Normal
            if [ ! -z $sta ] && [ $sta = "Y" ]; then
                cm_ctl switchover -a
            fi 
        fi

        chk_code='passed'
        if [ -z "$sta" ] || [ "$sta" != "YY" ]; then
            chk_code='failed'
        else
            break
        fi

        # wait for 5 minutes if 'failed'
        if [ $i -gt 30 ]; then 
            echo "####### $(date): Cluster Status = $chk_code; waiting ... #######"
            sleep 10 
        else
            break
        fi

        i=$(expr $i + 1)
    done

    echo "####### $(date): Cluster Status = $chk_code ########"
}


get_package()
{
	if [ -z $version ]; then
		echo "No db version is provided."
	elif [ $version = "V100R007C00" ]; then
		ftpurl="ftp://mpp:mpp@10.180.56.168:16162/V1R7TrunkC00_packges/${os_ver}"
	elif [ $version = "V100R007C10" ]; then
		ftpurl="ftp://mpp:mpp@10.180.56.168:16162/V1R7TrunkC10_packges/${os_ver}"
	elif [ $version = "V100R008C00" ]; then
		ftpurl="ftp://mpp:mpp@10.180.56.168:16162/V1R8TrunkC00_packges/${os_ver}"
	else
		echo "Current version $version is not supported yet."
		exit 1
	fi
	echo "ftp: $ftpurl"

    latest_pkg=""
ftp $ftpurl/ <<_EOF1_  > $CWD/package.list.$$
ls -lrt
_EOF1_

    if [ -z $revision ]; then
        # get latest package, e.g. "Gauss-MPPDB-ALL-PACKAGES-20161221175107-SVN7610"
        latest_pkg=$(cat $CWD/package.list.$$ | grep 'Gauss-MPPDB-ALL-PACKAGES' | tail -n 1 | awk '{print $NF}')
    else
        # get chosen package, e.g. "Gauss-MPPDB-ALL-PACKAGES-20161221175107-SVN7610"
        latest_pkg=$(cat $CWD/package.list.$$ | grep "$revision" | awk '{print $NF}')
    fi
	rm -f $CWD/package.list.$$
    if [ -z "$latest_pkg" ]; then
        echo "Cannot find proper package."
		exit 1
    fi

	revision1=$(echo $latest_pkg | awk 'BEGIN{FS="-"}{print $7}')
	if [ ! -z $revision ] && [ $revision != $revision1 ]; then
		echo "Cannot get proper version $revision($revision1)"
		exit 1
	fi

	revision=$revision1
	echo $revision
	test -d  $HOME/memchk/$revision || mkdir $HOME/memchk/$revision
	cd $HOME/memchk/$revision

    for t in debug memcheck; do
        test -d $t || mkdir $t
        cd $t

        echo "ftpurl: $ftpurl/$latest_pkg/$t/"
        ftp $ftpurl/$latest_pkg/$t/ <<_EOF2_
get Gauss200_OLAP_${version}_PACKAGES.tar.gz
_EOF2_

        echo "Got $latest_pkg."

        tar -xzf Gauss200_OLAP_${version}_PACKAGES.tar.gz
        tar -xzvf Gauss200-OLAP-${version}*-64bit.tar.gz

        mkdir gaussdb/
        cd gaussdb
        ../Gauss200-OLAP-${version}*-64bit.bin

        cd ../..
    done

    # debug version is also downloaded and extracted
	inst_pkg_dir=$HOME/memchk/$revision/memcheck/gaussdb

	cd $CWD
}

backup_binaries()
{
    for d in bin lib; do
        if [ ! -d $GAUSSHOME/$d.orig ]; then
            mkdir $GAUSSHOME/$d.orig
            echo "cp -fr $GAUSSHOME/$d/* $GAUSSHOME/$d.orig"
            cp -fr $GAUSSHOME/$d/* $GAUSSHOME/$d.orig
        else
            mkdir $GAUSSHOME/$d.$SUFFIX
            echo "cp -fr $GAUSSHOME/$d/* $GAUSSHOME/$d.$SUFFIX"
            cp -fr $GAUSSHOME/$d/* $GAUSSHOME/$d.$SUFFIX
        fi
    done
}

replace_binaries()
{
    echo "cp -f $inst_pkg_dir/bin/gaussdb $GAUSSHOME/bin/gaussdb"
    cp -f $inst_pkg_dir/bin/gaussdb $GAUSSHOME/bin/gaussdb
}

restore_binaries()
{
    echo "cp -f $GAUSSHOME/bin.orig/gaussdb $GAUSSHOME/bin/gaussdb"
    cp -f $GAUSSHOME/bin.orig/gaussdb $GAUSSHOME/bin/gaussdb
}


restart_om_monitor()
{
    ps -ef | grep -w 'om_monitor' | grep -w $user | grep -v grep
    ps -ef | grep -w 'om_monitor' | grep -w $user | grep -v grep | awk '{print "kill -9", $2}' | bash

    ps -ef | grep -w 'cm_agent' | grep -w $user | grep -v grep
    ps -ef | grep -w 'cm_agent' | grep -w $user | grep -v grep | awk '{print "kill -9", $2}' | bash
}

setup_jeprof()
{
    cm_ctl stop -mi
    backup_binaries
    replace_binaries

    echo "Fixing $env_file..."
    cp $env_file $env_file.$SUFFIX
    awk '!/export MALLOC_CONF/{print $0} /export MALLOC_CONF/{}' $env_file.$SUFFIX > $env_file
    echo "export MALLOC_CONF='prof:true,prof_final:false,prof_gdump:true,lg_prof_sample:20'" >> $env_file

    if [ ! -z $alter_env_file ]; then
        echo "Fixing $alter_env_file..."
        cp $alter_env_file $alter_env_file.$SUFFIX
        awk '!/export MALLOC_CONF/{print $0} /export MALLOC_CONF/{}' $alter_env_file.$SUFFIX > $alter_env_file
        echo "export MALLOC_CONF='prof:true,prof_final:false,prof_gdump:true,lg_prof_sample:20'" >> $alter_env_file
    fi

    restart_om_monitor

    cm_ctl start

    check_cluster
}

setup_asan()
{
    cm_ctl stop -mi
    backup_binaries
    replace_binaries

    test -d $HOME/memchk/asan || mkdir -p $HOME/memchk/asan

    echo "Fixing $env_file..."
    cp $env_file $env_file.$SUFFIX
    awk '!/export [AL]SAN_OPTIONS/{print $0} /export [AL]SAN_OPTIONS/{}' $env_file.$SUFFIX > $env_file
    echo "export ASAN_OPTIONS='detect_leaks=1:halt_on_error=0:alloc_dealloc_mismatch=0:log_path=$HOME/memchk/asan/runlog'" >> $env_file
    if [ -f $HOME/.memleak_ignore ]; then
        lopt="suppressions=$TOP_DIR/Tools/memory_check/memleak_ignore"
    fi
    echo "export LSAN_OPTIONS='exitcode=0:$lopt'" >> $env_file

    if [ ! -z $alter_env_file ]; then
        echo "Fixing $alter_env_file..."
        cp $alter_env_file $alter_env_file.$SUFFIX
        awk '!/export [AL]SAN_OPTIONS/{print $0} /export [AL]SAN_OPTIONS/{}' $alter_env_file.$SUFFIX > $alter_env_file
        echo "export ASAN_OPTIONS='detect_leaks=1:halt_on_error=0:alloc_dealloc_mismatch=0:log_path=$HOME/memchk/asan/runlog'" >> $alter_env_file
        lopt=""
        if [ -f $TOP_DIR/Tools/memory_check/memleak_ignore ]; then
            lopt="suppressions=$TOP_DIR/Tools/memory_check/memleak_ignore"
        fi
        echo "export LSAN_OPTIONS='exitcode=0:$lopt'" >> $alter_env_file
    fi

    restart_om_monitor

    cm_ctl start

    check_cluster
}

restore_jeprof()
{
    cm_ctl stop -mi
    restore_binaries

    cp $env_file $env_file.$SUFFIX
    awk '!/export MALLOC_CONF/{print $0} /export MALLOC_CONF/{}' $env_file.$SUFFIX > $env_file

    if [ ! -z $alter_env_file ]; then
        cp $alter_env_file $alter_env_file.$SUFFIX
        awk '!/export MALLOC_CONF/{print $0} /export MALLOC_CONF/{}' $alter_env_file.$SUFFIX > $alter_env_file
    fi

    restart_om_monitor

    cm_ctl start

    check_cluster
}

genreport_jeprof()
{
	if [ -z $jeprofpath ]; then
		echo "You need provide jeprof file path by '-p'."
		help 
		exit 1
	fi

	jeprofdir=$(dirname $jeprofpath)
	jeproffile=$(basename $jeprofpath)
	if [ ${jeproffile: -5} = ".heap" ]; then
		jeproffile=${jeproffile%.heap}
	fi 

	mkdir jeprof-report-$SUFFIX
	for f in $(ls $jeprofdir/${jeproffile}*.heap); do
		fn=$(basename $f)
	 	pprof --show_bytes --pdf $GAUSSHOME/bin/gaussdb  $f > jeprof-report-$SUFFIX/$fn.pdf
	done

}

restore_asan()
{
    cm_ctl stop -mi
    restore_binaries

    cp $env_file $env_file.$SUFFIX
    awk '!/export ASAN_OPTIONS/{print $0} /export ASAN_OPTIONS/{print ""}' $env_file.$SUFFIX > $env_file

    if [ ! -z $alter_env_file ]; then
        cp $alter_env_file $alter_env_file.$SUFFIX
        awk '!/export ASAN_OPTIONS/{print $0} /export ASAN_OPTIONS/{print ""}' $alter_env_file.$SUFFIX > $alter_env_file
    fi

    restart_om_monitor

    cm_ctl start

    check_cluster
}

genreport_asan()
{
    out_file="$TOP_DIR/Tools/memory_check/$(date +%Y-%m-%d-%H-%M-%S)"
    perl $TOP_DIR/Tools/memory_check/asan_report.pl --asanlog-dir $HOME/memchk/asan --output $out_file

    echo "Please check:"
    echo "$(ls ${out_file}*)"
}

while getopts f:m:p:r:t:u:v:h option
do
    case "${option}" in
        f)
            alter_env_file=${OPTARG} 
        ;;

        m)
            mode=${OPTARG} 
        ;;

        p)
            jeprofpath=${OPTARG}
        ;;

        r)
            revision=${OPTARG}
        ;;

        t)
            action=${OPTARG}
        ;;

        u)
            user=${OPTARG}
        ;;

        v)
            version=${OPTARG}
        ;;

        h)
            help
            exit 0
        ;;

        -)
            case "${OPTARG}" in
                gcc)
                    val="${!OPTIND}"; OPTIND=$(( $OPTIND + 1 ))
                    gcc_version=$val
                    ;;
                gcc=*)
                    val=${OPTARG#*=}
                    opt=${OPTARG%=$val}
                    gcc_version=$val
                    ;;
                *)
                    echo "Unknown option --${OPTARG}"
                    ;;
            esac
            ;;
 
        *) 
            echo "The option can only be '-f', '-t', '-u', '-f', or '-h'."
            echo " "
            help 
            exit 1
        ;;
    esac
done

if [ ${gcc_version:0:3} == "5.4" ];then
    gcc_version="5.4"
elif [ ${gcc_version:0:3} == "6.1" ];then
    gcc_version="6.1"
else
    echo "Unknown gcc version $gcc_version"
    exit 0
fi

if [ $mode != "asan" ] && [ $mode != "jeprof" ]; then
    echo "ERROR: mode ($mode) is not supported."
    echo " "
    help
    exit 1
fi
if [ $action != "commence" ] && [ $action != "restore" ] && [ $action != "gen-report" ] && [ $action != "llt-mem-check" ]; then
    echo "ERROR: action ($action) is not supported."
    echo " "
    help
    exit 1
fi

shift $((OPTIND -1))

inst_pkg_dir=$1

if [ $action = "commence" ]; then
    # -- get package if necessary
    if [ -z $inst_pkg_dir ]; then
        get_package
        # inst_pkg_dir is setup in above get_package function, if everything works well
    fi
    if [ -z $inst_pkg_dir ]; then
        echo "Cannot get proper package for memory check."
        exit 0
    fi

    if [ $mode = "asan" ]; then
        setup_asan
    elif [ $mode = "jeprof" ]; then
        setup_jeprof
    fi

elif [ $action = "restore" ]; then
    if [ $mode = "asan" ]; then
        restore_asan
    elif [ $mode = "jeprof" ]; then
        restore_jeprof
    fi

elif [ $action = "llt-mem-check" ]; then
    cd $TOP_DIR/Code/
    setup_environ
    make distclean -sj
    ./configure --gcc-version=${gcc_version}.0 --prefix="${INST_DIR}" CFLAGS='-O0 -g' --enable-debug --enable-cassert --enable-thread-safety --without-readline --without-zlib --enable-memory-check CC=g++
    make -sj > /dev/null || make -sj > /dev/null
    make fastcheck p=$PORT -sj

elif [ $action = "gen-report" ]; then
    if [ $mode = "jeprof" ]; then
        genreport_jeprof
    elif [ $mode = "asan" ]; then
        genreport_asan
    fi
fi
