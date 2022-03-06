#!/bin/bash
# Copyright (c) Huawei Technologies Co., Ltd. 2010-2022. All rights reserved.
# date: 2020-12-22
# version: 1.0

SRC_CODE="here will be change by cmake"
TO_DEST="here will be change by cmake"

DO_CMD=$1

function build_timezone()
{
    export LD_LIBRARY_PATH=$1:$LD_LIBRARY_PATH
    echo LD_LIBRARY_PATH:${LD_LIBRARY_PATH}
    ZIC_CMD="${CMAKE_BUILD_PATH}/zic"
    TIMEZONE_DATA_PATH=${TRUNK_CODE_PATH}
    $($CMAKE_BUILD_PATH/zic -d $CMAKE_BUILD_PATH/timezone -p 'US/Eastern' $TIMEZONE_DATA_PATH/africa $TIMEZONE_DATA_PATH/antarctica $TIMEZONE_DATA_PATH/asia $TIMEZONE_DATA_PATH/australasia $TIMEZONE_DATA_PATH/europe $TIMEZONE_DATA_PATH/northamerica $TIMEZONE_DATA_PATH/southamerica $TIMEZONE_DATA_PATH/pacificnew $TIMEZONE_DATA_PATH/etcetera $TIMEZONE_DATA_PATH/factory $TIMEZONE_DATA_PATH/backward $TIMEZONE_DATA_PATH/systemv $TIMEZONE_DATA_PATH/solar87 $TIMEZONE_DATA_PATH/solar88 $TIMEZONE_DATA_PATH/solar89)
}

function export_ldpath()
{
    export LD_LIBRARY_PATH=$2:$LD_LIBRARY_PATH
}


function create_conversionfile()
{
    sqlin=$(cat ${OPENGS_TOP_PATH}/src/common/backend/utils/mb/conversion_procs/conversion_create.sql.in)
    set $sqlin
    while [ "$#" -gt 0 ] ;
    do
        name=$1;shift;
        se=$1;shift;
        de=$1; shift;
        func=$1; shift;
        obj=$1; shift;
        echo "-- $se --> $de";
        echo "CREATE OR REPLACE FUNCTION $func (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '\$libdir/$obj', '$func' LANGUAGE C STRICT NOT FENCED;";
        echo "COMMENT ON FUNCTION $func(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for $se to $de';";
        echo "DROP CONVERSION pg_catalog.$name;";
        echo "CREATE DEFAULT CONVERSION pg_catalog.$name FOR '$se' TO '$de' FROM $func;";
        echo "COMMENT ON CONVERSION pg_catalog.$name IS 'conversion for $se to $de';";
    done > $CMAKE_BUILD_PATH/conversion_create.sql
}

function create_snowballfile()
{
    sqlin=$(cat ${OPENGS_TOP_PATH}/src/common/backend/snowball/snowball.sql.in)
    echo '-- Language-specific snowball dictionaries' > $CMAKE_BUILD_PATH/snowball_create.sql
    cat ${OPENGS_TOP_PATH}/src/common/backend/snowball/snowball_func.sql.in >> $CMAKE_BUILD_PATH/snowball_create.sql
    LANGUAGES="
        danish danish
        dutch dutch
        english english
        finnish finnish
        french french
        german german
        hungarian hungarian
        italian italian
        norwegian norwegian
        portuguese portuguese
        romanian romanian
        russian english
        spanish spanish 
        swedish swedish 
        turkish turkish
    "

    set $LANGUAGES
    while [ "$#" -gt 0 ] ;
    do
        lang=$1; shift;
        nonascdictname=$lang;
        ascdictname=$1; shift;
        if [ -s "${OPENGS_TOP_PATH}/src/common/backend/snowball/stopwords/${lang}.stop" ] ; then
            stop=", StopWords=${lang}" ;
        else
            stop="";
        fi;
        echo "$sqlin" |
            sed -e "s#_LANGNAME_#$lang#g" |
            sed -e "s#_DICTNAME_#${lang}_stem#g" |
            sed -e "s#_CFGNAME_#${lang}#g" |
            sed -e "s#_ASCDICTNAME_#${ascdictname}_stem#g" |
            sed -e "s#_NONASCDICTNAME_#${nonascdictname}_stem#g" |
            sed -e "s#_STOPWORDS_#${stop}#g" ;
    done >> $CMAKE_BUILD_PATH/snowball_create.sql
}

function runscript()
{
    if [ ! "X$1" = "X" ]; then
        eval $1
        echo "$1" >> $CMAKE_BUILD_PATH/runscript.log
    fi
    if [ ! "X$2" = "X" ]; then
        eval $2
        echo "$2" >> $CMAKE_BUILD_PATH/runscript.log
    fi
    if [ ! "X$3" = "X" ]; then
        eval $3
        echo "$3" >> $CMAKE_BUILD_PATH/runscript.log
    fi
    if [ ! "X$4" = "X" ]; then
        eval $4
        echo "$4" >> $CMAKE_BUILD_PATH/runscript.log
    fi
}

function get_gs_version()
{
    cd $1
    csv_version=$(git log | grep commit | head -1 | awk '{print $2}' | cut -b 1-8)
    commits=$(git log | grep "See merge request" | wc -l)
    mrid=$(git log | grep "See merge request" | head -1 | awk -F! '{print $2}' | grep -o '[0-9]\+')
    debug_str="$DEBUG_TYPE"
    product=$(cat build/script/gaussdb.ver | grep 'PRODUCT' | awk -F "=" '{print $2}')
    version=$(cat build/script/gaussdb.ver | grep 'VERSION' | awk -F "=" '{print $2}')
    if test "$enable_ccache" = yes; then
        default_gs_version="(${product} ${version} build 1f1f1f1f) compiled at 2100-00-00 00:00:00 commit 9999 last mr 9999 debug"
    else
        date_time=$(date -d today +"%Y-%m-%d %H:%M:%S")
        default_gs_version="(${product} ${version} build ${csv_version}) compiled at $date_time commit $commits last mr $mrid $debug_str"
    fi
    printf "${default_gs_version}"
}

function get_time_for_roach()
{
    tmp=$(date +'%d %b %Y %H:%M:%S')
    printf "${tmp}"
}

#These only used for *check cmd.
TRUNK_CODE_PATH=$2
CMAKE_BUILD_PATH=$3
INSTALLED_PATH=$4
ENABLE_MEMORY_CHECK=""
MAKE_PROGRAM=$6

OPENGS_TOP_PATH=${TRUNK_CODE_PATH}/${openGauss}

case "${DO_CMD}" in
    --build_timezone|build_timezone)
        build_timezone $5 ;;
    --export_ldpath|export_ldpath)
        export_ldpath $2;;
    --create_conversionfile|conversion)
        create_conversionfile ;;
    --create_snowballfile|snowball)
        create_snowballfile ;;
    --get_gs_versionstr|--s)
        get_gs_version "$2";;
    --get_time_for_roach)
        get_time_for_roach ;;
    --runscript*|runscript)
        runscript "$4" "$5" "$6" "$7" ;;
    --build-package*|package*|--p)
        cd ${TRUNK_CODE_PATH}/Build/Script && ./mpp_package_cmake.sh -i ${INSTALLED_PATH} -m;;

    *)
        echo "no $1: after you add cmd in cmake, then add response function in buildfunction.sh" ;;
esac

