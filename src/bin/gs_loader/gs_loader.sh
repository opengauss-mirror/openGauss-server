#!/usr/bin/env bash

#######################################################################
# Copyright (c): 2020-2025, Huawei Tech. Co., Ltd.
# descript: 
#     Load data into database
#     Return 0 means Load all rows OK.
#     Return 1 means Load failed.
#     Return 2 means Load all or some rows rejected or discarded.
# version:  0.1
# date:     2021-09-07
#######################################################################
set -e
gs_loader_version="gs_loader: version 0.1"

# env variables
gs_loader_log_level=${gs_loader_log_level:="warning"}
# gs_loader_log_fmt=${gs_loader_log_fmt:="%Y-%m-%d %H:%M:%S"}
gs_loader_log_fmt=${gs_loader_log_fmt:=""}

# user input parameters
cmd_param_host=""
cmd_param_port=""
cmd_param_user=""
cmd_param_passwd=""
cmd_param_db=""

cmd_param_create="true"
cmd_param_clean="false"

cmd_param_ctl=""
cmd_param_data=""
cmd_param_log=""
cmd_param_bad=""
cmd_param_discard=""
cmd_param_errors=""
cmd_param_skip=""

# global variables
rnd_suffix=$(dd if=/dev/urandom bs=128 count=1 2>/dev/null | tr -dc 'a-zA-Z0-9')
gs_loader_file_tmp=${gs_loader_file_tmp:=".gs_loader_file.tmp.${rnd_suffix}_end"}
gs_loader_table_name=""

EXIT_CODE_OK=0
EXIT_CODE_FAIL=1
EXIT_CODE_WARN=2
EXIT_CODE_FATAL=3

declare -A loader_datafiles
declare -A loader_badfiles
declare -A loader_discardfiles
declare -A loader_txids

loader_datafile_count=0
loader_datafile_index=0
loader_txids_count=0

function load_log()
{
    level=$1 && shift
    case "$level" in
        debug)
            if [[ "$gs_loader_log_level" =~ debug ]]; then
                echo -e "$( date "+${gs_loader_log_fmt}" )DEBUG: $@" 1>&2
                if [[ -f "$cmd_param_log" ]]; then
                    echo -e "$( date "+${gs_loader_log_fmt}" )INFO: $@" >> $cmd_param_log 2>&1
                fi
            fi
            ;;
        info)
            if [[ "$gs_loader_log_level" =~ debug|info ]]; then
                echo -e "$( date "+${gs_loader_log_fmt}" )INFO: $@" 1>&2
                if [[ -f "$cmd_param_log" ]]; then
                    echo -e "$( date "+${gs_loader_log_fmt}" )INFO: $@" >> $cmd_param_log 2>&1
                fi
            fi
            ;;
        warning)
            if [[ "$gs_loader_log_level" =~ debug|info|warn ]]; then
                echo -e "$( date "+${gs_loader_log_fmt}" )WARNING: $@" 1>&2
                if [[ -f "$cmd_param_log" ]]; then
                    echo -e "$( date "+${gs_loader_log_fmt}" )WARNING: $@" >> $cmd_param_log 2>&1
                fi
            fi
            ;;
        error)
            if [[ ! "$gs_loader_log_level" =~ none ]]; then
                echo -e "$( date "+${gs_loader_log_fmt}" )ERROR: $@" 1>&2
                if [[ -f "$cmd_param_log" ]]; then
                    echo -e "$( date "+${gs_loader_log_fmt}" )ERROR: $@" >> $cmd_param_log 2>&1
                fi
            fi
            ;;
    esac
    true
}

function gs_loader_check_res()
{
    msg="$1"
    res="$2"
    if [ $(echo "$res" | grep -i "ERROR:" | wc -l) -ge 1 ]; then
        load_log error "$msg"
        res=$(echo "$res"|sed 's/gsql:.gs_loader_file.tmp.*_end:/gsql:.gs_loader_file.tmp:/')
        load_log error "$res"
        exit $EXIT_CODE_FAIL
    fi
}

function gs_loader_check_res_with_clean_file()
{
    msg="$1"
    res="$2"
    if [ $(echo "$res" | grep -i "ERROR:" | wc -l) -ge 1 ]; then
        load_log error "$msg"
        res=$(echo "$res"|sed 's/gsql:.gs_loader_file.tmp.*_end:/gsql:.gs_loader_file.tmp:/')
        load_log error "$res"
        exit_with_clean_file
    fi
}
function gs_loader_check_res_with_clean_file_txid()
{
    msg="$1"
    res="$2"
    txid="$3"
    if [ $(echo "$res" | grep -i "ERROR:" | wc -l) -ge 1 ]; then
        load_log error "$msg"
        res=$(echo "$res"|sed 's/gsql:.gs_loader_file.tmp.*_end:/gsql:.gs_loader_file.tmp:/')
        load_log error "$res"
        clean_copy_table_record "$txid"
        exit_with_clean_file
    fi
}
function get_value()
{
    kv="$1"
    value=$(echo $kv | awk -F'=' '{print $2}')
    echo $value
}

function gen_load_options()
{
    datafile="$1"
    skip="$2"
    errors="$3"

    if [ "$datafile" == "" ]; then
        load_log error "data file is empty"
        exit $EXIT_CODE_FAIL
    fi

    if [ -d "$datafile" ]; then
        load_log error "data file $datafile is a directory"
        exit $EXIT_CODE_FAIL
    fi

    if [ ! -f "$datafile" ]; then
        load_log error "data file $datafile not found"
        exit $EXIT_CODE_FAIL
    fi
    
    if [ ! -r "$datafile" ]; then
        load_log error "data file $datafile Permission denied"
        exit $EXIT_CODE_FAIL
    fi

    load_log debug "generate OPTIONS"

    options="OPTIONS("

    options="${options}data='$datafile'"

    if [ "$skip" != "" ]; then
        options="${options},skip=$skip"
    fi

    if [ "$errors" != "" ]; then
        options="${options},errors=$errors"
    fi

    options="${options}) "
    echo $options
}

function load_data_sql()
{
    ctlfile="$1"
    res=$(cat ${ctlfile})
    # remove the options in control file
    res=$(echo -e "${res}" | sed -r 's/options *\(.*\)//ig')

    # remove the badfile in control file
    res=$(echo -e "${res}" | sed -r "s/badfile *'.*'//ig;s/badfile *\".*\"//ig")
    
    # remove the infile in control file
    res=$(echo -e "${res}" | sed -r "s/infile *'.*'//ig;s/infile *\".*\"//ig;s/infile *\* *\".*\"//ig;s/infile *\* *'.*'//ig")

    echo -e "$res"
}

function exec_sql()
{
    sql="$1"

    host=""
    if [ "$cmd_param_host" != "" ]; then
        host="-h $cmd_param_host"
    fi

    port=""
    if [ "$cmd_param_port" != "" ]; then
        port="-p $cmd_param_port"
    fi

    user=""
    if [ "$cmd_param_user" != "" ]; then
        user="-U $cmd_param_user"
    fi

    passwd=""
    if [ "$cmd_param_passwd" != "" ]; then
        passwd="-W $cmd_param_passwd"
    fi

    db=""
    if [ "$cmd_param_db" != "" ]; then
        db="-d $cmd_param_db"
    fi

    if res=$(gsql $host $port $user $passwd $db -t -c "$sql" 2>&1)
    then
        echo -e "$res" | sed -r 's/^Connect primary node.*//g' | sed -r 's/^total time.*//g'
    else
        echo "ERROR: function exec_sql"
    fi
}

function exec_sql_file()
{
    sql_file="$1"

    host=""
    if [ "$cmd_param_host" != "" ]; then
        host="-h $cmd_param_host"
    fi

    port=""
    if [ "$cmd_param_port" != "" ]; then
        port="-p $cmd_param_port"
    fi

    user=""
    if [ "$cmd_param_user" != "" ]; then
        user="-U $cmd_param_user"
    fi

    passwd=""
    if [ "$cmd_param_passwd" != "" ]; then
        passwd="-W $cmd_param_passwd"
    fi

    db=""
    if [ "$cmd_param_db" != "" ]; then
        db="-d $cmd_param_db"
    fi

    # delete last line: total time: 10ms
    if res=$(gsql $host $port $user $passwd $db -t -f "$sql_file" 2>&1)
    then
        echo -e "$res" | sed -r 's/^Connect primary node.*//g' | sed -r 's/^total time.*//g'
    else
        echo "ERROR: function exec_sql_file"
    fi
}

function trans_load_to_copy()
{
    load_sql="$1"
    copy_sql=$(exec_sql_file "$load_sql")
    echo "$copy_sql"
}

function copy_into_table()
{
    sql_file="$1"
    res=$(exec_sql_file "$sql_file")
    echo "$res"
}

function check_legal_path()
{
    file_path="$1"
    if touch $file_path 1>/dev/null 2>&1
    then
        echo "0"
    else 
        echo "1"
    fi
}

function init_logfile()
{
    logfile="$1"

    if [ "$logfile" == "" ]; then
        logfile=$(echo "${cmd_param_ctl%.*}")
        logfile="${logfile}.log"
        cmd_param_log=$logfile
    fi
    res=$(check_legal_path $logfile)
    if [ "$res" == "0" ]; then
        echo $gs_loader_version > $logfile
    else
        load_log warning "logfile file path $logfile is invalid or don't have permission"
        exit $EXIT_CODE_FAIL
    fi
}

function init_badfiles()
{
    badfile="$1"

    i=0

    # no bad file, generate bad filename according to data file
    if [ "$badfile" == "" ]; then
        for data in "${loader_datafiles[@]}"
        do
            bad=$(echo "${data%.*}")
            bad="${bad}.bad"
            loader_badfiles[$i]=$bad
            i=$(($i+1))
        done
        return 0
    fi

    # bad file is a directory, generate bad filename according to data file
    if [ -d "$badfile" ]; then
        for data in "${loader_datafiles[@]}"
        do
            data=$(basename $data)
            bad=$(echo "${data%.*}")
            bad="$badfile/${bad}.bad"
            loader_badfiles[$i]=$bad
            i=$(($i+1))
        done
        return 0
    fi

    res=$(check_legal_path "$badfile")
    if [ "$res" != "0" ]; then
        load_log warning "bad file path $badfile is invalid"
        return 0
    fi

    i=0
    for data in "${loader_datafiles[@]}"
    do
        loader_badfiles[$i]=$badfile
        i=$(($i+1))
    done
}

function init_discardfiles()
{
    discardfile="$1"

    i=0

    if [ "$discardfile" == "" ]; then
        load_log info "discard file is empty"
        return 0
    fi

    if [ -d "$discardfile" ]; then
        i=0
        for data in "${loader_datafiles[@]}"
        do
            data=$(basename $data)
            discard=$(echo "${data%.*}")
            discard="$discardfile/${discard}.dsc"
            loader_discardfiles[$i]=$discard
            i=$(($i+1))
        done
        return 0
    fi

    res=$(check_legal_path "$discardfile")
    if [ "$res" != "0" ]; then
        load_log warning "discard file path $discardfile is invalid"
        return 0
    fi

    i=0
    for data in "${loader_datafiles[@]}"
    do
        loader_discardfiles[$i]=$discardfile
        i=$(($i+1))
    done
}

function get_txids_array()
{
    txids_array="("
    for ((i=0; i<loader_txids_count; i++)); do
        txids_array="${txids_array}${loader_txids[$i]}"

        last_idx=$(($loader_txids_count-1))
        if [ "$i" != "$last_idx" ]; then
            txids_array="${txids_array},"
        fi
    done
    txids_array="${txids_array})"
    echo $txids_array
}

function loader_stat_summary()
{
    logfile="$1"

    txids_array=$(get_txids_array)

    echo "" >> $logfile
    echo "Table $gs_loader_table_name:" >> $logfile

    sql="select pg_catalog.sum(loadrows)||' Rows successfully loaded.'  from gs_copy_summary where id in $txids_array"
    res=$(exec_sql "$sql")
    gs_loader_check_res "query gs_copy_summary failed: $sql" "$res"
    echo " "$res >> $logfile

    sql="select pg_catalog.sum(errorrows)||' Rows not loaded due to data errors.'  from gs_copy_summary where id in $txids_array"
    res=$(exec_sql "$sql")
    gs_loader_check_res "query gs_copy_summary failed: $sql" "$res"
    echo " "$res >> $logfile

    sql="select pg_catalog.sum(whenrows)||' Rows not loaded because all WHEN clauses were failed.'  from gs_copy_summary where id in $txids_array"
    res=$(exec_sql "$sql")
    gs_loader_check_res "query gs_copy_summary failed: $sql" "$res"
    echo " "$res >> $logfile

    sql="select pg_catalog.sum(allnullrows)||' Rows not loaded because all fields were null.'  from gs_copy_summary where id in $txids_array"
    res=$(exec_sql "$sql")
    gs_loader_check_res "query gs_copy_summary failed: $sql" "$res"
    echo " "$res >> $logfile

    echo "" >> $logfile

    sql="select 'Total logical records skipped:    ' || pg_catalog.sum(skiprows) from gs_copy_summary where id in $txids_array"
    res=$(exec_sql "$sql")
    gs_loader_check_res "query gs_copy_summary failed: $sql" "$res"
    echo " "$res >> $logfile

    echo "" >> $logfile

    if [[ "$gs_loader_log_level" =~ info|debug ]]; then
        printf '\nLOG:\n%b\n\n' "$(cat $logfile)"
    fi
}

function gen_badfile()
{
    badfile="$1"
    txid="$2"

    if [ "$badfile" == "" ]; then
        load_log info "bad file is empty"
        return 0
    fi

    condition=$(exec_sql "select ' relname = ''' || relname || ''' and begintime = '''|| begintime || '''' from gs_copy_summary where id=$txid")
    sql="select rawrecord from pgxc_copy_error_log where detail not like 'COPY_WHEN_ROWS' and $condition"
    res=$(exec_sql "$sql")
    gs_loader_check_res_with_clean_file_txid "query pgxc_copy_error_log failed: $sql" "$res" "$txid"
    printf '%b\n' "$res" > $badfile
    if [[ "$gs_loader_log_level" =~ info|debug ]]; then
        printf 'BAD:\n%b\n\n' "$(cat $badfile)"
    fi
}

function gen_discardfile()
{
    discard="$1"
    txid="$2"

    if [ "$discard" == "" ]; then
        load_log info "discard file is empty"
        return 0
    fi

    condition=$(exec_sql "select ' relname = ''' || relname || ''' and begintime = '''|| begintime || '''' from gs_copy_summary where id=$txid")
    sql="select rawrecord from pgxc_copy_error_log where detail like 'COPY_WHEN_ROWS' and $condition"
    res=$(exec_sql "$sql")
    gs_loader_check_res_with_clean_file_txid "query pgxc_copy_error_log COPY_WHEN_ROWS failed: $sql" "$res" "$txid"
    printf '%b\n' "$res" > $discard
    if [[ "$gs_loader_log_level" =~ info|debug ]]; then
        printf 'DISCARD:\n%b\n\n' "$(cat $discard)"
    fi
}

function gen_full_copy_sql()
{
    copy_sql="$1"

    table_act=$(echo "$copy_sql" | sed -r "s/ (SELECT 'has_data_in_table' FROM .*? LIMIT 1;) (.*)/\1/")
    pat="SELECT 'has_data_in_table' FROM .*? LIMIT 1;"
    if [[ "$table_act" =~ $pat ]]; then
        if [ "$loader_datafile_index" == 0 ]; then
            res=$(exec_sql "$table_act")
            gs_loader_check_res_with_clean_file "check table empty using: $table_act" "$res"
            load_log debug "$res"
            has_data_in_table=$(echo $res | grep 'has_data_in_table' | wc -l)
            if [ "$has_data_in_table" == "1" ]; then
                load_log error "insert into table, but table is not empty"
                exit_with_clean_file
            fi
        fi
        copy_sql=$(echo "$copy_sql" | sed -r "s/ (SELECT 'has_data_in_table' FROM .*? LIMIT 1;) (.*)/\2/")
    else
        table_act=$(echo "$copy_sql" | sed -r 's/ (TRUNCATE TABLE .*? ;) .*/\1/')
        pat="TRUNCATE TABLE .*? ;"
        if [[ "$table_act" =~ $pat ]]; then
            if [ "$loader_datafile_index" == 0 ]; then
                res=$(exec_sql "$table_act")
                gs_loader_check_res_with_clean_file "truncate table using: $table_act" "$res"
            fi
            copy_sql=$(echo "$copy_sql" | sed -r 's/ (TRUNCATE TABLE .*? ;) (.*)/\2/')
        fi
    fi

    echo "BEGIN;"
    echo "$copy_sql"
    echo "select 'copy_txid:'||pg_catalog.txid_current();"
    echo "COMMIT;"
}

function get_gs_loader_version()
{
    echo $gs_loader_version
}

function gs_loader_help()
{
    echo "$(get_gs_loader_version)

Usage: gs_loader key=value [key=value ...]

General options:
      help -- print this
      host -- database server host
        -h -- database server host
      port -- database server port
        -p -- database server port
      user -- database user name
        -U -- database user name
    passwd -- the password of specified database user
        -W -- the password of specified database user
        db -- database name to connect to
        -d -- database name to connect to

    create -- create error and summary tables(default:true)
     clean -- clean error and summary tables(default:true)

   control -- the name of control file
      data -- the name of data file
       log -- the name of log file
       bad -- the name of bad file
   discard -- the name of discard file
      skip -- skip line numbers
    errors -- allow errors numbers
      rows -- not support, only compatible
  bindsize -- not support, only compatible
"
}

declare -A unique_params

function check_param_exists()
{
    param_name="$1"
    if [[ ! -z "${unique_params[$param_name]:-}" ]]; then
        load_log error "parameter '$param_name' can only be specified once"
        exit $EXIT_CODE_FAIL
    fi
    unique_params[$param_name]="true"
}

function isValidPort()
{
    string=$1
    if [[ ! $string =~ ^[0-9]+$ ]]; then
        echo "ERROR: '"$string"' is not a valid port number"
        exit 1
    fi
}

function parse_cmd_params()
{
    while [[ $# -gt 0 ]]; do
        key="$1"
        case "$key" in
            host=*)
                check_param_exists "host"
                cmd_param_host=$(get_value $1)
                shift # past value
                ;;
            -h)
                check_param_exists "host"
                shift 
                cmd_param_host=$1
                shift 
                ;;
            port=*)
                check_param_exists "port"
                cmd_param_port=$(get_value $1)
                isValidPort $cmd_param_port
                shift # past value
                ;;
            -p)
                check_param_exists "port"
                shift 
                cmd_param_port=$1
                isValidPort $cmd_param_port
                shift 
                ;;
            user=*)
                check_param_exists "user"
                cmd_param_user=$(get_value $1)
                shift # past value
                ;;
            -U)
                check_param_exists "user"
                shift 
                cmd_param_user=$1
                shift 
                ;;
            db=*)
                check_param_exists "db"
                cmd_param_db=$(get_value $1)
                shift # past value
                ;;
            -d)
                check_param_exists "db"
                shift 
                cmd_param_db=$1
                shift 
                ;;
            create=*)
                check_param_exists "create"
                cmd_param_create=$(get_value $1)
                shift;
                ;;
            clean=*)
                check_param_exists "clean"
                cmd_param_clean=$(get_value $1)
                shift;
                ;;
            control=*)
                check_param_exists "control"
                cmd_param_ctl=$(get_value $1)
                shift # past value
                ;;
            log=*)
                check_param_exists "log"
                cmd_param_log=$(get_value $1)
                shift # past value
                ;;
            data=*)
                cmd_param_data=$(get_value $1)
                append_one_data "$cmd_param_data"
                shift # past value
                ;;
            bad=*)
                check_param_exists "bad"
                cmd_param_bad=$(get_value $1)
                shift # past value
                ;;
            discard=*)
                check_param_exists "discard"
                cmd_param_discard=$(get_value $1)
                shift # past value
                ;;
            skip=*)
                check_param_exists "skip"
                cmd_param_skip=$(get_value $1)

                # check skip is digit number
                re='^[0-9]+$'
                if ! [[ "$cmd_param_skip" =~ $re ]] ; then
                    load_log error "invalid param:$1"
                    exit $cmd_param_skip
                fi

                shift # past argument
                ;;
            errors=*)
                check_param_exists "errors"
                cmd_param_errors=$(get_value $1)

                # check errors is digit number
                re='^[0-9]+$'
                if ! [[ "$cmd_param_errors" =~ $re ]] ; then
                    load_log error "invalid param:$1"
                    exit $EXIT_CODE_FAIL
                fi

                shift # past argument
                ;;
            bindsize=*)
                check_param_exists "bindsize"
                shift # past argument
                ;;
            rows=*)
                check_param_exists "rows"
                shift # past argument
                ;;
            help)
                gs_loader_help
                exit $EXIT_CODE_OK
                ;;
            version)
                get_gs_loader_version
                exit $EXIT_CODE_OK
                ;;
            *)    # unknown option
                load_log error "unknown param:$1"
                exit $EXIT_CODE_FAIL
                ;;
        esac
    done
    read -r cmd_param_passwd
}

function create_copy_tables()
{
    if [ "$cmd_param_create" == "true" ]; then
        sql="SELECT copy_summary_create() WHERE NOT EXISTS(SELECT * FROM pg_tables WHERE schemaname='public' AND tablename='gs_copy_summary');"
        res=$(exec_sql "$sql")
        gs_loader_check_res "call copy_summary_create failed: $sql" "$res"

        sql="SELECT copy_error_log_create() WHERE NOT EXISTS(SELECT * FROM pg_tables WHERE schemaname='public' AND tablename='pgxc_copy_error_log');"
        res=$(exec_sql "$sql")
        gs_loader_check_res "call copy_error_log_create failed: $sql" "$res"
    fi
}

function clean_copy_table_record() {
    txid="$1"
    condition=$(exec_sql "select ' relname = ''' || relname || ''' and begintime = '''|| begintime || '''' from gs_copy_summary where id=$txid");
    
    if [ "$cmd_param_clean" == "true" ]; then
        condition=$(exec_sql "select ' relname = ''' || relname || ''' and begintime = '''|| begintime || '''' from gs_copy_summary where id=$txid")
        res=$(exec_sql "delete from pgxc_copy_error_log where $condition")
        res=$(exec_sql "delete from gs_copy_summary where id = $txid")
    fi
}

function clean_copy_tables()
{
    for ((i=0; i<loader_txids_count; i++)); do
        txid=${loader_txids[$i]}
        clean_copy_table_record "$txid"
    done
}

function exit_with_clean_file() {
    rm -f ${gs_loader_file_tmp}
    exit $EXIT_CODE_FAIL
}

function clean_and_get_exit_code()
{
    rm -f ${gs_loader_file_tmp}

    txids_array=$(get_txids_array)
    sql="select 'not_load_lines:'||pg_catalog.sum(errorrows)+pg_catalog.sum(whenrows)+pg_catalog.sum(allnullrows) from gs_copy_summary where id in $txids_array"
    res=$(exec_sql "$sql")
    gs_loader_check_res "query gs_copy_summary failed: $sql" "$res"
    not_load_lines=$(echo $res | grep 'not_load_lines:' | sed 's/[^0-9]*//g')
    load_log info "not loaded lines:"$not_load_lines

    clean_copy_tables

    if [ "$not_load_lines" == "0" ]; then
        return $EXIT_CODE_OK
    fi

    return $EXIT_CODE_WARN
}

function check_command_exist()
{
    res=$(which $1)
    if [ -z "$res" ]; then
        load_log error "can not find command: $1"
        exit $EXIT_CODE_FAIL
    fi
}

function check_parameters()
{
    check_command_exist "gsql"
    # check control file
    ctlfile="$cmd_param_ctl"
    if [ "$ctlfile" == "" ]; then
        echo "ERROR: control file is empty"
        exit $EXIT_CODE_FAIL
    fi

    if [ -d "$ctlfile" ]; then
        echo "ERROR: control file $ctlfile is a directory"
        exit $EXIT_CODE_FAIL
    fi

    if [ ! -f "$ctlfile" ]; then
        echo "ERROR: control file $ctlfile not found or don't have permission"
        exit $EXIT_CODE_FAIL
    fi
    
    if [ ! -r "$ctlfile" ]; then
        echo "ERROR: control file $ctlfile Permission denied"
        exit $EXIT_CODE_FAIL
    fi

    # check data file
    if [ "$loader_datafile_count" == 0 ]; then
        echo "ERROR: data file is empty, or don't have permission"
        exit $EXIT_CODE_FAIL
    fi

    if [[ "$cmd_param_create" != "true" ]] && [[ "$cmd_param_create" != "false" ]]; then
        echo "ERROR: 'create' parameter should be true/false"
        exit $EXIT_CODE_FAIL
    fi
    if [[ "$cmd_param_clean" != "true" ]] && [[ "$cmd_param_clean" != "false" ]]; then
        echo "ERROR: 'clean' parameter should be true/false"
        exit $EXIT_CODE_FAIL
    fi
}

function pre_process_load_file()
{
    load_file="$1"

    # replace WHEN (1:2) -> WHEN (1-2)
    sed -i -r 's/WHEN[[:space:]]*(\([[:space:]]*[[:digit:]]+[[:space:]]*):([[:space:]]*[[:digit:]]+[[:space:]]*\))/WHEN \1-\2/ig' $load_file

    # replace AND (1:2) -> AND (1-2)
    sed -i -r 's/AND[[:space:]]*(\([[:space:]]*[[:digit:]]+[[:space:]]*):([[:space:]]*[[:digit:]]+[[:space:]]*\))/AND \1-\2/ig' $load_file
    
    # replace POSITION (1:2) -> POSITION (1-2)
    sed -i -r 's/POSITION[[:space:]]*(\([[:space:]]*[[:digit:]]+[[:space:]]*):([[:space:]]*[[:digit:]]+[[:space:]]*\))/POSITION \1-\2/ig' $load_file
    
    # replace sequence (MAX, 1) -> sequence (MAXVALUE, 1)
    sed -i -r 's/SEQUENCE[[:space:]]*\([[:space:]]*MAX[[:space:]]*,([[:space:]]*[[:digit:]]+[[:space:]]*\))/SEQUENCE (MAXVALUE,\1/ig' $load_file
    
    # replace sequence (COUNT, 1) -> sequence (ROWS, 1)
    sed -i -r 's/SEQUENCE[[:space:]]*\([[:space:]]*COUNT[[:space:]]*,([[:space:]]*[[:digit:]]+[[:space:]]*\))/SEQUENCE (ROWS,\1/ig' $load_file

    # replace constant "" -> constant ''
    sed -i -r 's/CONSTANT[[:space:]]*""/CONSTANT '\'''\''/ig' $load_file
}

function check_db_conn()
{
    sql="select 'GS_LOADER_CONNECT_OK'"
    res=$(exec_sql "$sql")
    # gs_loader_check_res "check db connection using: $sql" "$res"

    load_log debug "$res"

    conn_ok=$(echo $res | grep 'GS_LOADER_CONNECT_OK' | wc -l)
    if [ "$conn_ok" != "1" ]; then
        load_log error "check db connection failed"
        exit $EXIT_CODE_FAIL
    fi
}

function append_one_data()
{
    datafile=$1

    for file in $(ls $datafile)
    do
        loader_datafiles[$loader_datafile_count]=$file
        loader_datafile_count=$(($loader_datafile_count+1))
    done
}

function append_one_txid()
{
    txid=$1
    loader_txids[$loader_txids_count]=$txid
    loader_txids_count=$(($loader_txids_count+1))
}

function recalcuate_errors()
{
    txid=$1

    if [ "$cmd_param_errors" == "" ]; then
        return 0
    fi

    sql="select 'errorrows:' || errorrows from gs_copy_summary where id = $txid"
    res=$(exec_sql "$sql")
    gs_loader_check_res_with_clean_file_txid "query gs_copy_summary failed: $sql" "$res" "$txid"
    errorrows=$(echo $res | grep 'errorrows:' | sed -r 's/.*errorrows:([0-9]+).*/\1/')
    cmd_param_errors=$((cmd_param_errors-errorrows))
}

function recalcuate_skip()
{
    txid=$1

    if [ "$cmd_param_skip" == "" ]; then
        return 0
    fi

    sql="select 'skiprows:' || skiprows from gs_copy_summary where id = $txid"
    res=$(exec_sql "$sql")
    gs_loader_check_res_with_clean_file_txid "query gs_copy_summary failed: $sql" "$res" "$txid"
    skiprows=$(echo $res | grep 'skiprows:' | sed -r 's/.*skiprows:([0-9]+).*/\1/')
    cmd_param_skip=$((cmd_param_skip-skiprows))
}

# load on datafile into table
function load_one_datafile()
{
    datafile="$1"
    badfile="$2"
    discardfile="$3"

    # generate LOAD DATA SQL
    echo "" > ${gs_loader_file_tmp}
    gen_load_options "$datafile" "$cmd_param_skip" "$cmd_param_errors" >> ${gs_loader_file_tmp}
    load_data_sql "$cmd_param_ctl" >> ${gs_loader_file_tmp}

    pre_process_load_file "$gs_loader_file_tmp"

    # transform to \COPY SQL
    copy_sql=$(trans_load_to_copy ${gs_loader_file_tmp})
    load_log info "copy sql: $copy_sql"
    gs_loader_check_res_with_clean_file "transform load to copy failed: $(cat ${gs_loader_file_tmp})" "$copy_sql"

    gen_full_copy_sql "$copy_sql" > ${gs_loader_file_tmp}

    # execute \COPY SQL
    copy_res=$(copy_into_table ${gs_loader_file_tmp})
    load_log info "copy result: $copy_res"
    gs_loader_check_res_with_clean_file "after transform: $(cat ${gs_loader_file_tmp})" "$copy_res"

    # get txid
    txid=$(echo $copy_res | grep 'copy_txid:' | sed -r 's/.*copy_txid:([0-9]+).*/\1/')
    load_log info "txid is "$txid
    if [ "$txid" == "" ]; then
        load_log error "cannot get copy txid"
        exit_with_clean_file
    fi

    append_one_txid $txid

    recalcuate_errors $txid
    recalcuate_skip $txid

    gen_badfile "$badfile" $txid
    gen_discardfile "$discardfile" $txid
}

function loader_stat_files()
{
    logfile="$1"
    echo ""                                                      >> $logfile
    echo "Control File:    $cmd_param_ctl"                       >> $logfile
    echo ""                                                      >> $logfile

    echo "There are $loader_datafile_count data files:"          >> $logfile
    for ((i=0; i<loader_datafile_count; i++)); do
        echo " Data File:     "${loader_datafiles[$i]}           >> $logfile
        echo " Bad File:      "${loader_badfiles[$i]}            >> $logfile
        echo " Discard File:  "${loader_discardfiles[$i]}        >> $logfile
        echo ""                                                  >> $logfile
    done
}

function loader_stat_info()
{
    start_time="$1"
    end_time="$2"

    loader_stat_files "$cmd_param_log"
    loader_stat_summary "$cmd_param_log"
    echo -e "$gs_loader_version\n"
    succ_rows_info=$(grep -e "Rows successfully loaded" $cmd_param_log | xargs)
    echo " "$succ_rows_info" "
    echo -e ""
    echo -e "log file is: \n $cmd_param_log"

    echo "Run began on $start_time" >>  $cmd_param_log
    echo "Run ended on $end_time" >>  $cmd_param_log

    start_time=$(date -u -d "$start_time" +"%s.%N")
    end_time=$(date -u -d "$end_time" +"%s.%N")

    echo -e "" >> $cmd_param_log

    elapsed_time=$(date -u -d "0 $end_time sec - $start_time sec" +"%H:%M:%S.%3N")
    echo "Elapsed time was:     $elapsed_time" >>  $cmd_param_log
}

function main()
{
    start_time=$(date "+%Y-%m-%d %H:%M:%S.%3N")

    parse_cmd_params "$@"
    check_parameters

    init_logfile "$cmd_param_log"
    init_badfiles "$cmd_param_bad"
    init_discardfiles "$cmd_param_discard"

    check_db_conn
    create_copy_tables

    load_log info "load data begin..."

    for ((i=0; i<loader_datafile_count; i++)); do
        load_log info "processing data: ${loader_datafiles[$i]} bad: ${loader_badfiles[$i]} discard: ${loader_discardfiles[$i]}"
        load_log info "options: skip:${cmd_param_skip} errors:${cmd_param_errors}"
        loader_datafile_index=$i
        load_one_datafile "${loader_datafiles[$i]}" "${loader_badfiles[$i]}" "${loader_discardfiles[$i]}"
    done

    gs_loader_table_name=$(cat ${gs_loader_file_tmp}|grep "\COPY" | awk '{print $2}')

    end_time=$(date "+%Y-%m-%d %H:%M:%S.%3N")

    loader_stat_info "$start_time" "$end_time"

    clean_and_get_exit_code
    exit_code=$?

    load_log info "load data end."

    exit $exit_code
}

main "$@"
