source ./util.sh

function test_1()
{
    set_default
    kill_cluster
    printf "set extreme_rto_standby_read para\n"
    gs_guc set -Z datanode -D $primary_data_dir -c "recovery_parse_workers = 3"
    gs_guc set -Z datanode -D $primary_data_dir -c "recovery_redo_workers = 3"
    gs_guc set -Z datanode -D $primary_data_dir -c "hot_standby = on"

    gs_guc set -Z datanode -D $standby_data_dir -c "recovery_parse_workers = 3"
    gs_guc set -Z datanode -D $standby_data_dir -c "recovery_redo_workers = 3"
    gs_guc set -Z datanode -D $standby_data_dir -c "hot_standby = on"
    start_cluster
    echo "start cluster success"
    sleep 5

    echo "insert data on primary"
    gsql -d postgres -p ${dn1_primary_port} -m -c "drop table aaa;"
    gsql -d postgres -p ${dn1_primary_port} -m -c "create table aaa(number int);"
    for((i=1;i<=100;i++))
    do
        gsql -d postgres -p ${dn1_primary_port} -m -c "insert into aaa(number) values($i);"
    done

    echo "primary guc check"
    res=`gsql -d postgres -p ${dn1_primary_port} -m -c "show recovery_parse_workers \x" | grep recovery_parse_workers | awk '{print $NF}'`
    if [ "$res" != 3 ]; then
        echo "extreme_rto_standby_read is off 1, $failed_keyword"
        exit 1
    fi

    res=`gsql -d postgres -p ${dn1_primary_port} -m -c "show recovery_redo_workers \x" | grep recovery_redo_workers | awk '{print $NF}'`
    if [ "$res" != 3 ]; then
        echo "extreme_rto_standby_read is off 2, $failed_keyword"
        exit 1
    fi

    res=`gsql -d postgres -p ${dn1_primary_port} -m -c "show hot_standby \x" | grep hot_standby | awk '{print $NF}'`
    if [ "$res" != "on" ]; then
        echo "extreme_rto_standby_read is off 3, $failed_keyword"
        exit 1
    fi

    echo "standby guc check"
    res=`gsql -d postgres -p ${dn1_standby_port} -m -c "show recovery_parse_workers \x" | grep recovery_parse_workers | awk '{print $NF}'`
    if [ "$res" != 3 ]; then
        echo "extreme_rto_standby_read is off 4, $failed_keyword"
        exit 1
    fi

    res=`gsql -d postgres -p ${dn1_standby_port} -m -c "show recovery_redo_workers \x" | grep recovery_redo_workers | awk '{print $NF}'`
    if [ "$res" != 3 ]; then
        echo "extreme_rto_standby_read is off 5, $failed_keyword"
        exit 1
    fi

    res=`gsql -d postgres -p ${dn1_standby_port} -m -c "show hot_standby \x" | grep hot_standby | awk '{print $NF}'`
    if [ "$res" != "on" ]; then
        echo "extreme_rto_standby_read is off 6, $failed_keyword"
        exit 1
    fi

    echo "query data on standby"
    gsql -d postgres -p ${dn1_standby_port} -m -c "select * from aaa;" -x | grep number | awk '{print $NF}' > ./results/exrtostandbyread/start_exrto_standby_read_multi_data.txt
    if [ "$?" -ne "0" ]; then
        echo "extreme_rto_standby_read is off 7, $failed_keyword"
        exit 1
    fi
    number=0
    for line in $(cat ./results/exrtostandbyread/start_exrto_standby_read_multi_data.txt)
    do
        let number++
        if [ $line != $number ]; then
            echo "extreme_rto_standby_read query data is wrong, $line, $number, $failed_keyword"
            exit 1
        fi
    done
}

function tear_down() {
    stop_streaming_cluster
    gs_guc set -Z datanode -D $primary_data_dir -c "recovery_parse_workers = 1"
    gs_guc set -Z datanode -D $primary_data_dir -c "recovery_redo_workers = 1"

    gs_guc set -Z datanode -D $standby_data_dir -c "recovery_parse_workers = 1"
    gs_guc set -Z datanode -D $standby_data_dir -c "recovery_redo_workers = 1"
}
test_1
tear_down
