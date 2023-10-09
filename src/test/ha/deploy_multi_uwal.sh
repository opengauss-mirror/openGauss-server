#!/bin/sh
# deploy UWAL primary-standby

source ./standby_env.sh

standby_num=1

sleep 2
#init the database and build standby and run all

python $scripts_dir/pgxc_multi_uwal.py -i -d $standby_num -l 1 -D $g_data_path
