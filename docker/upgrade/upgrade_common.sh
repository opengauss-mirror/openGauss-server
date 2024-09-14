#!/bin/bash
# Copyright (c) Huawei Technologies Co., Ltd. 2010-2022. All rights reserved.
# date: 2021-12-22
# version: 1.0

function init_config() {
  # upgrade tool path
  UPGRADE_TOOL_PATH=$(
    cd $(dirname $0)
    pwd
  )
  source ${UPGRADE_TOOL_PATH}/upgrade_errorcode.sh

  if [[ X"$ENVFILE" = X ]]; then
    ENVFILE=~/.bashrc
  fi
  if ! source $ENVFILE; then
    echo "Error: please check $ENVFILE first"
    exit 1
  fi

  if [[ X"$GAUSSDATA" = X ]]; then
    GAUSSDATA=/var/lib/opengauss/data
  fi
  if [[ X"$GAUSS_UPGRADE_BASE_PATH" = X ]]; then
    GAUSS_UPGRADE_BASE_PATH=$GAUSSDATA/upgrade
  fi
  if [[ X"$UPGRADE_NEW_PKG_PATH" = X ]]; then
    UPGRADE_NEW_PKG_PATH=$GAUSS_UPGRADE_BASE_PATH/pkg_new
  fi
}

function check_and_init() {
  check_config
  check_user
  check_cmd_conflict
}

function check_config() {
  # check GAUSS_UPGRADE_BASE_PATH and cd in
  check_config_path "$GAUSS_UPGRADE_BASE_PATH"
  if [[ ! -d "$GAUSS_UPGRADE_BASE_PATH" ]]; then
    die "$GAUSS_UPGRADE_BASE_PATH must exist" ${err_check_init}
  fi
  if ! cd "$GAUSS_UPGRADE_BASE_PATH"; then
    die "Cannot access $GAUSS_UPGRADE_BASE_PATH" ${err_check_init}
  fi

  # check and init GAUSS_LOG_FILE
  check_and_init_log_file
  
  # check env
  check_env

  # check GAUSS_LISTEN_PORT
  check_port

  # check UPGRADE_NEW_PKG_PATH and cd in
  check_config_path "$UPGRADE_NEW_PKG_PATH"
  if [[ ! -d "$UPGRADE_NEW_PKG_PATH" ]]; then
    die "$UPGRADE_NEW_PKG_PATH must exist" ${err_check_init}
  fi
  if ! cd "$UPGRADE_NEW_PKG_PATH"; then
    die "Cannot access $UPGRADE_NEW_PKG_PATH" ${err_check_init}
  fi

  # check GAUSS_TMP_PATH
  if [[ X"$GAUSS_TMP_PATH" = X ]]; then
    GAUSS_TMP_PATH=${GAUSS_UPGRADE_BASE_PATH}/tmp
  fi
  check_config_path "$GAUSS_TMP_PATH"
  if [[ ! -d "$GAUSS_TMP_PATH" ]]; then
    if ! mkdir -p -m 700 "$GAUSS_TMP_PATH"; then
      die "mkdir -p -m 700 $GAUSS_TMP_PATH failed" ${err_check_init}
    fi
  fi

  # check GAUSS_BACKUP_BASE_PATH
  if [[ X"$GAUSS_BACKUP_BASE_PATH" = X ]]; then
    GAUSS_BACKUP_BASE_PATH=${GAUSS_UPGRADE_BASE_PATH}/bak
  fi
  check_config_path "$GAUSS_BACKUP_BASE_PATH"
  if [[ ! -d "$GAUSS_BACKUP_BASE_PATH" ]]; then
    if ! mkdir -p -m 700 "$GAUSS_BACKUP_BASE_PATH"; then
      die "mkdir -p -m 700 $GAUSS_BACKUP_BASE_PATH failed" ${err_check_init}
    fi
  fi

  # query current dn role and save it to temp file
  query_dn_role

  # check GAUSS_UPGRADE_SYNC_CONFIG_LIST
  check_config_sync_path
}

function check_and_init_log_file() {
  # check and init GAUSS_LOG_FILE
  if [[ X"$GAUSS_LOG_PATH" = X ]]; then
    GAUSS_LOG_FILE=$GAUSS_UPGRADE_BASE_PATH/upgrade.log
  else
    check_config_path "$GAUSS_LOG_PATH"
    GAUSS_LOG_FILE="$GAUSS_LOG_PATH"/upgrade.log
  fi
  log_dir=$(dirname "$GAUSS_LOG_FILE")
  if [[ ! -d "$log_dir" ]]; then
    if ! mkdir -p -m 700 "$log_dir"; then
      echo "mkdir -p -m 700 ${log_dir} failed"
      exit ${err_check_init}
    fi
  fi
  if touch "$GAUSS_LOG_FILE" && chmod 600 "$GAUSS_LOG_FILE"; then
    echo "check log file is ok" >>"$GAUSS_LOG_FILE"
  else
    echo "touch $GAUSS_LOG_FILE && chmod 600 $GAUSS_LOG_FILE failed"
    exit ${err_check_init}
  fi
}

function check_port() {
  #check GAUSS_LISTEN_PORT
  if [[ X"$GAUSS_LISTEN_PORT" = X ]]; then
    GAUSS_LISTEN_PORT=$(grep -E "^[ ]{0,}port" "$GAUSSDATA"/postgresql.conf | sed 's/\s*//g' | awk -F# '{print $1}' | awk -F= '{print $2}')
  fi
  if [[ X"$GAUSS_LISTEN_PORT" = X ]]; then
    GAUSS_LISTEN_PORT=5432
    debug "The value of port cannot be obtained from configuration, use default value 5432"
  fi
  if ! echo "$GAUSS_LISTEN_PORT" | grep -Eqw "[0-9]{4,5}"; then
    die "GAUSS_LISTEN_PORT may be not right" ${err_check_init}
  fi
}

function usage() {
  echo "
Usage: $0 [OPTION]
Arguments:
   -h|--help                   show this help, then exit
   -t                          upgrade_pre,upgrade_bin,upgrade_post,rollback_pre,rollback_bin,rollback_post,upgrade_commit
                               query_start_mode,switch_over
   --min_disk                  reserved upgrade disk space in MB, default 2048
   -D                          openGauss data dir, default /var/lib/opengauss/data
   -B                          upgrade root dir, default /var/lib/opengauss/data/upgrade
   -N                          new package dir, default /var/lib/opengauss/data/upgrade/pkg_new
   -E                          env file, default /home/omm/.bashrc
   "
}

function debug() {
  local current_time=$(date +"%Y-%m-%d %H:%M:%S")
  echo "[$current_time]" "$1" >>"${GAUSS_LOG_FILE}"
}

function log() {
  local current_time=$(date +"%Y-%m-%d %H:%M:%S")
  echo "[$current_time]" "$1" >>"${GAUSS_LOG_FILE}"
  echo "$1"
}

function die() {
  local current_time=$(date +"%Y-%m-%d %H:%M:%S")
  if [[ -f "${GAUSS_LOG_FILE}" ]];then
    echo "[$current_time]" "$1" >>"${GAUSS_LOG_FILE}"
  fi
  echo -e "\033[31mError: $1\033[0m"
  exit $2
}

function parse_cmd_line() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
    -h | --help)
      usage
      exit 0
      ;;
    -t)
      if [[ "$2" = "" ]]; then
        die "the parameter -t cannot be empty." ${err_parameter}
      fi
      action=$2
      action_list="upgrade_pre upgrade_bin upgrade_post rollback_pre rollback_bin rollback_post upgrade_commit query_start_mode switch_over"
      if ! echo "$action_list"|grep -wq "$action"; then
        local error_msg="only these actions are supported: upgrade_pre, upgrade_bin, upgrade_post, rollback_pre, \
rollback_bin, rollback_post, upgrade_commit and query_start_mode switch_over"
        die "$error_msg" ${err_parameter}
      fi
      shift 2
      ;;
    --min_disk)
      if [[ "$2" = "" ]]; then
        die "the parameter --min_disk cannot be empty." ${err_parameter}
      fi
      min_disk=$2
      if echo ${min_disk} | grep -q "[^0-9]";then
         die "min_disk value must be int " ${err_parameter}
      fi
      if echo ${min_disk} | grep -q "^0";then
         die "min_disk value cannot start with 0 " ${err_parameter}
      fi
      if [[ ${min_disk} -lt 2048 || ${min_disk} -gt 204800 ]]; then
        die "min_disk value must be >= 2048 and <= 204800" ${err_parameter}
      fi
      shift 2
      ;;
    -D)
      if [[ "$2" = "" ]]; then
        die "the parameter -d cannot be empty." ${err_parameter}
      fi
      GAUSSDATA=$2
      shift 2
      ;;
    -B)
      if [[ "$2" = "" ]]; then
        die "the parameter -B cannot be empty." ${err_parameter}
      fi
      GAUSS_UPGRADE_BASE_PATH=$2
      shift 2
      ;;
    -N)
      if [[ "$2" = "" ]]; then
        die "the parameter -N cannot be empty." ${err_parameter}
      fi
      UPGRADE_NEW_PKG_PATH=$2
      shift 2
      ;;
    -E)
      if [[ "$2" = "" ]]; then
        die "the parameter -e cannot be empty." ${err_parameter}
      fi
      ENVFILE=$2
      shift 2
      ;;
    *)
      echo "please input right parameter, the following command may help you"
      echo "sh upgrade.sh --help or sh upgrade.sh -h"
      exit ${err_parameter}
      ;;
    esac
  done
  echo "Parse cmd line successfully."

}

function check_user() {
  user=$(whoami)
  if [[ X"$user" == X"root" ]]; then
    die "Can not exec the script with root!" ${err_check_init}
  fi
}

function check_env() {
  if [[ "$GAUSSHOME" == "" ]]; then
    die "GAUSSHOME cannot be null!" ${err_check_init}
  fi
  if [[ "$GAUSSDATA" == "" ]]; then
    die "GAUSSDATA cannot be all null!" ${err_check_init}
  fi
  check_config_path "$GAUSSHOME"
  check_config_path "$GAUSSDATA"
  log "Current env value: GAUSSHOME is $GAUSSHOME, GAUSSDATA is $GAUSSDATA."
}

function check_config_path() {
  local temp_value="$1"
  if [[ "$temp_value" == *[\(\)\{\}\[\]\<\>\"\'\`\\\ \*\!\|\;\&\$\~\?]* ]];then
      die "$temp_value may contain illegal characters" ${err_check_init}
  fi
  if echo "$temp_value"|grep -Eq "^/{1,}$"; then
      die "path cannot be / " ${err_check_init}
  fi
}

function check_config_sync_path() {
  if [[ "abc$GAUSS_UPGRADE_SYNC_CONFIG_LIST" == "abc" ]]; then
    debug "GAUSS_UPGRADE_SYNC_CONFIG_LIST is null"
  fi
  if [[ "$GAUSS_UPGRADE_SYNC_CONFIG_LIST" == *[\(\)\{\}\[\]\<\>\"\'\`\\\ \*\!\|\;\&\$\~\?]* ]];then
      die "$GAUSS_UPGRADE_SYNC_CONFIG_LIST may contain illegal characters" ${err_check_init}
  fi
  local array=(${GAUSS_UPGRADE_SYNC_CONFIG_LIST//,/ })
  for var in ${array[@]}
  do
    if echo "$var"|grep -Eq "^/"; then
      die "path in ${GAUSS_UPGRADE_SYNC_CONFIG_LIST} must not be start with /" ${err_check_init}
    fi
  done

}

function check_config_user() {
  local temp_value="$1"
  if [[ "$temp_value" == *[\(\)\{\}\[\]\<\>\"\'\`\\\ \*\!\|\;\&\$\~\?/]* ]];then
      die "$temp_value may contain illegal characters" ${err_check_init}
  fi
}

function check_version() {
  # get old version
  if [[ ! -f ${GAUSSHOME}/version.cfg ]]; then
    die "Cannot find current version.cfg!" ${err_upgrade_pre}
  fi
  old_version=$(sed -n 3p $GAUSSHOME/version.cfg)
  old_cfg=$(sed -n 2p "$GAUSSHOME"/version.cfg | sed 's/\.//g')

  # get new version
  if [[ ! -f $UPGRADE_NEW_PKG_PATH/version.cfg ]]; then
    die "Cannot find new version.cfg!" ${err_upgrade_pre}
  fi
  new_version=$(sed -n 3p $UPGRADE_NEW_PKG_PATH/version.cfg)
  new_cfg=$(sed -n 2p $UPGRADE_NEW_PKG_PATH/version.cfg | sed 's/\.//g')

  if [[ X"$old_version" == X || X"$old_cfg" == X || X"$new_version" == X || X"$new_cfg" == X ]]; then
    die "Maybe version.cfg is not normal" ${err_upgrade_pre}
  fi
  if ! echo "$old_cfg"|grep -Ewq "[0-9]{3,6}";then
    die "Maybe old version.cfg is not normal" ${err_upgrade_pre}
  fi
  if ! echo "$new_cfg"|grep -Ewq "[0-9]{3,6}";then
    die "Maybe new version.cfg is not normal" ${err_upgrade_pre}
  fi

  if [[ "$old_version" == "$new_version" ]]; then
    die "New version is same as old, the commitId is $old_version!" ${err_version_same}
  fi
  if [[ ${new_cfg} -lt ${old_cfg} ]]; then
    die "Current version is newer!" ${err_upgrade_pre}
  fi
  big_cfg="False"
  if [[ ${new_cfg} -gt ${old_cfg} ]]; then
    log "Big upgrade is needed!"
    big_cfg="True"
  fi

  local flag_file="$GAUSS_TMP_PATH"/version_flag
  if echo "old_version=$old_version" > "$flag_file" && chmod 600 "$flag_file"; then
    debug "Begin to generate $flag_file"
  else
    die "Write $flag_file failed" ${err_upgrade_pre}
  fi
  if ! echo "new_version=$new_version" >> "$flag_file"; then
    die "Write $flag_file failed" ${err_upgrade_pre}
  fi
  if ! echo "big_cfg=$big_cfg" >> "$flag_file"; then
    die "Write $flag_file failed" ${err_upgrade_pre}
  fi
  if ! echo "old_cfg=$old_cfg" >> "$flag_file"; then
    die "Write $flag_file failed" ${err_upgrade_pre}
  fi
  log "Old version commitId is $old_version, version info is $old_cfg"
  log "New version commitId is $new_version, version info is $new_cfg"

  # need version.cfg to check big upgrade,note user exec sql on primary dn
}

function check_disk() {
  avail_disk=$(df -BM "$GAUSS_UPGRADE_BASE_PATH" | tail -n 1 | awk '{print $4}')
  avail_disk=${avail_disk:0:-1}
  if [[ X"$min_disk" == "X" ]]; then
    min_disk=2048
  fi
  if [[ ${avail_disk} -lt ${min_disk} ]]; then
    die "avail disk must be >= ${min_disk}MB, check with cmd: df -BM $GAUSSHOME!" ${err_check_init}
  fi
  log "Check available disk space successfully."
}

function check_db_process() {
  #ps wwx | grep "$GAUSSHOME/bin/gaussdb" | grep -v grep > /dev/null
 ps -ef|grep gaussdb | grep -v grep > /dev/null
}

function check_cmd_conflict() {
    LOCKFILE="${GAUSS_TMP_PATH}/.lock_GAUSSDB_UPGRADE_V5"
    if [[ -f "$LOCKFILE" ]]
    then
         pid=$(cat ${LOCKFILE})
         if [[ -n "$pid" ]];then
            if ps -p ${pid} | grep -w ${pid} >/dev/null;then
                die "Maybe upgrade_GAUSSV5.sh is running!" ${err_check_init}
            fi
         fi
    fi
    if ! echo $$ > "$LOCKFILE"; then
        die "Write $LOCKFILE failed" ${err_check_init}
    fi
}

function check_pkg() {
  if [[ -d "${GAUSS_UPGRADE_BIN_PATH}" ]]; then
    local file_list=(bin etc include lib share version.cfg)
    for temp_file in ${file_list[@]}
    do
      if [[ ! -e "${GAUSS_UPGRADE_BIN_PATH}/${temp_file}" ]];then
        die "$GAUSS_UPGRADE_BIN_PATH may be not right, ${temp_file} not exits" ${err_upgrade_pre}
      fi
    done
    if [[ $(ls "${GAUSS_UPGRADE_BIN_PATH}" |wc -l) -eq 6 ]]; then
      log "The upgrade will use existing files in $GAUSS_UPGRADE_BIN_PATH"
      return 0
    else
      die "$GAUSS_UPGRADE_BIN_PATH may be not right，exits other files" ${err_upgrade_pre}
    fi

  fi
  #get OS distributed version.
  kernel=""
  if [[ -f "/etc/euleros-release" ]]; then
    kernel=$(cat /etc/euleros-release | awk -F ' ' '{print $1}' | tr a-z A-Z)
    if [[ "${kernel}" = "EULEROS" ]]; then
      kernel="EULER"
    fi
  elif [[ -f "/etc/openEuler-release" ]]; then
    kernel=$(cat /etc/openEuler-release | awk -F ' ' '{print $1}')
  elif [[ -f "/etc/centos-release" ]]; then
    kernel=$(cat /etc/centos-release | awk -F ' ' '{print $1}')
  else
    kernel=$(lsb_release -d | awk -F ' ' '{print $2}')
  fi
  log "kernel: ${kernel}"

  #detect platform information.
  platform_arch=$(uname -p)
  bin_name="openGauss-3.0.1-CentOS-64bit.tar.bz2"
  binfile="openGauss-3.0.1-CentOS-64bit.tar.bz2"
  shafile="openGauss-3.0.1-CentOS-64bit.sha256"
  if [[ ! -f "${binfile}" ]] || [[ ! -f "${shafile}" ]]; then
    die "bin or sha256 file not exit for the platform ${kernel}-${platform_arch}!" ${err_upgrade_pre}
  fi
  sha_expect=$(cat ${shafile})
  sha_current=$(sha256sum ${binfile} | awk '{print $1}')
  if [[ "$sha_expect" != "$sha_current" ]]; then
    die "The sha256 value of $binfile does not match $shafile!" ${err_upgrade_pre}
  fi
  if ! echo "binfile=$binfile" >>"$GAUSS_TMP_PATH"/version_flag; then
    die "Write $GAUSS_TMP_PATH/version_flag failed" ${err_upgrade_pre}
  fi
  if [[ ! -f "$UPGRADE_NEW_PKG_PATH"/version.cfg ]]; then
    die "version.cfg must be exist in $UPGRADE_NEW_PKG_PATH" ${err_upgrade_pre}
  fi
}

function bak_gauss() {
  # bak postgresql.conf & pg_hba.conf
  if ! cp -rf "$GAUSSDATA"/postgresql.conf "$GAUSS_BACKUP_BASE_PATH"/postgresql.conf_"$old_version"; then
    die "Bak postgresql.conf failed!" ${err_upgrade_pre}
  else
    log "Bak postgresql.conf successfully."
  fi

  if ! cp -rf "$GAUSSDATA"/pg_hba.conf "$GAUSS_BACKUP_BASE_PATH"/pg_hba.conf_"$old_version"; then
    die "Bak pg_hba.conf failed!" ${err_upgrade_pre}
  else
    log "Bak pg_hba.conf successfully."
  fi

  # 如果新旧版本跨越了92601，则需要备份pg_filenode.map和pg_filenode.map.backup
  if [ $old_cfg -lt 92601 ] && [ $new_cfg -ge 92601 ] ; then
    if ! cp -rf $GAUSSDATA/global/pg_filenode.map $GAUSS_BACKUP_BASE_PATH/pg_filenode.map_$old_version; then
      die "Bak pg_filenode.map failed!" ${err_upgrade_pre}
    else
      log "Bak pg_filenode.map successfully."
    fi

    if ! cp -rf $GAUSSDATA/global/pg_filenode.map.backup $GAUSS_BACKUP_BASE_PATH/pg_filenode.map.backup_$old_version; then
      die "Bak pg_filenode.map.backup failed!" ${err_upgrade_pre}
    else
      log "Bak pg_filenode.map.backup successfully."
    fi
  fi
}

function decompress_pkg() {
  if [[ -d "${GAUSS_UPGRADE_BIN_PATH}" ]]; then
    return 0
  fi
  if [[ -d "$GAUSS_TMP_PATH"/install_bin_"$new_version" ]];then
    rm -rf "$GAUSS_TMP_PATH"/install_bin_"$new_version"
  fi
  if mkdir -p -m 700 "$GAUSS_TMP_PATH"/install_bin_"$new_version"; then
    log "begin decompress pkg in $GAUSS_TMP_PATH/install_bin_$new_version"
  fi
  if [[ X"$binfile" == X ]]; then
    binfile=$(grep binfile "$GAUSS_TMP_PATH"/version_flag | awk -F= '{print $2}')
    if [[ X"$binfile" == X ]]; then
      die "binfile name cannot be null" ${err_upgrade_pre}
    fi
  fi

  if cp "$binfile" "$GAUSS_TMP_PATH"/install_bin_"$new_version" && cd "$GAUSS_TMP_PATH"/install_bin_"$new_version" && tar -xf "$binfile" \
  && rm -f "$binfile"; then

    log "Decompress $binfile successfully."
  else
    die "Decompress $binfile failed" ${err_upgrade_pre}
  fi

  if cd "$UPGRADE_NEW_PKG_PATH" && cp "$UPGRADE_NEW_PKG_PATH"/version.cfg "$GAUSS_TMP_PATH"/install_bin_"$new_version"; then
    log "cp version.cfg successfully"
  else
    die "cp version.cfg failed" ${err_upgrade_pre}
  fi
}

function cp_pkg() {
  if [[ X"$new_version" == X ]]; then
    new_version=$(grep new_version "$GAUSS_TMP_PATH"/version_flag | awk -F= '{print $2}')
  fi
  if [[ -d "$GAUSS_UPGRADE_BIN_PATH" ]]; then
    new_bin_path="${GAUSS_UPGRADE_BIN_PATH}"
  else
    new_bin_path="$GAUSS_TMP_PATH"/install_bin_"$new_version"
  fi
  #check pkg's version.cfg is equal to version_flag
  temppkg_version=$(sed -n 3p "$new_bin_path"/version.cfg)
  if [[ "$new_version" != "$temppkg_version" ]]; then
    die "pkg's version.cfg is not correct!" ${err_upgrade_bin}
  fi
  if [[ -d "$GAUSSHOME" ]];then
    rm -rf "$GAUSSHOME"/*
  fi
  if cp -rf "$new_bin_path"/* "$GAUSSHOME"; then
    log "Binfile upgrade to new version successfully."
  else
    die "Binfile upgrade to new version failed" ${err_upgrade_bin}
  fi
}

function prepare_sql() {
  #$1: upgrade,upgrade-post,rollback,rollback-post
  #$2: maindb,otherdb
  temp_old=${old_cfg}
  temp_new=${new_cfg}
  local action="$1"
  local dbname="$2"
  local tempfile="$GAUSS_TMP_PATH"/temp_sql/"temp_"${action}_${dbname}.sql
  temp_file_num=0

  if echo "START TRANSACTION;set IsInplaceUpgrade = on;" > "$tempfile" && chmod 600 "$tempfile"; then
    debug "Begin to generate $tempfile"
  else
    die "Write $tempfile failed" ${err_upgrade_pre}
  fi
  if ! echo "SET search_path = 'pg_catalog';SET local client_min_messages = NOTICE;SET local log_min_messages = NOTICE;" >> "$tempfile"; then
    die "Write $tempfile failed" ${err_upgrade_pre}
  fi
  if ! echo "SET statement_timeout = 36000;"  >> "$tempfile"; then
    die "Write $tempfile failed" ${err_upgrade_pre}
  fi

  if [[ "$action" == "upgrade" || "$action" == "upgrade-post" ]]; then
    while [[ ${temp_old} -lt ${temp_new} ]]; do
      ((temp_old=$temp_old+1))
      local upgrade_sql_file="upgrade_sql/upgrade_catalog_${dbname}/${action}_catalog_${dbname}_${temp_old:0:2}_${temp_old:2}.sql"
      if [[ -f "$upgrade_sql_file" ]]; then
        if ! cat "$upgrade_sql_file" >> "$tempfile"; then
          die "Write $tempfile failed" ${err_upgrade_pre}
        fi
        debug "$upgrade_sql_file >> $tempfile"
        ((temp_file_num=temp_file_num+1))
      fi
    done
  fi

  if [[ "$1" == "rollback" || "$1" == "rollback-post" ]]; then
    while [[ ${temp_new} -gt ${temp_old} ]]; do
      local upgrade_sql_file="upgrade_sql/rollback_catalog_${dbname}/${action}_catalog_${dbname}_${temp_new:0:2}_${temp_new:2}.sql"
      if [[ -f "$upgrade_sql_file" ]]; then
        # skip *92_506.slq for now
        if echo "$upgrade_sql_file" | grep "92_506" > /dev/null; then
          local sql_92_506=$upgrade_sql_file
          ((temp_new=$temp_new-1))
          debug "skip 92506"
          continue
        fi
        if ! cat "$upgrade_sql_file" >> "$tempfile"; then
          die "Write $tempfile failed" ${err_upgrade_pre}
        fi
        debug "$upgrade_sql_file >>$tempfile"
        ((temp_file_num=temp_file_num+1))
      fi
      ((temp_new=$temp_new-1))
    done
    # append *92_506.sql to the end of tempfile
    if [[ X"$sql_92_506" != X ]]; then
      if ! cat "$sql_92_506" >> "$tempfile"; then
        die "Write $tempfile failed" ${err_upgrade_pre}
      fi
    fi
  fi

  if ! echo "COMMIT;" >> "$tempfile";then
    die "Write $tempfile failed" ${err_upgrade_pre}
  fi
  #file not meet requirements
  if [[ ${temp_file_num} -eq 0 ]]; then
    debug "No sql file for ${action} ${dbname}!"
    rm -f "$tempfile"
  else
    debug "get ${temp_file_num} files for ${action} ${dbname}!"
  fi
}

function prepare_sql_all() {
  local dir_temp_sql="$GAUSS_TMP_PATH"/temp_sql
  local sql_tar_file="$UPGRADE_NEW_PKG_PATH"/upgrade_sql.tar.gz
  local sql_tar_sha="$UPGRADE_NEW_PKG_PATH"/upgrade_sql.sha256

  if [[ ! -f "${sql_tar_file}" ]] || [[ ! -f "${sql_tar_sha}" ]]; then
    die "${sql_tar_file} or ${sql_tar_sha} not exit!" ${err_upgrade_pre}
  fi
  local sha_expect=$(cat ${sql_tar_sha})
  local sha_current=$(sha256sum ${sql_tar_file} | awk '{print $1}')
  if [[ "$sha_expect" != "$sha_current" ]]; then
    die "The sha256 value of $sql_tar_file does not match $sql_tar_sha!" ${err_upgrade_pre}
  fi
  if [[ -d "$dir_temp_sql" ]];then
      rm -rf "$dir_temp_sql"
  fi
  if mkdir -p -m 700 "$dir_temp_sql" && tar -xf "$sql_tar_file" -C "$dir_temp_sql"; then
    log "decompress upgrade_sql.tar.gz successfully."
  else
    die "decompress upgrade_sql.tar.gz failed" ${err_upgrade_pre}
  fi
  # total 8
  cd $dir_temp_sql
  for action in upgrade upgrade-post rollback rollback-post; do
    for db_base in maindb otherdb; do
      prepare_sql ${action} ${db_base}
    done
  done
  cd ${GAUSS_UPGRADE_BASE_PATH}
}

function exec_sql() {
  #$1: sqlfilename
  #$2: maindb,otherdb
  if [[ X"$dn_role" == X"standby" || X"$dn_role" == X"cascade_standby" ]]; then
    return 0
  fi
  if [[ ! -f "$1" ]]; then
    return 0
  fi

  if ! check_upgrade_mode_by_sql;then
    return 1
  fi
  
  tempresult="$GAUSS_TMP_PATH"/"temp_sql_tempresult_$(date +%Y%m%d_%H%M%S)"
  if echo "" > "$tempresult" && chmod 600 "$tempresult"; then
    debug "begin exec sql ,file name is $1"
  else
    log "Generate $tempresult failed."
  fi
  sqlbegin="gsql -p $GAUSS_LISTEN_PORT -X -t -A "

  if [[ "$2" == "maindb" ]]; then
    if echo "$db_password" | ${sqlbegin} -d postgres --echo-queries --set ON_ERROR_STOP=on -f $1 >> "$tempresult" 2>&1;then
      debug "Exec $1 on database: postgres successfully"
    else
      log "Exec sql on postgres failed."
      debug "$(cat ${tempresult})"
      rm -f ${tempresult}
      return 1
    fi
  else
    if databases=$(${sqlbegin} -d postgres -c "SELECT datname FROM pg_catalog.pg_database where datname != 'postgres';");then
        temp_num=$(echo ${databases}|awk '{print NF}')
        debug "Num of other databases: $temp_num"
    else
      log "Exec sql to get databases failed."
      return 1
    fi
    for database in ${databases}; do
      debug "Begin exec $1 on database: $database "
      ${sqlbegin} -d ${database} --echo-queries --set ON_ERROR_STOP=on -f $1 >> "$tempresult" 2>&1
    done
  fi
  if grep -wE "ERROR:|FATAL:|could not connect to server" ${tempresult}; then
    log "Exec sql failed."
    debug "$(cat ${tempresult})"
    rm -f ${tempresult}
    return 1
  else
    debug "Exec all sql successfully."
    rm -f ${tempresult}
    return 0
  fi
}

function guc_delete() {
  bak_ifs=$IFS
  IFS=$'\n'
  if [[ ! -f "$GAUSS_TMP_PATH"/temp_sql/upgrade_sql/set_guc/delete_guc ]]; then
    log "No need to delete guc"
  fi
  for para in $(cat "$GAUSS_TMP_PATH"/temp_sql/upgrade_sql/set_guc/delete_guc); do
    if echo ${para}|grep -w datanode > /dev/null;then
      para=$(echo ${para}|awk '{print $1}')
      if sed -i "/^${para}[ =]/d" ${GAUSSDATA}/postgresql.conf; then
        debug "$para was deleted successfully."
      else
        die "$para was deleted failed" ${err_upgrade_bin}
      fi
    fi
  done
  IFS=${bak_ifs}
  log "Delete guc successfully"
}

function stop_dbnode() {
  if ! check_db_process; then
    return 0
  fi
  gs_ctl stop -D ${GAUSSDATA} >>"${GAUSS_LOG_FILE}" 2>&1
}

function start_dbnode() {
  start_cmd="gs_ctl start  -D ${GAUSSDATA} -t 600 "
  if [[ X"$dn_role" = X ]]; then
    return 1
  fi
  if [[ X"$dn_role" != X"normal" ]]; then
    start_cmd="$start_cmd -M $dn_role"
  fi
  start_option=""
  if [[ X"$1" != X ]]; then
    start_option="-o '-u $1'"
  fi

  log "start gaussdb by cmd: $start_cmd $start_option"
  ${start_cmd} "$start_option" >>"${GAUSS_LOG_FILE}" 2>&1
}

function query_dn_role() {
  if [ -f "$GAUSS_TMP_PATH/temp_dn_role" ]; then
    dn_role=$(grep local_role "${GAUSS_TMP_PATH}/temp_dn_role" | head -1 | awk '{print $3}')
  else
    gs_ctl query -D ${GAUSSDATA} > ${GAUSS_TMP_PATH}/temp_dn_role
    dn_role=$(grep local_role "${GAUSS_TMP_PATH}/temp_dn_role" | head -1 | awk '{print $3}')
  fi

  if [[ "$dn_role" = "Normal" ]]; then
    dn_role="normal"
  elif [[ "$dn_role" = "Primary" ]]; then
    dn_role="primary"
  elif [[ "$dn_role" = "Standby" ]]; then
    dn_role="standby"
  elif [[ "$dn_role" = "Cascade" ]]; then
    dn_role="cascade_standby"
  else
    dn_role=""
  fi

  if [[ X"$dn_role" = X ]]; then
    die "dn_role cannot be null" ${err_dn_role_null}
  fi
}

function hide_password() {
  echo "input sql password:"
  read -s db_password
}

function reload_upgrade_config() {
  if check_upgrade_config "$1" "$2"; then
    return 0
  fi
  local current_time=$(date +"%Y-%m-%d %H:%M:%S")
  echo -n \[${current_time}\] "  " >>"${GAUSS_LOG_FILE}"
  for i in $(seq 1 3);do
      if gs_guc reload -D ${GAUSSDATA} -c "$1=$2" >>"${GAUSS_LOG_FILE}" 2>&1; then
        return 0
      fi
      sleep 2
  done
  return 1
}

function set_upgrade_config() {
  if check_upgrade_config "$1" "$2"; then
    return 0
  fi
  local current_time=$(date +"%Y-%m-%d %H:%M:%S")
  echo -n \[${current_time}\] "  " >>"${GAUSS_LOG_FILE}"
  for i in $(seq 1 3);do
      if gs_guc set -D ${GAUSSDATA} -c "$1=$2" >>"${GAUSS_LOG_FILE}" 2>&1; then
        debug "guc set $1=$2 successfully"
        return 0
      fi
      sleep 2
  done
  return 1
}

function check_upgrade_config() {
  local tempfile="$GAUSS_TMP_PATH"/".temp_check_guc_value"
  if gs_guc check -D ${GAUSSDATA} -c "$1" > "$tempfile" 2>&1 ;then
      tempvalue=$(cat "$tempfile"|tail -2|head -1|sed 's/[[:space:]]//g'|awk -F= '{print $2}')
      if ! rm -f ${tempfile}; then
        log "rm -f $tempfile failed"
        return 1
      fi
      if [[ "$tempvalue" == "$2" ]]; then
        debug "guc check $1=$2 successfully"
        return 0
      else
        return 1
      fi
  else
      if ! rm -f ${tempfile}; then
        log "rm -f $tempfile failed"
        return 1
      fi
      return 1
  fi
}

function check_upgrade_mode_by_sql(){
  # check upgrade_mode = 2 by sql
  check_upgrade_mode_result="$GAUSS_TMP_PATH"/".temp_upgrade_mode"
  if echo "" > ${check_upgrade_mode_result} && chmod 600 ${check_upgrade_mode_result}; then
    debug "Begin to generate check_upgrade_mode_result."
  else
    log "generate $check_upgrade_mode_result failed."
    return 1
  fi
  gsql -p ${GAUSS_LISTEN_PORT} -d postgres -X -t -A \
    -c "show upgrade_mode;" > ${check_upgrade_mode_result} 2>&1 &
  sleep 0.1

  for i in $(seq 1 60);
  do
    check_mode_sql=$(cat ${check_upgrade_mode_result})
    if [[ "$check_mode_sql" == "2" ]];then
        rm -f ${check_upgrade_mode_result}
        return 0
    elif [[ "$check_mode_sql" == "0" ]];then
        rm -f ${check_upgrade_mode_result}
        echo ${db_password} | gsql -p ${GAUSS_LISTEN_PORT} -d postgres -X -t -A \
        -c "show upgrade_mode;" > ${check_upgrade_mode_result} 2>&1 &
    elif [[ "$check_mode_sql" == "" ]];then
        debug "Wait for check_upgrade_mode_result..."
    else
        log "$(cat ${check_upgrade_mode_result})"
        return 1
    fi
    sleep 0.5
  done
  if [[ -f "${check_upgrade_mode_result}" ]]; then
      debug "check_upgrade_mode_result is $(cat ${check_upgrade_mode_result})"
      rm -f ${check_upgrade_mode_result}
  else
      debug "get upgrade_mode by gsql failed"
  fi
  return 1
}

function rollback_pre() {
  parses_step
  if [[ "$current_step" -lt 1 ]]; then
    log "no need do rollback_pre step"
  elif [[ "$current_step" -gt 2 ]]; then
    die "You should rollback_bin first" ${err_rollback_pre}
  else
    if [[ X"$big_cfg" == X ]]; then
      big_cfg=$(grep big_cfg "$GAUSS_TMP_PATH"/version_flag | awk -F= '{print $2}')
    fi
    if [[ "$big_cfg" == "True" ]]; then
      if ! check_db_process; then
        die "Gaussdb is not running" ${err_rollback_pre}
      fi
      if ! reload_upgrade_config upgrade_mode 2; then
        die "set upgrade_mode to 2 failed" ${err_rollback_pre}
      fi
      record_step 1
      if exec_sql "$GAUSS_TMP_PATH"/temp_sql/temp_rollback_maindb.sql maindb && exec_sql "$GAUSS_TMP_PATH"/temp_sql/temp_rollback_otherdb.sql otherdb; then
        debug "rollback pre sql successfully"
      else
        die "rollback pre sql failed" ${err_rollback_pre}
      fi
      if ! reload_upgrade_config upgrade_mode 0; then
        die "set upgrade_mode to 0 failed" ${err_upgrade_pre}
      fi
    fi
    record_step 0
    log "The rollback_pre step is executed successfully. "
  fi
}

function rollback_bin() {
  parses_step
  if [[ "$current_step" -lt 3 ]]; then
    log "no need do rollback_bin step"
  elif [[ "$current_step" -gt 4 ]]; then
    die "You should rollback_post first" ${err_rollback_bin}
  else
    if [[ X"$old_version" == X ]]; then
      old_version=$(grep old_version "$GAUSS_TMP_PATH"/version_flag | awk -F= '{print $2}')
    fi
    if ! cd "$GAUSS_BACKUP_BASE_PATH"; then
      die "$GAUSS_BACKUP_BASE_PATH cannot access" ${err_rollback_bin}
    fi
    # Once the rollback starts, you must change the status code first.
    # Otherwise, the upgrade may continue after the rollback is interrupted.
    record_step 3
    if ! stop_dbnode; then
      die "Stop gaussdb failed" ${err_rollback_bin}
    fi

    if ! cp -rf ./postgresql.conf_"$old_version" "$GAUSSDATA"/postgresql.conf; then
      die "Restore postgresql.conf failed!" ${err_rollback_bin}
    fi
    if ! cp -rf ./pg_hba.conf_"$old_version" "$GAUSSDATA"/pg_hba.conf; then
      die "Restore pg_hba.conf failed!" ${err_rollback_bin}
    fi
    if [ -f ./pg_filenode.map_$old_version ]; then
      if ! cp -rf ./pg_filenode.map_$old_version $GAUSSDATA/global/pg_filenode.map; then
        die "Restore pg_filenode.map failed!" ${err_rollback_bin}
      fi
      if ! cp -rf ./pg_filenode.map.backup_$old_version $GAUSSDATA/global/pg_filenode.map.backup; then
        die "Restore pg_filenode.map.backup failed!" ${err_rollback_bin}
      fi
    fi
    log "Restore postgresql.conf, pg_hba.conf successfully."
    # when rollback postgresql.conf, upgrade_mode should be set to 2
    if [[ X"$big_cfg" == X ]]; then
      big_cfg=$(grep big_cfg "$GAUSS_TMP_PATH"/version_flag | awk -F= '{print $2}')
    fi
    if [[ "$big_cfg" == "True" ]]; then
      if ! set_upgrade_config upgrade_mode 2; then
        die "set upgrade_mode to 2 failed" ${err_rollback_bin}
      fi
    fi
    if ! cd "$GAUSS_UPGRADE_BASE_PATH"; then
      die "$GAUSS_UPGRADE_BASE_PATH cannot access" ${err_rollback_bin}
    fi

    record_step 2
    log "The rollback_bin step is executed successfully. "
  fi

}

function rollback_post() {
  if [ X"$dn_role" == X"standby" ] || [ X"$dn_role" == X"cascade_standby" ]; then
    record_step 4
    log "The rollback_post step is executed successfully."
    return 0
  fi

  parses_step
  if [[ "$current_step" -lt 5 ]]; then
    log "Cannot do rollback_post step"
  else
    if [[ X"$big_cfg" == X ]]; then
      big_cfg=$(grep big_cfg "$GAUSS_TMP_PATH"/version_flag | awk -F= '{print $2}')
    fi
    if [[ "$big_cfg" == "True" ]]; then
      if ! check_db_process; then
        die "Gaussdb is not running" ${err_rollback_post}
      fi
      if ! reload_upgrade_config upgrade_mode 2; then
        die "set upgrade_mode to 2 failed" ${err_upgrade_post}
      fi
      record_step 5
      if exec_sql "$GAUSS_TMP_PATH"/temp_sql/temp_rollback-post_maindb.sql maindb && exec_sql "$GAUSS_TMP_PATH"/temp_sql/temp_rollback-post_otherdb.sql otherdb; then
        debug "rollback post sql successfully"
      else
        die "rollback post sql failed" ${err_rollback_post}
      fi
    fi
    record_step 4
    log "The rollback_post step is executed successfully. "
  fi

}

function upgrade_pre() {
  parses_step
  if [[ "$current_step" -lt 0 ]]; then
    die "Step file may be changed invalid" ${err_upgrade_pre}
  elif [[ "$current_step" -lt 1 ]]; then
    upgrade_pre_step1
    upgrade_pre_step2
  elif [[ "$current_step" -eq 1 ]]; then
    rollback_pre
    upgrade_pre_step2
  else
    log "no need do upgrade_pre step"
  fi
}

function cp_dolphin_upgrade_script_step1() {
  if ls "$GAUSSHOME"/share/postgresql/extension/ | grep -qE "dolphin--(.*)--(.*)sql" ; then
    if cp -f "$GAUSSHOME"/share/postgresql/extension/dolphin--*--*sql "$GAUSS_TMP_PATH"/ ; then
      log "cp dolphin upgrade script step1[upgrade_pre] successfully"
    else
      die "cp dolphin upgrade script step1[upgrade_pre] failed" ${err_upgrade_pre}
    fi
  fi
}

function cp_dolphin_upgrade_script_step2() {
  if ls "$GAUSS_TMP_PATH/" | grep -qE "dolphin--(.*)--(.*)sql" ; then
    if cp -f "$GAUSS_TMP_PATH"/dolphin--*--*sql "$GAUSSHOME"/share/postgresql/extension/ ; then
      log "cp dolphin upgrade script step1[upgrade_bin] successfully"
    else
      die "cp dolphin upgrade script step1[upgrade_bin] failed" ${err_upgrade_pre}
    fi
  fi
}

function upgrade_pre_step1() {
  check_disk
  check_version
  check_cluster_state
  if [[ "$big_cfg" == "True" ]]; then
    prepare_sql_all
  fi
  bak_gauss
  cp_dolphin_upgrade_script_step1
  record_step 1
}

function upgrade_pre_step2() {
  if [[ X"$big_cfg" == X ]]; then
    big_cfg=$(grep big_cfg "$GAUSS_TMP_PATH"/version_flag | awk -F= '{print $2}')
  fi
  if [[ "$big_cfg" == "True" ]]; then
    if ! check_db_process; then
      die "Gaussdb is not running" ${err_upgrade_pre}
    fi
    # set upgrade_mode to 2
    if ! reload_upgrade_config upgrade_mode 2; then
      die "set upgrade_mode to 2 failed" ${err_upgrade_pre}
    fi

    if exec_sql "$GAUSS_TMP_PATH"/temp_sql/temp_upgrade_maindb.sql maindb && exec_sql "$GAUSS_TMP_PATH"/temp_sql/temp_upgrade_otherdb.sql otherdb; then
      debug "exec pre sql successfully"
    else
      die "exec pre sql failed" ${err_upgrade_pre}
    fi
  fi
  record_step 2
  log "The upgrade_pre step is executed successfully. "
}

function upgrade_bin() {
  parses_step
  cp_dolphin_upgrade_script_step2
  if [[ "$current_step" -lt 0 ]]; then
    die "Step file may be changed invalid" ${err_upgrade_bin}
  elif [[ "$current_step" -lt 2 ]]; then
    die "exec upgrade pre first" ${err_upgrade_bin}
  elif [[ "$current_step" -gt 3 ]]; then
    log "no need do upgrade_bin step"
    return
  fi
  upgrade_bin_check
  upgrade_bin_step4
}

function upgrade_bin_check() {
  # Before performing the upgrade_bin operation, check if the gaussdb process exists.
  # If it exists, it indicates that the gaussdb process was not stopped after the upgrade_pre.
  if gs_ctl query -D ${GAUSSDATA} > /dev/null; then
    local error_msg="Before performing the upgrade_bin, it is necessary to ensure that the old gaussdb \
process has been stopped, but the current gaussdb is running."
    die "$error_msg" ${err_upgrade_bin}
  fi
  debug "The old process has been stopped; you can proceed with executing upgrade_bin."
}

function upgrade_bin_step4() {
  record_step 3
  guc_delete
  if [[ X"$big_cfg" == X ]]; then
    big_cfg=$(grep big_cfg "$GAUSS_TMP_PATH"/version_flag | awk -F= '{print $2}')
  fi
  if [[ X"$old_cfg" == X ]]; then
    old_cfg=$(grep old_cfg "$GAUSS_TMP_PATH"/version_flag | awk -F= '{print $2}')
  fi
  if [[ "$big_cfg" == "True" ]]; then
    if ! echo " -u $old_cfg" > "$GAUSSHOME"/bin/start_flag;then
      die "Create $GAUSSHOME/bin/start_flag file failed" ${err_upgrade_bin}
    fi
    if ! start_dbnode "$old_cfg"; then
      die "Start gaussdb failed" ${err_upgrade_bin}
    fi
  fi
  record_step 4
  log "The upgrade_bin step is executed successfully. "
}

function check_real_gaussdb_version() {
  log "Checking the real gaussdb version."
  local real_gaussdb_version
  real_gaussdb_version=$(gsql -tA -d postgres -p $GAUSS_LISTEN_PORT -c "select version();")
  if [[ $? -ne 0 ]]; then
    die "Get real gaussdb version failed" ${err_upgrade_bin}
  fi
  local new_version=""
  new_version=`sed -n 3p $UPGRADE_NEW_PKG_PATH/version.cfg`
  debug "new_version: $new_version"
  debug "real_gaussdb_version: $real_gaussdb_version"
  if ! echo "$real_gaussdb_version" | grep "$new_version" > /dev/null; then
    die "real gaussdb version is still old!" ${err_upgrade_bin}
  fi
  log "Check the real gaussdb version successfully."
}

function check_cluster_state() {
  log "Checking the cluster state."
  debug "dn_role: $dn_role"
  if [[ "$dn_role" != "primary" ]]; then
    return
  fi
  gs_ctl query -D ${GAUSSDATA} > ${GAUSS_TMP_PATH}/temp_dn_role
  if [[ $? -ne 0 ]]; then
    die "Get cluster state failed" ${err_upgrade_bin}
  fi
  local static_connections=$(grep "static_connections" ${GAUSS_TMP_PATH}/temp_dn_role | awk -F': ' '{print $2}')
  local standby_nums=$(grep "sender_pid" ${GAUSS_TMP_PATH}/temp_dn_role -c)
  if [[ $standby_nums -lt $static_connections ]]; then
    local error_msg="The cluster state is abnormal.\nThe number of standby nodes($standby_nums) is less than \
the number of static connections($static_connections). Please check the standby state."
    die "$error_msg" ${err_upgrade_bin}
  fi

  log "Check the cluster state successfully."
}

function upgrade_post_check() {
  check_real_gaussdb_version
  check_cluster_state
}

function upgrade_post() {
  parses_step
  if [[ "$current_step" -lt 0 ]]; then
    die "Step file may be changed invalid" ${err_upgrade_post}
  elif [[ "$current_step" -lt 4 ]]; then
    die "You should exec upgrade_bin first" ${err_upgrade_post}
  elif [[ "$current_step" -gt 5 ]]; then
    log "no need do upgrade_post step"
    return
  fi

  upgrade_post_check

  if [[ "$current_step" -eq 5 ]]; then
    rollback_post
  fi
  # "$current_step" -eq 4
  upgrade_post_step56
}

function upgrade_post_step56() {
  if [ X"$dn_role" == X"standby" ] || [ X"$dn_role" == X"cascade_standby" ]; then
    record_step 6
    log "The upgrade_post step is executed successfully."
    return 0
  fi

  if [[ X"$big_cfg" == X ]]; then
    big_cfg=$(grep big_cfg "$GAUSS_TMP_PATH"/version_flag | awk -F= '{print $2}')
  fi
  if [[ "$big_cfg" == "True" ]]; then
    if ! check_db_process; then
      die "Guassdb is not running" ${err_upgrade_post}
    fi
    record_step 5

    if exec_sql "$GAUSS_TMP_PATH"/temp_sql/temp_rollback-post_maindb.sql maindb && exec_sql "$GAUSS_TMP_PATH"/temp_sql/temp_rollback-post_otherdb.sql otherdb; then
      debug "upgrade-rollback post sql successfully"
    else
      die "upgrade-rollback post sql failed" ${err_rollback_post}
    fi

    if exec_sql "$GAUSS_TMP_PATH"/temp_sql/temp_upgrade-post_maindb.sql maindb && exec_sql "$GAUSS_TMP_PATH"/temp_sql/temp_upgrade-post_otherdb.sql otherdb; then
      debug "upgrade post sql successfully"
    else
      die "upgrade post sql failed" ${err_upgrade_post}
    fi
  fi
  record_step 6
  log "The upgrade_post step is executed successfully. "
}

function upgrade_commit() {
  parses_step
  if [[ "$current_step" -eq 0 ]]; then
    die "No need commit,upgrade directly" ${err_no_need_commit}
  fi
  if [[ "$current_step" -ne 6 ]]; then
    die "Now you can't commit because the steps are wrong" ${err_upgrade_commit}
  fi
  if ! rm -f "$GAUSSHOME"/bin/start_flag; then
    die "rm $GAUSSHOME/bin/start_flag file failed" ${err_upgrade_commit}
  fi
  if ! reload_upgrade_config upgrade_mode 0; then
    die "set upgrade_mode to 0 failed" ${err_upgrade_commit}
  fi
  # after commit, need to reset step to 0
  record_step 0
  # cleanup
  rm -rf $GAUSS_TMP_PATH $GAUSS_BACKUP_BASE_PATH
  log "The upgrade_commit step is executed successfully."
}

function record_step() {
  local record_file="$GAUSS_TMP_PATH"/record_step.txt
  if echo "$1" > "$record_file" && chmod 600 "$record_file"; then
    debug "record step $1 successfully."
  else
    die "Write step file failed" ${err_inner_sys}
  fi
}

function parses_step() {
  if [[ ! -f "$GAUSS_TMP_PATH"/record_step.txt ]]; then
    current_step=0
  else
    if grep -q [^0-6] "$GAUSS_TMP_PATH"/record_step.txt; then
      die "$GAUSS_TMP_PATH/record_step.txt may be changed manually" ${err_inner_sys}
    else
      current_step=$(cat "$GAUSS_TMP_PATH"/record_step.txt)
    fi
  fi
  debug "current_step is $current_step"
}

function query_start_mode() {
  parses_step
  if [[ "$current_step" -lt 3 ]]; then
    log "Start directly"
    return 0
  fi
  if [[ ! -f "$GAUSS_TMP_PATH"/version_flag ]]; then
    log "Start directly"
    return 0
  fi

  if grep -Ewq "new_version=\w{8}" "$GAUSS_TMP_PATH"/version_flag ; then
    new_version=$(grep new_version "$GAUSS_TMP_PATH"/version_flag | awk -F= '{print $2}')
    if gaussdb -V|grep -wq "$new_version";then
      if grep -q "^big_cfg=True" "$GAUSS_TMP_PATH"/version_flag;then
        old_cfg=$(grep old_cfg "$GAUSS_TMP_PATH"/version_flag | awk -F= '{print $2}')
        log "You must start with -u $old_cfg"
        return 0
      fi
    fi
  fi
  log "Start directly"
  return 0
}

function switch_over() {
  local temp_file="${GAUSS_TMP_PATH}/temp_switch_over"
  if gs_ctl query -D "$GAUSSDATA" > "$temp_file" && chmod 400 "$temp_file"; then
      local_role_temp=$(grep local_role "$temp_file" | head -1 | awk '{print $3}')
      db_state_temp=$(grep db_state "$temp_file" | head -1 | awk '{print $3}')
      peer_role_temp=$(grep peer_role "$temp_file" | head -1 | awk '{print $3}')
      rm -f "$temp_file"
  else
      die "gs_ctl query -D $GAUSSDATA failed"  1
  fi

  if [[ "$local_role_temp" = "Standby" ]] && [[ "$db_state_temp" = "Normal" ]] && [[ "$peer_role_temp" = "Primary" ]]; then
    log "Current node can do switchover"
    if gs_ctl switchover -D "$GAUSSDATA" -f ;then
        log "Switchover success"
    else
        die "Switchover failed"
    fi
  else
    log "no need do switchover"
  fi
}
