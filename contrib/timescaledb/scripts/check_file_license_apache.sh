#!/bin/bash
# Copyright (c) 2026 Huawei Technologies Co., Ltd.
# openGauss-server is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#          http://license.coscl.org.cn/MulanPSL2
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.
# Copyright (c) 2017-2022 Timescale, Inc. All Rights Reserved.
# Copyright (c) 2016-2017 440 Labs Inc. dba Timescale. All Rights Reserved


FLAG=${1}
FILE=${2}
SCRIPTPATH="$( cd "$(dirname "${0}")" ; pwd -P )"
TIMESCALE_LOCATION=`dirname ${SCRIPTPATH}`

LICENSE_FILE=
LICENSE_STRING=
FIRST_COMMENT=

if [[ "${FLAG}" == '-c' ]]; then
    LICENSE_FILE=${SCRIPTPATH}/c_license_header.h
    LICENSE_STRING=`awk 'BEGIN {ORS=""}{if($1 == "*/") {print; exit;}} {print}' ${LICENSE_FILE}`
    FIRST_COMMENT=`awk 'BEGIN {ORS=""}{if($1 == "*/") {print; exit;}} {print}' ${FILE}`
elif [[ "${FLAG}" == '-s' ]]; then
    LICENSE_FILE=${SCRIPTPATH}/sql_license.sql
    LICENSE_STRING=`awk 'BEGIN {ORS=""}{if($1 == "") {print; exit;}} {print}' ${SCRIPTPATH}/sql_license.sql`
    FIRST_COMMENT=`awk 'BEGIN {ORS=""}{if($1 == "") {print; exit;}} {print}' ${FILE}`
elif [[ "${FLAG}" == '-t' ]]; then
    LICENSE_FILE=${SCRIPTPATH}/test_license.sql
    LICENSE_STRING=`awk 'BEGIN {ORS=""}{if($1 == "") {print; exit;}} {print}' ${SCRIPTPATH}/test_license.sql`
    FIRST_COMMENT=`awk 'BEGIN {ORS=""}{if($1 == "") {print; exit;}} {print}' ${FILE}`
else
    echo "Unkown flag" ${1}
    exit 1;
fi

if [[ "${FIRST_COMMENT}" != "${LICENSE_STRING}" ]]; then
    echo ${FILE#"$TIMESCALE_LOCATION/"} "lacks a license header. Add";
    echo
    cat ${LICENSE_FILE}
    echo
    echo "to the top of the file";
    exit 1;
fi

exit 0;
