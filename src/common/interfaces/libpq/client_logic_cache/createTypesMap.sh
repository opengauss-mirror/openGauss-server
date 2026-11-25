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
TEMP_FILE=types_to_oid_tmp.h
FILE=types_to_oid.h
CPPFILE=types_to_oid.cpp
echo "#pragma once" > ${TEMP_FILE}
echo "#include \"client_logic_common/cstring_oid_map.h\"" >> ${TEMP_FILE}
echo "#include \"client_logic_common/client_logic_utils.h\"" >> ${TEMP_FILE}
echo "static CStringOidMap typesTextToOidMap;" >> ${TEMP_FILE}
cat dataTypes.def | sed 's/#define //' | sed 's/OID//' | tr '[:upper:]' '[:lower:]' | awk '{print "typesTextToOidMap.set(\""$1"\""","$2");"}' >> ${TEMP_FILE}

mv ${TEMP_FILE} ${FILE}
