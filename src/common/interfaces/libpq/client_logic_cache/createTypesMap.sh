#!/bin/bash
# Copyright: (c) Huawei Technologies Co., Ltd. 2021. All rights reserved.
TEMP_FILE=types_to_oid_tmp.h
FILE=types_to_oid.h
CPPFILE=types_to_oid.cpp
echo "#pragma once" > ${TEMP_FILE}
echo "#include \"client_logic_common/cstring_oid_map.h\"" >> ${TEMP_FILE}
echo "#include \"client_logic_common/client_logic_utils.h\"" >> ${TEMP_FILE}
echo "static CStringOidMap typesTextToOidMap;" >> ${TEMP_FILE}
cat dataTypes.def | sed 's/#define //' | sed 's/OID//' | tr '[:upper:]' '[:lower:]' | awk '{print "typesTextToOidMap.set(\""$1"\""","$2");"}' >> ${TEMP_FILE}

mv ${TEMP_FILE} ${FILE}
