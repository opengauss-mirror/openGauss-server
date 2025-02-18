/*
 * Copyright (c) 2024 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * --------------------------------------------------------------------------------------
 *
 * bfile.h
 *
 *  Definition about bfile type.
 *
 * IDENTIFICATION
 *        src/include/utils/bfile.h
 *
 * --------------------------------------------------------------------------------------
 */
#ifndef SRC_INCLUDE_UTILS_BFILE_H
#define SRC_INCLUDE_UTILS_BFILE_H

#include "fmgr.h"

typedef struct BFile {
    int32 vl_len_;
    int32 slot_id;
    int32 location_len;
    int32 filename_len;
    char data[FLEXIBLE_ARRAY_MEMBER];
} BFile;

extern Datum bfilein(PG_FUNCTION_ARGS);
extern Datum bfileout(PG_FUNCTION_ARGS);
extern Datum bfilesend(PG_FUNCTION_ARGS);
extern Datum bfilerecv(PG_FUNCTION_ARGS);
extern Datum bfilename(PG_FUNCTION_ARGS);
#endif /* SRC_INCLUDE_UTILS_BFILE_H */