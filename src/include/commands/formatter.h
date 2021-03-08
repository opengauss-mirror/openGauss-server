/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
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
 * ---------------------------------------------------------------------------------------
 * 
 * formatter.h
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/commands/formatter.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef FORMATTER_H_
#define FORMATTER_H_

#include "c.h"
#include "nodes/pg_list.h"
#include "bulkload/utils.h"

typedef enum { ALIGN_INVALID, ALIGN_LEFT, ALIGN_RIGHT } FieldAlign;

struct FieldDesc {
    char* fieldname;
    char* nullString;
    int fieldPos;
    int fieldSize;
    int attnum;
    FieldAlign align;
};

struct Formatter {
    FileFormat format;
    int nfield;
};

struct FixFormatter : public Formatter {
    int lineSize;
    FieldDesc* fieldDesc;
    bool forceLineSize;
};

#endif /* FORMATTER_H_ */
