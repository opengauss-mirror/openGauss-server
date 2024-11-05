/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2021, openGauss Contributors
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
 * jsonpath.h
 *        definition of jsonpath's parse result.
 *
 *
 * IDENTIFICATION
 *        src/include/utils/jsonpath.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef JSONPATH_H
#define JSONPATH_H

#include "utils/json.h"

/* jsonpath .y 和.l 中使用 BEGIN */
#define YY_NO_UNISTD_H
union YYSTYPE;
/* jsonpath .y 和.l 中使用 END */

typedef enum JsonPathItemType {
    JPI_NULL,
    JPI_ABSOLUTE_START,
    JPI_ARRAY,
    JPI_OBJECT
} JsonPathItemType;

typedef struct JsonPathItem {
    JsonPathItemType type;
    JsonPathItem* next;
} JsonPathItem;

typedef struct JsonPathArrayStep {
    JsonPathItemType type;
    JsonPathItem* next;
    List* indexes;
} JsonPathArrayStep;

typedef struct JsonPathObjectStep {
    JsonPathItemType type;
    JsonPathItem* next;
    char* fieldName;
} JsonPathObjectStep;

typedef struct JsonPathParseItem {
    JsonPathItem* head;
    JsonPathItem* tail;
} JsonPathParseItem;

extern JsonPathItem* MakeItemType(JsonPathItemType type);
extern JsonPathParseItem* MakePathParseItem();
extern JsonPathItem* ParseJsonPath(const char* str, int len);

#endif   /* JSONPATH_H */