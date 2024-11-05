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
 * -------------------------------------------------------------------------
 *
 * jsonpath.cpp
 *
 * IDENTIFICATION
 *    src/common/backend/utils/adt/jsonpath.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "utils/json.h"
#include "utils/jsonapi.h"
#include "utils/jsonpath.h"

/*
 * The helper functions below allocate and fill JsonPathParseItem's of various
 * types.
 */

static bool IsJsonText(text* t);
static JsonPathItem* MakeItemBasic(JsonPathItemType type);
static JsonPathItem* MakeItemArrayStep();
static JsonPathItem* MakeItemObjectStep();

JsonPathItem* MakeItemType(JsonPathItemType type)
{
    JsonPathItem* v = NULL;
    switch (type) {
        case JPI_NULL:
        case JPI_ABSOLUTE_START:
            v = MakeItemBasic(type);
            break;
        case JPI_ARRAY:
            v = MakeItemArrayStep();
            break;
        case JPI_OBJECT:
            v = MakeItemObjectStep();
            break;
        default:
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                 errmsg("unrecognized json path item type: %d", type)));
    }
    return v;
}

JsonPathItem* MakeItemBasic(JsonPathItemType type)
{
    JsonPathItem* v = (JsonPathItem*)palloc(sizeof(JsonPathItem));

    v->type = type;
    v->next = NULL;

    return v;
}
JsonPathItem* MakeItemArrayStep()
{
    JsonPathArrayStep* v = (JsonPathArrayStep*)palloc(sizeof(JsonPathArrayStep));

    v->type = JPI_ARRAY;
    v->next = NULL;
    v->indexes = NIL;

    return (JsonPathItem*)v;
}

JsonPathItem* MakeItemObjectStep()
{
    JsonPathObjectStep* v = (JsonPathObjectStep*)palloc(sizeof(JsonPathObjectStep));

    v->type = JPI_OBJECT;
    v->next = NULL;
    v->fieldName = NULL;

    return (JsonPathItem*)v;
}

JsonPathParseItem* MakePathParseItem()
{
    JsonPathParseItem* v = (JsonPathParseItem*)palloc(sizeof(JsonPathParseItem));

    v->head = NULL;
    v->tail = NULL;

    return (JsonPathParseItem*)v;
}