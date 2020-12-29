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
 * list.h
 *        definitions for instruments workload
 * 
 * 
 * IDENTIFICATION
 *        src/include/instruments/list.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef LIST_H
#define LIST_H
#include "c.h"
#include "lib/stringinfo.h"
/* top dn info*/
typedef struct WLMTopDnInfo {
    char nodeName[NAMEDATALEN];
    int64 data;
} WLMTopDnInfo;

/* list of top dn*/
typedef struct WLMTopDnList {
    WLMTopDnInfo topDn;
    WLMTopDnList* nextTopDn;
} WLMTopDnList;

void InsertElemSortTopN(WLMTopDnList** head, WLMTopDnList** elem, int topN);
void DeleteList(WLMTopDnList** head);
void GetTopNInfo(WLMTopDnList* head, char name[][NAMEDATALEN], int* values, int topN);
void GetTopNInfoJsonFormat(StringInfoData topNstr, char nodeName[][NAMEDATALEN], int* value, int topN);
#endif
