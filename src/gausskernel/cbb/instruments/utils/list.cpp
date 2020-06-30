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
 * list.cpp
 *        Used in CPU/Memroy TOP 5 DNs
 * 
 * IDENTIFICATION
 *	  src/gausskernel/cbb/instruments/utils/list.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "instruments/list.h"
#include "securec.h"
#include "utils/elog.h"
#include "utils/palloc.h"

using namespace std;
void InsertElemSortTopN(WLMTopDnList** head, WLMTopDnList** elem, int topN)
{
    WLMTopDnList* p1 = NULL;
    WLMTopDnList* prep1 = NULL;
    WLMTopDnList* ppre = NULL;
    prep1 = (*head)->nextTopDn;

    if (prep1 == NULL) {
        (*head)->nextTopDn = (*elem);
        (*elem)->nextTopDn = NULL;
        return;
    }
    ppre = *head;
    p1 = prep1->nextTopDn;
    int i = 1;
    bool done = false;

    while (p1 != NULL && i <= topN) { /* if prep1->nextTopDn == null, prep1 point to last elem */
        if ((*elem)->topDn.data >= prep1->topDn.data && !done) {
            (*elem)->nextTopDn = prep1;
            ppre->nextTopDn = (*elem);
            done = true; /* insert elem into list before prep1 */
            i++;
            ppre = ppre->nextTopDn;
        } else if ((*elem)->topDn.data >= p1->topDn.data && !done) {
            (*elem)->nextTopDn = p1;
            prep1->nextTopDn = (*elem);
            done = true; /* insert elem into list behind prep1 */
            i++;
            ppre = ppre->nextTopDn;
            prep1 = prep1->nextTopDn;
        } else {
            ppre = ppre->nextTopDn;
            prep1 = prep1->nextTopDn;
            p1 = p1->nextTopDn;
            i++;
        }
    }

    if (p1 == NULL && i < topN && !done) {
        if ((*elem)->topDn.data >= prep1->topDn.data) {
            (*elem)->nextTopDn = prep1;
            ppre->nextTopDn = (*elem);
        } else {
            prep1->nextTopDn = (*elem);
            (*elem)->nextTopDn = NULL;
        }
    } else if (i > topN && done) {
        pfree_ext(prep1); /* free last one of list */
        ppre->nextTopDn = NULL;
    } else if (i >= topN && !done) {
        pfree_ext((*elem)); /* free the elem, if not insert into list */
    }
}

void DeleteList(WLMTopDnList** head)
{
    if (head == NULL) {
        return;
    }
    WLMTopDnList* p = NULL;
    while ((*head) != NULL) {
        p = (*head)->nextTopDn;
        pfree_ext(*head);
        (*head) = p;
    }
}

void GetTopNInfo(WLMTopDnList* head, char nodeName[][NAMEDATALEN], int* value, int topN)
{
    if (head == NULL || topN == 0) {
        return;
    }
    WLMTopDnList* p1 = head->nextTopDn;
    int i = 0;
    while (p1 != NULL && topN > 0) {
        errno_t rc = memcpy_s(&nodeName[i], NAMEDATALEN, p1->topDn.nodeName, strlen(p1->topDn.nodeName));
        securec_check(rc, "\0", "\0");
        *(value++) = p1->topDn.data;
        p1 = p1->nextTopDn;
        i++;
        topN--;
    }
}

void GetTopNInfoJsonFormat(StringInfoData topNstr, char nodeName[][NAMEDATALEN], int* value, int topN)
{
    if (topN <= 0) {
        return;
    }
    appendStringInfo(&topNstr, "%s", "[");
    for (int i = 0; i < topN; ++i) {
        if (strlen(nodeName[i]) == 0) {
            continue;
        }
        if (i > 0) {
            appendStringInfo(&topNstr, "%s", ",");
        }
        appendStringInfo(&topNstr, "%s", "{");
        appendStringInfo(&topNstr, "\"%s\":", nodeName[i]);
        appendStringInfo(&topNstr, "%d", value[i]);
        appendStringInfo(&topNstr, "%s", "}");
    }
    appendStringInfo(&topNstr, "%s", "]");
}
