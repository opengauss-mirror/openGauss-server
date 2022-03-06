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
 * -------------------------------------------------------------------------
 *
 * bucketinfo.cpp
 *     functions related to bucketinfo
 *
 * IDENTIFICATION
 *     src/gausskernel/optimizer/util/bucketinfo.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "optimizer/bucketinfo.h"
#include "nodes/bitmapset.h"
#include "knl/knl_thread.h"
#include "pgxc/groupmgr.h"

/*
 * @Description: check if the user specified the right bucketids
 *               report error if the buketid is out of range

 * @in RangeVar:  the user sql sematic input
 */
bool hasValidBuckets(RangeVar* r, int bucketmapsize)
{
    if (!(r->isbucket))
        return false;

    /* check for valid range of bucket ids */
    ListCell* lc = NULL;
    Bitmapset* bms = NULL;
    foreach (lc, r->buckets) {
        uint2 v = (uint2)intVal(lfirst(lc));
        if (v >= bucketmapsize) {
            int eleval = (bucketmapsize == 0 ? PANIC : ERROR);
            ereport(eleval,
                (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                    errmsg("buckets id %d of table \"%s\" is outsize range [%d,%d]",
                        v,
                        r->relname,
                        0,
                        bucketmapsize - 1)));
        }

        /* save it for later duplicate detection */
        bms = bms_add_member(bms, v);
    }

    int num_duplicate_buckets = list_length(r->buckets) - bms_num_members(bms);
    if (num_duplicate_buckets) {
        ereport(ERROR, (errmsg("%d of duplicate bucket id found", num_duplicate_buckets)));

        /* just for complier */
        return false;
    }

    return true;
}

/*
 * @Description: convert range var buckets to int list

 * @in RangeVarBuckets:  the user sql sematic input integer list
 */
List* RangeVarGetBucketList(RangeVar* rangeVar)
{
    ListCell* lc = NULL;
    List* result = NIL;

    foreach (lc, rangeVar->buckets) {
        uint2 v = (uint2)intVal(lfirst(lc));

        result = lappend_int(result, v);
    }

    return result;
}

/*
 * @Description: get the dot format of bucket list
 *
 *           the dot format replace the continuous bucket ids
 *           with one upper bound and one lower bounds in between
 *           with two dots. for example, a pruning bucketlist
 *           0 1 2 3 5 6 7 which 4 is pruned. the dot format
 *           can be like 0 .. 3 5 .. 7
 *
 *           the algorithm is like this, first we alloc a dot[] arry
 *           to indicate if the bucket id should be printed as dot.
 *           if the dot[i] is 1 the arr[i] should be printed as dot
 *           we start from the 3rd element in the array. if the former
 *           two numbers are continuous, the 3rd-1 element should be
 *           printed as dot.
 *           for example
 *              bucket id array  0 1 2 3 5 6 7
 *                 the arr[2] element's previous 2 element is continuous,
 *                 so the 2-1 element should be print as dot so dot[1]=1
 *                 the arr[3] element's previous 2 element is continuous,
 *                 so the 3-1 element should be print as dot so dot[2]=1
 *                 the arr[4] element's previous 2 element is not continuous,
 *                 so the 4-1 element should not be print as dot.
 *              the dot array is 0 1 1 0 0 1 0
 *
 * @in BucketInfo:  the targeted BucketInfo
 * @out StringInfo: where string form of BucketInfo will be placed
 */
static StringInfo bucketInfoToDotString(BucketInfo* bucket_info)
{
    StringInfo str = makeStringInfo();
    int* dot = (int*)palloc0(sizeof(int) * BUCKETDATALEN);
    int* arr = (int*)palloc0(sizeof(int) * BUCKETDATALEN);
    int len = 0;
    ListCell* lc = NULL;

    foreach (lc, bucket_info->buckets) {
        arr[len++] = lfirst_int(lc);
    }

    /*
     * construct the dot array based on rule:
     *
     *  start from the 3rd element of the array. if the preceding two
     *  element is continuous, the i-1 element should be printed as dot
     *  so set the dot[i - 1] = 1
     *
     *      bucket id array  0 1 2 3 5 6 7
     *      the dot array is 0 1 1 0 0 1 0
     */
    for (int i = 2; i < len; i++) {
        if (arr[i - 2] + 2 == arr[i] && arr[i - 1] + 1 == arr[i]) {
            dot[i - 1] = 1;
        }
    }

    /*
     * assemble the final string result
     */
    for (int i = 0; i < len; i++) {
        if (dot[i]) {
            /* this id should be print as dot */
            appendStringInfoString(str, ".. ");

            /*
             * find the right most dot, so the next
             * iteration will i++ and the dot[i]==0
             * hence the number will be printed
             */
            while ((i + 1) < len && dot[i + 1])
                i++;
        } else {
            appendStringInfo(str, "%d ", arr[i]);
        }
    }

    pfree(dot);
    pfree(arr);

    return str;
}

/*
 * @Description: get the not format repsentation of bucketinfo array
 *
 *      for each bucketid not in the bucket list
 *      which might have be pruned based on quals in bucketpruning.cpp
 *      we add a ! in front of the bucket id and return it.
 *      for example, if we have a pruned bucket list 0 1 2 4 5 6 7
 *      which 3 is pruned, so the not format is !3
 *
 * @in BucketInfo:  the targeted BucketInfo
 * @out StringInfo: where string form of BucketInfo will be placed
 */
static StringInfo bucketInfoToNotString(BucketInfo* bucket_info)
{
    StringInfo str = makeStringInfo();
    int* arr = (int*)palloc0(sizeof(int) * BUCKETDATALEN);
    int len = 0;
    ListCell* lc = NULL;

    foreach (lc, bucket_info->buckets) {
        arr[len++] = lfirst_int(lc);
    }

    /*
     * i: index of the [0, BUCKETDATALEN]
     * j: index of the pruned bucket id array
     *
     *    i
     *    v
     *    0 1 2 3 4 5 6 7
     *    0 1 2 3 5 6 7  (note 4 is missing)
     *    ^
     *    j
     */
    for (int i = 0, j = 0; i < BUCKETDATALEN;) {
        if (arr[j] != i) {
            /*
             * i: index of the [0, BUCKETDATALEN]
             * j: index of the pruned bucket id array
             *
             *            i
             *            v
             *    0 1 2 3 4 5 6 7
             *    0 1 2 3 5 6 7  (note 4 is missing)
             *            ^
             *            j
             */
            appendStringInfo(str, "!%d ", i);
            i++;
        } else {
            i++;
            j++;
        }
    }

    pfree(arr);

    return str;
}

/*
 * @Description: show the string representation of BucketInfo
 *
 *      in order to save display space, we have two ways to show
 *      the bucket info, the dot format and the not format.
 *      Suppose we have selected buckets as 0 1 2 3 4 7 8
 *      (1) the dot format: if the bucket id is continuous, we
 *          only show the lower & upper bounds of bucket id and
 *          place .. in between. In this way the above selected
 *          buckets will be compressed as:
 *                    Selected Buckets:  0 .. 4 7 8
 *      (2) the not format: only show the pruned buckets with a !
 *          in front of its index. In this way the above selected
 *          buckets will be compressed as:
 *                    Selected Buckets:  !5 !6
 *      We choose the shorter one of dot/not format as the result
 *
 * @in BucketInfo:  the targeted BucketInfo
 * @out StringInfo: where string form of BucketInfo will be placed
 */
char* bucketInfoToString(BucketInfo* bucket_info)
{
    if (bucket_info == NULL) {
        return "bucket_info is null";
    }
    if (bucket_info->buckets == NIL) {
        return "all";
    } else {
        StringInfo dotstr = bucketInfoToDotString(bucket_info);
        StringInfo notstr = bucketInfoToNotString(bucket_info);
        /* use the shorter one */
        if (dotstr->len <= notstr->len) {
            return dotstr->data;
        } else {
            return notstr->data;
        }
    }
}
