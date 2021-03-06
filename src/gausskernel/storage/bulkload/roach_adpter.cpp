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
 *  roach_adpter.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/bulkload/roach_adpter.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "bulkload/roach_adpter.h"
#include "parser/parse_func.h"
#include "utils/lsyscache.h"
#include "optimizer/streamplan.h"

template bool getNextRoach<true>(CopyState cstate);
template bool getNextRoach<false>(CopyState cstate);
template void initRoachState<true>(CopyState cstate, const char *filename, List *totalTask);
template void initRoachState<false>(CopyState cstate, const char *filename, List *totalTask);

RoachRoutine *initRoachRoutine()
{
    Datum datum;
    RoachRoutine *routine = NULL;
    Oid func_oid;

    func_oid = LookupFuncName(list_make1(makeString("roach_handler")), 0, NULL, false);
    /* check that handler has correct return type */
    if (get_func_rettype(func_oid) != FDW_HANDLEROID)
        ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                        errmsg("function roach_handler must return type \"fdw_handler\"")));

    datum = OidFunctionCall0(func_oid);
    routine = (RoachRoutine *)DatumGetPointer(datum);

    return routine;
}

template <bool import>
bool getNextRoach(CopyState cstate)
{
    List *taskList = cstate->taskList;
    ListCell *prevTaskPtr = cstate->curTaskPtr;
    ListCell *curTaskPtr = (prevTaskPtr == NULL ? list_head(taskList) : lnext(prevTaskPtr));

    Assert(cstate->roach_routine);
    Assert(IsA(cstate->roach_routine, RoachRoutine));
    RoachRoutine *roachroutine = (RoachRoutine *)cstate->roach_routine;

    if (curTaskPtr == NULL || cstate->roach_context != NULL) {
        /* only support one roach url, retrun false to end the import */
        return false;
    }

    DistFdwFileSegment *segment = (DistFdwFileSegment *)lfirst(curTaskPtr);
    Assert(segment);

    const char *mode = import ? "r" : "w";
    void *roach_context = roachroutine->Open(segment->filename, (char *)mode);
    if (roach_context == NULL)
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not open roach %s", cstate->filename)));

    cstate->roach_context = roach_context;
    return true;
}

int copyGetRoachData(CopyState cstate, void *databuf, int minread, int maxread)
{
    Assert(cstate->roach_routine);
    Assert(IsA(cstate->roach_routine, RoachRoutine));
    RoachRoutine *roachroutine = (RoachRoutine *)cstate->roach_routine;

    int bytesread = (int)(roachroutine->Read(databuf, 1, maxread, cstate->roach_context));
    if (roachroutine->Error(cstate->roach_context))
        ereport(ERROR, (errcode(ERRCODE_FILE_READ_FAILED), errmsg("could not read from roach")));

    return bytesread;
}

template <bool import>
void initRoachState(CopyState cstate, const char *filename, List *totalTask)
{
    cstate->copy_dest = COPY_ROACH;

    if (IS_PGXC_DATANODE) {
        // init roach interface
        cstate->roach_routine = (Node *)initRoachRoutine();
        if (!cstate->roach_routine)
            ereport(ERROR,
                    (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("Failed to init roach routine.")));

#ifdef USE_ASSERT_CHECKING
        List *urllist = DeserializeLocations(filename);
        // just support one roach url
        if (list_length(urllist) > 1) {
            ereport(ERROR,
                    (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("can not specify multiple locations")));
        }
#endif

        /* datanode private path = url + nodename */
        char roachPath[PATH_MAX + 1];
        const char *pos = strstr(filename, ROACH_PREFIX);
        pos += ROACH_PREFIX_LEN;
        errno_t ret = snprintf_s(roachPath, sizeof(roachPath), PATH_MAX, "%s/%s", pos,
                                 g_instance.attr.attr_common.PGXCNodeName);
        securec_check_ss(ret, "", "");
        roachPath[PATH_MAX] = '\0';

        /* add to task list */
        List *taskList = NIL;
        DistFdwFileSegment *segment = makeNode(DistFdwFileSegment);
        segment->filename = pstrdup(roachPath);
        taskList = lappend(taskList, segment);
        if (list_length(taskList) == 0) {
            taskList = addNullTask(taskList);
        }
        cstate->taskList = taskList;
        cstate->curTaskPtr = NULL;
        cstate->curReadOffset = 0;

        (void)getNextRoach<import>(cstate);
    }
}

void endRoachBulkLoad(CopyState cstate)
{
    if (IS_PGXC_DATANODE) {
        Assert(cstate->roach_routine);
        Assert(IsA(cstate->roach_routine, RoachRoutine));
        RoachRoutine *roachroutine = (RoachRoutine *)cstate->roach_routine;

        if (cstate->roach_context && roachroutine->Close(cstate->roach_context))
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not close roach %s", cstate->filename)));
    }
}

void exportRoach(CopyState cstate)
{
    Assert(cstate->copy_dest == COPY_ROACH);
    Assert(cstate->fileformat != FORMAT_BINARY);
    errno_t rc = 0;

    // first append the newline to the new record data.
    if (cstate->eol_type == EOL_NL)
        CopySendChar(cstate, '\n');
    else
        CopySendString(cstate, "\r\n");

    StringInfo in = cstate->fe_msgbuf;
    const char *pdata = in->data;
    int dsize = in->len;

    if (cstate->raw_buf_len == RAW_BUF_SIZE) {
        // Flush out and set cstate->raw_buf_len = 0
        exportRoachFlushOut(cstate, true);
    }

    do {
        int avail = (RAW_BUF_SIZE - cstate->raw_buf_len);
        Assert(avail > 0);

        if (dsize <= avail) {
            rc = (errno_t)memcpy_s(cstate->raw_buf + cstate->raw_buf_len, avail, pdata, dsize);
            securec_check(rc, "\0", "\0");
            cstate->raw_buf_len += dsize;
            break;
        }

        rc = (errno_t)memcpy_s(cstate->raw_buf + cstate->raw_buf_len, avail, pdata, avail);
        securec_check(rc, "\0", "\0");
        Assert((cstate->raw_buf_len + avail) == RAW_BUF_SIZE);
        cstate->raw_buf_len = RAW_BUF_SIZE;
        // Flush out and set cstate->raw_buf_len = 0
        exportRoachFlushOut(cstate, false);

        pdata += avail;
        dsize -= avail;
        Assert(dsize > 0);
    } while (true);

    resetStringInfo(in);
}

void exportRoachFlushOut(CopyState cstate, bool isWholeLineAtEnd)
{
    Assert(cstate->roach_routine);
    Assert(IsA(cstate->roach_routine, RoachRoutine));
    RoachRoutine *roachroutine = (RoachRoutine *)cstate->roach_routine;

    Assert(((cstate->roach_context == NULL) && IS_STREAM_PLAN) ||
           (!(cstate->roach_context == NULL) && !IS_STREAM_PLAN));

    if ((cstate->roach_context != NULL) &&
        ((roachroutine->Write(cstate->raw_buf, cstate->raw_buf_len, 1, cstate->roach_context, isWholeLineAtEnd) != 1) ||
         roachroutine->Error(cstate->roach_context)))
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not write to roach")));

    cstate->raw_buf_len = 0;
}
