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
 * obs_stream.cpp
 *
 * IDENTIFICATION
 *	  src/gausskernel/optimizer/commands/obs_stream.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/poll.h>

#ifndef OBS_SERVER
#define OBS_SERVER
#endif

#include "storage/parser.h"
#include "bulkload/foreignroutine.h"

#include "access/obs/obs_am.h"
#include "commands/copy.h"
#include "commands/gds_stream.h"
#include "commands/obs_stream.h"
#include "libpq/ip.h"
#include "utils/builtins.h"
#include "storage/smgr/fd.h"
#include "miscadmin.h"

using namespace GDS;

#define OBS_SEGMENT_SIZE (1024 * 1024 * 1024)

#define OBS_NOT_IMPLEMENT \
    ereport(ERROR,        \
        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmodule(MOD_OBS), errmsg("%s not implemented", __FUNCTION__)));

extern List* getDataNodeTask(List* totalTask, const char* dnName);

/* constructor */
OBSStream::OBSStream(FileFormat format, bool is_write)
{
    m_write = is_write;
    m_parser = NULL;
    m_cur_segment = NULL;
    m_cur_segment_num = 0;
    m_buffile = NULL;
    m_cur_segment_offset = 0;
    m_hostname = NULL;
    m_bucket = NULL;
    m_prefix = NULL;
    m_obs_options = NULL;

    /* Set OBS related stuffs allocation memory context */
    SetObsMemoryContext(CurrentMemoryContext);

    if (!m_write)
        m_parser = CreateParser(format);
}

/* de-constructor */
OBSStream::~OBSStream()
{
    UnSetObsMemoryContext();

    if (m_parser != NULL) {
        IGNORE_EXCEPTION(m_parser->destroy(m_parser));
        m_parser = NULL;
    }
}

void OBSStream::VerifyAddr()
{
    /* OBS's url: gsobs://obs_endpoint/bucket_name/[S3_prefix] */
    FetchUrlProperties(m_url.c_str(), &m_hostname, &m_bucket, &m_prefix);

    Assert(m_hostname != NULL && m_bucket != NULL && m_prefix);
}

const char* OBSStream::StartNewSegment(void)
{
    /* start a new segment */
    MemoryContext oldContext = MemoryContextSwitchTo(GetObsMemoryContext());

    m_cur_segment = makeStringInfo();
    appendStringInfo(
        m_cur_segment, "%s_%s_segment.%d", m_prefix, g_instance.attr.attr_common.PGXCNodeName, m_cur_segment_num);

    /* Clear file internal offset */
    m_cur_segment_offset = 0;
    m_cur_segment_num++;

    m_buffile = BufFileCreateTemp(false);
    if (!m_buffile) {
        ereport(ERROR,
            (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                errcode_for_file_access(),
                errmsg("could not write to staging temporary file while exporting segment %s", m_cur_segment->data)));
    }

    (void)MemoryContextSwitchTo(oldContext);
    return (const char*)m_cur_segment->data;
}

int OBSStream::Write(void* src, Size len)
{
    const char* filename = NULL;

    if (!m_buffile) {
        /* start a new segment */
        filename = StartNewSegment();
        elog(LOG, "Start writing data to %s", filename);
    }

    /* Write data content to local buffer file */
    size_t nwrites = BufFileWrite(m_buffile, src, len);
    if (nwrites < len) {
        ereport(ERROR,
            (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                errcode_for_file_access(),
                errmsg("could not write to local temporary buffile: %m")));
    }

    /* Update cur segment cursor */
    m_cur_segment_offset += nwrites;

    /* We are finish the previous segment, we flush it and start a new one */
    if (m_cur_segment_offset >= OBS_SEGMENT_SIZE) {
        this->Flush();

        filename = StartNewSegment();
        elog(LOG, "Start writing data to %s", filename);

        m_cur_segment_offset = 0;
    }

    return nwrites;
}

void OBSStream::Initialize(const char* url, CopyState cstate)
{
    m_url = string(url);
    m_filelist.push_back(url);

    VerifyAddr();

    /* For write case, we don't initialize parser */
    if (m_write)
        return;

    /* init CSV parser param */
    CmdBegin cmd;
    if (IS_CSV(cstate)) {
        cmd.m_escape = cstate->escape[0];
        cmd.m_quote = cstate->quote[0];
    }

    cmd.m_header = cstate->header_line;

    /* Object Parser initialization */
    m_parser->init(m_parser, &cmd, &m_filelist, SOURCE_TYPE_OBS);
}

void OBSStream::Close()
{
    if (m_parser != NULL) {
        m_parser->destroy(m_parser);
        m_parser = NULL;
    }

    if (m_buffile) {
        this->Flush();
    }
}

void initOBSModeState(CopyState cstate, const char* objectpath, List* totalTask)
{
    cstate->copy_dest = COPY_OBS;

    const int OFFSETNUM = 2; /* The offset of Array pointer */

    /*
     * For OBS write path, we only have one task todo to write DN data to target file
     */
    if (cstate->writelineFunc) {
        if (IS_PGXC_COORDINATOR) {
            // check export path is empty
            List* obs_file_list = list_obs_bucket_objects(objectpath,
                cstate->obs_copy_options.encrypt,
                cstate->obs_copy_options.access_key,
                cstate->obs_copy_options.secret_access_key);

            if (list_length(obs_file_list) > 1)
                ereport(ERROR,
                    (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                        errmsg("Write-Only table's output directory %s is not empty", objectpath)));

            /* if the output path is not created manually on S3 website, we should check it is a path or object*/
            if (list_length(obs_file_list) == 1) {
                Value* tempObjStr = (Value*)lfirst(list_head(obs_file_list));
                /*
                 *tempObjStr->val.str starts with 'gsobs',  objectpath starts with 'obs',
                 * therefore it should begin to compare from the second character of tempObjStr->val.str.
                 */
                if (strcmp(tempObjStr->val.str + OFFSETNUM, objectpath) != 0) {
                    ereport(ERROR,
                        (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                            errmsg("Write-Only table's output directory %s is not empty", objectpath)));
                }
            }
            return;
        } else {
            DistFdwFileSegment* segment = makeNode(DistFdwFileSegment);
            DistFdwDataNodeTask* task = makeNode(DistFdwDataNodeTask);
            task->dnName = pstrdup((char*)g_instance.attr.attr_common.PGXCNodeName);
            task->task = lappend(task->task, segment);

            /*
             * Create current DN's exporting segment prefix, in OBSStream::StartNewSegment()
             * will assign actual segno
             */
            StringInfo filename = makeStringInfo();
            appendStringInfo(filename, "%s%s", objectpath, RelationGetRelationName(cstate->rel));
            segment->filename = pstrdup((const char*)filename->data);

            cstate->taskList = lappend(cstate->taskList, segment);
            cstate->curTaskPtr = NULL;

            /* Kick off OBS process round */
            (void)getNextOBS(cstate);

            pfree_ext(filename->data);
            pfree_ext(filename);

            return;
        }
    }

    if (IS_PGXC_DATANODE && totalTask != NIL) {
        /*
         * If the current DN isn't involved in GDS foreign table distribution info
         * the assigned taskList is NULL.
         */
        List* taskList = getDataNodeTask(totalTask, g_instance.attr.attr_common.PGXCNodeName);

        /*
         * If we have u_sess->opt_cxt.query_dop enabled, we will further split task to each smp slot
         * to parrallize the scan process. We don't support this function in current version.
         * The parrallel scan process will be added in future.
         */
        /* removed */

        /* Assign the oritinal tasklist without any modification if no DOP specified */
        cstate->taskList = taskList;

        cstate->curTaskPtr = NULL;

        (void)getNextOBS(cstate);
    }
}

void endOBSModeBulkLoad(CopyState cstate)
{
    Assert(cstate->copy_dest == COPY_OBS);
    if (IS_PGXC_COORDINATOR) {
        /*
         * In OBS importing case, we do all OBS related work in DN, so we have nothing
         * to do in CN, just return.
         */
        return;
    } else if (cstate->io_stream) {
        cstate->io_stream->Close();
        delete ((OBSStream*)cstate->io_stream);
        cstate->io_stream = NULL;
    }
}

/*
 * Fetch a line of data into line buffer and let uper layer to do line-2-heap tuple
 * parsing
 */
bool CopyGetNextLineFromOBS(CopyState cstate)
{
    OBSStream* stream = dynamic_cast<OBSStream*>(cstate->io_stream);
    int result = 0;

    if (stream == NULL) {
        /*
         * If cstate->io_stream is not initialized, it is a case where this DN is
         * not assigned with object loading task, so return true to represent EOF
         */
        return true;
    }

    /* Get real parser to do 'buffered line' read/parse */
    GDS::Parser* parser = stream->GetOBSParser();
    cstate->eol_type = EOL_NL;
    resetStringInfo(&(cstate->line_buf));

retry1:
    /* First try to get a line from line buffer */
    if (!parser->line_buffers.GetNextLine(&cstate->line_buf)) {
        /*
         * No more rows in current line buffer, we are going to read next batch
         * of rows in to line_buffer with in this object
         *
         * Reset line buffer first before do next batch of fecthing
         */
        parser->line_buffers.Reset();

        result = parser->readlines(parser);
        switch (result) {
            case RESULT_BUFFER_FULL:
                /* nothing to do */
                break;
            case RESULT_NEW_ONE:
                /* mark the last line completed if it uncompleted  */
                parser->line_buffers.MarkLastLineCompleted();
                break;
            case RESULT_EOF:
                /* mark the last line completed if it uncompleted  */
                parser->line_buffers.MarkLastLineCompleted();

                /* Return true if get end of current object */
                if (parser->line_buffers.NoMoreProcessed())
                    return true;
                break;
            default:
                Assert(false);
                break;
        }

        goto retry1;
    }

    Assert(cstate->line_buf.len >= 2);
    if (cstate->line_buf.data[cstate->line_buf.len - 2] == '\r') {
        /* process \r\n to \n */
        cstate->line_buf.data[cstate->line_buf.len - 2] = '\n';
        cstate->line_buf.data[cstate->line_buf.len - 1] = '\0';
        cstate->line_buf.len--;
    } else if (cstate->line_buf.data[cstate->line_buf.len - 1] != '\n' &&
               cstate->line_buf.data[cstate->line_buf.len] == '\0') {
        /* process no EOL to \n */
        appendBinaryStringInfo(&(cstate->line_buf), "\n", 1);
    } else if (cstate->line_buf.data[cstate->line_buf.len - 1] == '\r') {
        /* process \r to \n */
        cstate->line_buf.data[cstate->line_buf.len - 1] = '\n';
    }

    return false;
}

/*
 * Start processing next OBS object assigned to current DN, return 'FALSE' indicates
 * there is no more tasks to for this DN and 'TRUE' to start next one
 */
bool getNextOBS(CopyState cstate)
{
    List* taskList = cstate->taskList;
    ListCell* prevTaskPtr = cstate->curTaskPtr;
    ListCell* curTaskPtr = (prevTaskPtr == NULL ? list_head(taskList) : lnext(prevTaskPtr));
    bool is_write = (cstate->writelineFunc != NULL);

    /* we must clean stream object in time. */
    if (cstate->io_stream != NULL) {
        cstate->io_stream->Close();
        delete (OBSStream*)cstate->io_stream;
        cstate->io_stream = NULL;
    }

    /*
     * All tasks have been done or task list maybe empty cstate->io_stream close
     * in end_dist_copy
     */
    if (curTaskPtr == NULL) {
        cstate->curTaskPtr = NULL;

        return false;
    }

    /* Prepare the new task */
    DistFdwFileSegment* segment = (DistFdwFileSegment*)lfirst(curTaskPtr);
    Assert(segment);

    /* set COPY memorytext  for paser */
    u_sess->cmd_cxt.OBSParserContext = cstate->copycontext;

    MemoryContext oldcontext = MemoryContextSwitchTo(cstate->copycontext);
    OBSStream* stream = New(CurrentMemoryContext) OBSStream(cstate->fileformat, is_write);
    (void)MemoryContextSwitchTo(oldcontext);

    cstate->io_stream = stream;
    cstate->curTaskPtr = curTaskPtr;

    stream->set_obs_copy_options(&(cstate->obs_copy_options));

    if (!is_write) {
        /*
         * For OBS read case, we need specify chunksize to content parser and also binding
         * OBS related copy options to source object, this looks a twist implementation, the
         * the reason is that we have to invoke source_next()/source_read() in side of CSV/TXT
         * Parser where the backend runtime structure is not visible(GDS parser legacy issue)
         */
        stream->set_parser_chunksize(cstate->obs_copy_options.chunksize);
        stream->Initialize(segment->filename, cstate);

        /*
         * After source object has been setup up parser->init() we can binding CopyOption
         * pointers there
         */
        stream->set_source_obs_copy_options(&(cstate->obs_copy_options));
    } else {
        stream->Initialize(segment->filename, cstate);
    }

    return true;
}

int OBSStream::ReadMessage(StringInfoData& dst)
{
    OBS_NOT_IMPLEMENT;
    return 0;
}

int OBSStream::Read()
{
    OBS_NOT_IMPLEMENT;
    return 0;
};

int OBSStream::InternalRead()
{
    OBS_NOT_IMPLEMENT;
    return 0;
};

/*
 * Flush current segment to OBS and also free m_buffile handler
 */
void OBSStream::Flush()
{
    if (m_buffile == NULL || m_cur_segment == NULL) {
        /* In case of current DN has no work to do */
        return;
    }

    OBSReadWriteHandler* handler = NULL;

    elog(LOG, "Flushing object '%s' to OBS", m_cur_segment->data);

    string bucket_str = string(m_bucket);
    string object_url = m_url.substr(0, m_url.find(bucket_str)) + bucket_str + "/" + string(m_cur_segment->data);
    handler = CreateObsReadWriteHandler(object_url.c_str(), OBS_WRITE, this->m_obs_options);

    /* Seek to header of temp file as we pass in temp file handler to OBS */
    if (BufFileSeek(m_buffile, 0, 0L, SEEK_SET) != 0) {
        ereport(ERROR,
            (errcode(ERRCODE_DATA_EXCEPTION),
                errcode_for_file_access(),
                errmsg("could not rewind OBS exporting temporary file: %m")));
    }

    /* Flush buffile's content into remote OBS service */
    if (m_cur_segment_offset != write_bucket_object(handler, m_buffile, m_cur_segment_offset)) {
        ereport(ERROR,
            (errcode(ERRCODE_FLUSH_DATA_SIZE_MISMATCH),
                errmsg(
                    "Fail to write object %s on %s", m_cur_segment->data, g_instance.attr.attr_common.PGXCNodeName)));
    }

    /* Close current temp file handler  */
    BufFileClose(m_buffile);
    m_buffile = NULL;

    DestroyObsReadWriteHandler(handler, false);
    handler = NULL;
}

void OBSStream::set_parser_chunksize(uint32_t chunksize)
{
    ((ReadableParser*)m_parser)->buf_len = chunksize;
}

void OBSStream::set_source_obs_copy_options(ObsCopyOptions* options)
{
    ((ReadableParser*)m_parser)->source->m_obs_options = options;
    return;
}

const ObsCopyOptions* OBSStream::get_obs_copy_options()
{
    return m_obs_options;
}

void OBSStream::set_obs_copy_options(ObsCopyOptions* options)
{
    Assert(options->access_key != NULL && options->secret_access_key != NULL);
    m_obs_options = options;
}
