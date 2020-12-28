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
 * common_reader.cpp
 *	  As for text, csv format file, we have some common function that can read data from file.
 *    we extract these funtions and put them into class.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/dfs/common_reader.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "common_reader.h"

#include "access/dfs/dfs_query.h"
#include "access/dfs/dfs_stream_factory.h"
#include "commands/copy.h"
#include "foreign/foreign.h"
#include "utils/palloc.h"
#include "mb/pg_wchar.h"

namespace dfs {
namespace reader {
CommonReader::CommonReader(ReaderState *reader_state, dfs::DFSConnector *conn, DFSFileType fileFormat)
    : m_reader_state(reader_state),
      m_conn(conn),
      m_memory_context(NULL),
      m_memory_context_parser(NULL),
      m_cur_file_path(NULL),
      m_in_functions(NULL),
      m_typioparams(NULL),
      m_accept_empty_str(NULL),
      m_parser_impl(NULL),
      m_line_buffer(NULL),
      m_raw_fields(NULL),
      m_values(NULL),
      m_nulls(NULL),
      m_line_converted(NULL),
      m_cur_attname(NULL),
      m_cur_attval(NULL),
      m_read_rows(0),
      m_total_rows(0),
      m_is_eof(false),
      m_parser_options(NULL),
      m_fileFormat(fileFormat)
{
}

CommonReader::~CommonReader()
{
    m_typioparams = NULL;
    m_line_converted = NULL;
    m_cur_file_path = NULL;
    m_cur_attname = NULL;
    m_in_functions = NULL;
    m_raw_fields = NULL;
    m_parser_impl = NULL;
    m_parser_options = NULL;
    m_memory_context = NULL;
    m_conn = NULL;
    m_values = NULL;
    m_line_buffer = NULL;
    m_memory_context_parser = NULL;
    m_cur_attval = NULL;
    m_nulls = NULL;
    m_accept_empty_str = NULL;
    m_reader_state = NULL;
}

void CommonReader::Destroy()
{
    /* Clear conn handle */
    if (m_conn != NULL) {
        delete (m_conn);
        m_conn = NULL;
    }

    /* Clear the internal memory context. */
    if (m_memory_context != NULL && m_memory_context != CurrentMemoryContext) {
        MemoryContextDelete(m_memory_context);
    }

    if (m_memory_context_parser != NULL && m_memory_context_parser != CurrentMemoryContext) {
        MemoryContextDelete(m_memory_context_parser);
    }
}

/* Initialization works */
void CommonReader::begin()
{
    /* memory context */
    m_memory_context =
        AllocSetContextCreate(m_reader_state->persistCtx, "Internal batch reader level context for common reader",
                              ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);

    m_memory_context_parser = AllocSetContextCreate(m_reader_state->persistCtx, "common reader parser",
                                                    ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE,
                                                    ALLOCSET_DEFAULT_MAXSIZE);

    /* switch to text reader memory context */
    MemoryContext old_context = MemoryContextSwitchTo(m_memory_context);

    uint32_t attr_num = m_reader_state->relAttrNum;
    bool *global_read_required = m_reader_state->globalReadRequired;
    Oid in_func_oid = InvalidOid;

    /* init text parse function */
    m_in_functions = (FmgrInfo *)palloc0(attr_num * sizeof(FmgrInfo));
    m_typioparams = (Oid *)palloc0(attr_num * sizeof(Oid));
    m_accept_empty_str = (bool *)palloc0(attr_num * sizeof(bool));

    for (uint32_t i = 0; i < attr_num; ++i) {
        /* it dose not include partition column. */
        if (global_read_required[i]) {
            const Var *var = GetVarFromColumnList(m_reader_state->allColumnList, static_cast<int>(i + 1));
            if (var != NULL) {
                m_accept_empty_str[i] = IsTypeAcceptEmptyStr(var->vartype);

                /* Fetch the input function and typioparam info */
                getTypeInputInfo(var->vartype, &in_func_oid, &m_typioparams[i]);
                fmgr_info(in_func_oid, &m_in_functions[i]);
            }
        }
    }

    /* field pt */
    int max_read_fields = attr_num - m_reader_state->partNum;
    m_raw_fields = (char **)palloc0(max_read_fields * sizeof(char *));

    /* tuple values */
    m_values = (Datum *)palloc(attr_num * sizeof(Datum));
    m_nulls = (bool *)palloc(attr_num * sizeof(bool));

    /* line buffer to store one row tuple data. */
    m_line_buffer = createLineBuffer();

    /* init the parser option. get options from foreign table options and server options. */
    m_parser_options = createParserOption(m_fileFormat, m_reader_state);

    (void)MemoryContextSwitchTo(old_context);
}

/* Handle the tailing works which are not destroyers. */
void CommonReader::end()
{
    collectFileReadingStatus();
}

/*
 *
 */
void CommonReader::collectFileReadingStatus()
{
    if (m_inputstream.get() != NULL) {
        uint64_t local = 0;
        uint64_t remote = 0;

        m_inputstream->getLocalRemoteReadCnt(&local, &remote);

        m_reader_state->remoteBlock += remote;
        m_reader_state->localBlock += local;

        m_reader_state->nnCalls = 0;
        m_reader_state->dnCalls = 0;
    }
}

/*
 * Load the file transfered in.
 * @_in_param filePath: The path of the file to be load.
 * @_in_param fileRestriction: The restrictions built by the desc table.
 * @return true: return true always. As for text, csv file, this file dose
 * not have statitics, we do not filter using restriction.
 *
 */
bool CommonReader::loadFile(char *file_path, List *file_restriction)
{
    /* switch to text reader memory context */
    MemoryContext old_context = MemoryContextSwitchTo(m_memory_context);

    m_cur_file_path = file_path;
    std::string path(file_path);

    collectFileReadingStatus();
    /* create input stream */
    m_inputstream.reset();
    m_inputstream = dfs::InputStreamFactory(m_conn, path, m_reader_state, g_instance.attr.attr_sql.enable_orc_cache);

    /* create parser object according to file format.  */
    if (m_parser_impl == NULL) {
        m_parser_impl = createParser(m_fileFormat);
    }
    m_parser_impl->init(m_inputstream.get(), m_parser_options);

    /* init rows and eof */
    m_read_rows = 0;
    m_total_rows = 0;
    m_is_eof = false;

    /* skip header line */
    if (DFS_CSV == m_fileFormat && m_parser_options->getHeaderOption()) {
        (void)m_parser_impl->skipLine();
    }

    (void)MemoryContextSwitchTo(old_context);

    return true;
}

/*
 * Fetch the next batch including 1000 or less(reach the end of file)
 * rows which meets the predicates.
 * @_in_param batch: The batch to be filled with the proper tuples.
 * @return rows: The number of rows which has been read in the file,
 *					   it isgreater than or equal to the size of batch returned.
 */
uint64_t CommonReader::nextBatch(VectorBatch *batch, uint64_t cur_rows_in_file, DFSDesc *dfs_desc,
                                 FileOffset *file_offset)
{
    uint64_t read_rows = 0;
    int ret = 0;

    /* setup error callback, must remove befor return */
    ErrorContextCallback errcontext;
    errcontext.callback = (CommonReader::ErrorCallback);
    errcontext.arg = (void *)this;
    errcontext.previous = t_thrd.log_cxt.error_context_stack;
    t_thrd.log_cxt.error_context_stack = &errcontext;

    /* switch to text reader memory context */
    MemoryContext old_context = MemoryContextSwitchTo(m_memory_context);

    /* reset parser function memory context */
    MemoryContextReset(m_memory_context_parser);

    /* set enable compatible illegal char */
    u_sess->cmd_cxt.bulkload_compatible_illegal_chars = (m_reader_state->checkEncodingLevel == LOW_ENCODING_CHECK);

    int max_read_fields = m_reader_state->relAttrNum - m_reader_state->partNum;

    while (!m_is_eof && read_rows < BatchMaxSize) {
        /* check interrupts */
        CHECK_FOR_INTERRUPTS();
        m_line_buffer->reset();
        m_line_converted = NULL;

        /* read a line */
        ret = m_parser_impl->readLine(m_line_buffer);
        if (ret == RESULT_SUCCESS) {
            char *buf = m_line_buffer->getLineBuffer();
            int len = m_line_buffer->getLineLength();
            /* trim eol */
            trimEol(buf, len);

            /* like gds, always check encoding */
            char *cvt = pg_any_to_server(buf, len, m_reader_state->fdwEncoding);
            len = strlen(cvt);

            /* recored for error callback */
            m_line_converted = cvt;

            /* read fields */
            int read_fields = m_parser_impl->getFields(cvt, len, m_raw_fields, max_read_fields);

            /* parse field */
            parserFields(m_raw_fields, read_fields);

            /* add to batch */
            fillVectorBatch(batch, m_values, m_nulls, read_rows);

            ++read_rows;
            m_read_rows++;

            if (cvt != buf) {
                m_line_buffer->reset();
                pfree_ext(cvt);
            }
        } else if (ret == RESULT_EOF) {
            m_is_eof = true;
        } else if (ret == RESULT_BUFFER_FULL) {
            ereport(ERROR, (errcode(ERRCODE_BAD_COPY_FILE_FORMAT), errmsg("line is larger than 1GB.")));
        }
    }

    /* reset enable compatible illegal char */
    u_sess->cmd_cxt.bulkload_compatible_illegal_chars = false;

    (void)MemoryContextSwitchTo(old_context);

    /* remove error callback */
    t_thrd.log_cxt.error_context_stack = errcontext.previous;

    return read_rows;
}

/*
 * @Description: Fetch the next batch by tid
 * @IN batch: vector batch
 * @IN rowsSkip: the rows to be skipped before reading
 * @Return: true if tid row is satisfied, or return false
 * @See also:
 */
void CommonReader::nextBatchByContinueTids(uint64_t rowsSkip, uint64_t rowsToRead, VectorBatch *batch,
                                           uint64_t startOffset, DFSDesc *dfsDesc, FileOffset *fileOffset)
{
}

/*
 * Add a bloom filter into the reader.
 * @_in_param bloomFilter: The bloom filter to be added.
 * @_in_param colIdx: The index of the column on which to add the bloom filter.
 */
void CommonReader::addBloomFilter(filter::BloomFilter *bloomfilter, int colidx, bool is_runtime)
{
}

/*
 * Copy bloomFilter to runTimeBloomFilter
 */
void CommonReader::copyBloomFilter()
{
}

bool CommonReader::checkBFPruning(uint32_t colID, char *colValue)
{
    return false;
}

/*
 * Get the number of all the rows in the current file.
 * @return rows: The number of rows of file.
 * @notice: after call this function. the read file will reach EOF
 */
uint64_t CommonReader::getNumberOfRows()
{
    uint64_t total_rows = 0;
    int ret = RESULT_SUCCESS;

    while (ret != RESULT_EOF) {
        m_line_buffer->reset();
        ret = m_parser_impl->readLine(m_line_buffer);
        if (ret == RESULT_SUCCESS) {
            ++total_rows;
        } else if (ret == RESULT_BUFFER_FULL) {
            const char *file_name = m_inputstream->getName().c_str();
            uint64_t read_offset = m_parser_impl->getReadOffset();
            uint64_t line_len = (uint64_t)((uint)m_line_buffer->getLineLength());
            uint64_t line_offset = (read_offset > line_len) ? read_offset - line_len : 0;
            ereport(ERROR, (errcode(ERRCODE_BAD_COPY_FILE_FORMAT), errmsg("line is larger than 1GB."),
                            errcontext("file \"%s\", offset %lu", file_name, line_offset)));
        }
    }

    m_total_rows = total_rows;
    m_is_eof = true;
    return total_rows;
}

/*
 * @Description: Set the buffer size of the reader
 * @IN capacity: the buffer size(number of rows)
 * @See also:
 */
void CommonReader::setBatchCapacity(uint64_t capacity)
{
}

/*
 * @Description: file is EOF
 * @IN rowsReadInFile: rows read in file
 * @Return: true if tid file is EOF, or return false
 * @See also:getNumberOfRows
 */
bool CommonReader::fileEOF(uint64_t rows_read_in_file)
{
    /* m_total_rows will set in  getNumberOfRows() for SELECT COUNT(*) */
    bool is_eof = (m_total_rows == 0) ? m_is_eof : (rows_read_in_file >= m_total_rows);
    return is_eof;
}

/*
 * @Description: parer field to tuple
 * @IN raw_fields: field pointer array
 * @IN fields: field number
 * @See also:
 */
void CommonReader::parserFields(char **raw_fields, int fields)
{
    bool *global_read_required = m_reader_state->globalReadRequired;
    uint32_t attr_num = m_reader_state->relAttrNum;
    Form_pg_attribute *attr = m_reader_state->scanstate->ss_currentRelation->rd_att->attrs;
    int rc = EOK;

    rc = memset_s(m_values, (attr_num * sizeof(Datum)), 0, (attr_num * sizeof(Datum)));
    securec_check(rc, "", "");

    rc = memset_s(m_nulls, (attr_num * sizeof(bool)), true, (attr_num * sizeof(bool)));
    securec_check(rc, "", "");

    int field_no = 0;

    for (uint32_t i = 0; i < attr_num; ++i) {
        if (!global_read_required[i]) {
            /* i not partition key then ++field_no */
            if (!list_member_int(m_reader_state->partList, (i + 1))) {
                ++field_no;
            }
            continue;
        }

        /* check missing fields */
        checkMissingFields(field_no, fields, i);

        char *field_str = NULL;

        if (field_no < fields) {
            field_str = raw_fields[field_no];

            /* treat empty C string as NULL if either one of the followings does:
             * 1. A db SQL compatibility requires; or
             * 2. This column donesn't accept any empty string.
             */
            if ((u_sess->attr.attr_sql.sql_compatibility == A_FORMAT || !m_accept_empty_str[i]) &&
                (field_str != NULL && field_str[0] == '\0')) {
                /* for any type, '' = null */
                field_str = NULL;
            }
        } else {
            field_str = NULL;
        }

        /* switch to parser function memory context */
        MemoryContext old_context = MemoryContextSwitchTo(m_memory_context_parser);

        /* record attribute name and string for error callback */
        m_cur_attname = NameStr(attr[i]->attname);
        m_cur_attval = field_str;

        /* process datetype */
        char *date_time_fmt = m_parser_options->getDataTypeFmt(m_typioparams[i]);
        if (date_time_fmt == NULL) {
            m_values[i] = InputFunctionCall(&m_in_functions[i], field_str, m_typioparams[i], attr[i]->atttypmod);
        } else {
            m_values[i] = InputFunctionCallForDateType(&m_in_functions[i], field_str, m_typioparams[i],
                                                       attr[i]->atttypmod, date_time_fmt);
        }

        m_cur_attname = NULL;
        m_cur_attval = NULL;

        (void)MemoryContextSwitchTo(old_context);

        if (field_str != NULL) {
            m_nulls[i] = false;
        }

        ++field_no;
    }

    /* check extra data */
    checkExtraFields(field_no, fields);
}

/*
 * @Description: fill vector batch from tuple
 * @IN/OUT batch: vector batch
 * @IN values: tuple values
 * @IN nulls: tuple nulls
 * @IN/OUT row_index row index for tuple add to vector batch
 * @See also:
 */
void CommonReader::fillVectorBatch(VectorBatch *batch, const Datum *values, const bool *nulls, int row_index)
{
    Assert(batch->m_cols == (int)m_reader_state->relAttrNum);

    bool *global_read_required = m_reader_state->globalReadRequired;

    for (int i = 0; i < batch->m_cols; i++) {
        if (!global_read_required[i]) {
            continue;
        }

        ScalarVector *vec = &(batch->m_arr[i]);
        if (nulls[i]) {
            vec->m_rows++;
            vec->SetNull(row_index);
            continue;
        }

        if (vec->m_desc.encoded) {
            vec->AddVar(values[i], row_index);
        } else {
            vec->m_vals[row_index] = values[i];
        }

        vec->m_rows++;
    }

    batch->m_rows = row_index + 1;
}

/*
 * @Description: check if missing field
 * @IN field_no: field number
 * @IN fields: total field
 * @IN attr_index: attribute index
 * @See also:
 */
void CommonReader::checkMissingFields(int field_no, int fields, int attr_index)
{
    Form_pg_attribute *attr = m_reader_state->scanstate->ss_currentRelation->rd_att->attrs;

    if (field_no > fields || (!m_parser_options->fill_missing_fields && field_no == fields)) {
        ereport(ERROR, (errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
                        errmsg("missing data for column \"%s\"", NameStr(attr[attr_index]->attname))));
    }
}

/*
 * @Description: check if extra data
 * @IN max_field_no: the max field number to read
 * @IN fields: total field parsered from text file
 * @See also:
 */
void CommonReader::checkExtraFields(int max_field_no, int fields) const
{
    /* check for overflowing fields. if cstate->ignore_extra_data is true,  ignore overflowing fields */
    if (max_field_no > 0 && fields > max_field_no && !m_parser_options->ignore_extra_data) {
        ereport(ERROR, (errcode(ERRCODE_BAD_COPY_FILE_FORMAT), errmsg("extra data after last expected column")));
    }
}

/*
 * @Description: trim line buffer eol
 * @IN/OUT buf: line buffer
 * @IN/OUT len: line buffer len
 * @See also:
 */
void CommonReader::trimEol(char *buf, int &len) const
{
    if (len >= 2) {
        if (buf[len - 2] == '\r' && buf[len - 1] == '\n') {
            /*  \r\n */
            len -= 2;
            buf[len] = '\0';
        } else if (buf[len - 1] == '\r' || buf[len - 1] == '\n') {
            /* \r  or \n */
            --len;
            buf[len] = '\0';
        }
    } else if (len >= 1) {
        /* \r  or \n */
        if (buf[len - 1] == '\r' || buf[len - 1] == '\n') {
            --len;
            buf[len] = '\0';
        }
    }
}

/*
 * @Description: add more detail  info about text parser error
 * @IN: callback arg
 * @See also:
 */
void CommonReader::ErrorCallback(void *arg)
{
    CommonReader *common_reader = (CommonReader *)arg;

    const char *file_name = common_reader->m_inputstream->getName().c_str();
    uint64_t read_offset = common_reader->m_parser_impl->getReadOffset();
    uint64_t line_len = (uint64_t)((uint)common_reader->m_line_buffer->getLineLength());
    uint64_t line_offset = (read_offset > line_len) ? read_offset - line_len : 0;

    /* format error_info */
    StringInfo error_info = makeStringInfo();

    /* file name */
    appendStringInfo(error_info, "file \"%s\", ", file_name);
    /* column name and value */
    if (common_reader->m_cur_attname && common_reader->m_cur_attval) {
        appendStringInfo(error_info, "column %s: \"%s\", ", common_reader->m_cur_attname, common_reader->m_cur_attval);
    } else if (NULL != common_reader->m_cur_attval) {
        appendStringInfo(error_info, "column %s: null input, ", common_reader->m_cur_attname);
    }

    /* line string */
    if (common_reader->m_line_converted != NULL) {
        /* line is converted , can be print */
        /* limit output length */
        char *line_buf = limit_printout_length(common_reader->m_line_converted);
        appendStringInfo(error_info, "lineNo %lu, offset %lu: \"%s\"", common_reader->m_read_rows + 1, line_offset,
                         line_buf);
        pfree(line_buf);
        line_buf = NULL;
    } else {
        appendStringInfo(error_info, "lineNo %lu, offset %lu ", common_reader->m_read_rows + 1, line_offset);
    }

    (void)errcontext("%s", error_info->data);
    pfree(error_info->data);
    error_info->data = NULL;
    pfree(error_info);
    error_info = NULL;
}
}  // namespace reader
}  // namespace dfs
