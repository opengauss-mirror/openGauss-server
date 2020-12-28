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
 * common_parser.cpp
 *	  support the common class for parser.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/dfs/common_parser.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "common_parser.h"
#include "text/text_parser.h"
#include "csv/csv_parser.h"

namespace dfs {
/* the default size of reading data from hdfs server for text, csv format file.  */
#define HDFS_READ_BUFFER_LEN (4 * 1024 * 1024)
/* the default size of reading data from obs server for text, csv format file. */
#define OBS_READ_BUFFER_LEN (32 * 1024 * 1024)

#define MAX_INT32 2147483600

BaseParserOption::BaseParserOption()
    : delimiter(NULL),
      null(NULL),
      fill_missing_fields(false),
      ignore_extra_data(false),
      chunk_size(0),
      fScanState(NULL),
      file_encoding(PG_UTF8)
{
    date_format.str = NULL;
    date_format.fmt = NULL;
    time_format.str = NULL;
    time_format.fmt = NULL;
    timestamp_format.str = NULL;
    timestamp_format.fmt = NULL;
    smalldatetime_format.str = NULL;
    smalldatetime_format.fmt = NULL;
}

void BaseParserOption::initTimeFormat()
{
    if (date_format.str)
        date_format.fmt = get_time_format(date_format.str);

    if (time_format.str)
        time_format.fmt = get_time_format(time_format.str);

    if (timestamp_format.str)
        timestamp_format.fmt = get_time_format(timestamp_format.str);

    if (smalldatetime_format.str)
        smalldatetime_format.fmt = get_time_format(smalldatetime_format.str);
}

char *BaseParserOption::getDataTypeFmt(Oid typioparam)
{
    if (typioparam == DATEOID)
        return (char *)date_format.fmt;
    else if (typioparam == TIMEOID)
        return (char *)time_format.fmt;
    else if (typioparam == TIMESTAMPOID)
        return (char *)timestamp_format.fmt;
    else if (typioparam == SMALLDATETIMEOID)
        return (char *)smalldatetime_format.fmt;
    else
        return NULL;
}

void BaseParserOption::setCommonOptionValue()
{
    if (fScanState->options == NULL) {
        /* this branch is not computing pool. */
        Oid foreignOid = RelationGetRelid(fScanState->ss.ss_currentRelation);
        /* encoding  */
        char *encoding_str = HdfsGetOptionValue(foreignOid, OPTION_NAME_ENCODING);
        if (encoding_str != NULL) {
            file_encoding = pg_char_to_encoding(encoding_str);
        }

        DefElem *def_elem = NULL;
        /* set the fill_missing_fields option.  */
        def_elem = HdfsGetOptionDefElem(foreignOid, OPTION_NAME_FILL_MISSING_FIELDS);
        fill_missing_fields = def_elem ? defGetBoolean(def_elem) : false;
        def_elem = HdfsGetOptionDefElem(foreignOid, OPTION_NAME_IGNORE_EXTRA_DATA);
        ignore_extra_data = def_elem ? defGetBoolean(def_elem) : false;

        /* init_time_format */
        date_format.str = HdfsGetOptionValue(foreignOid, OPTION_NAME_DATE_FORMAT);
        time_format.str = HdfsGetOptionValue(foreignOid, OPTION_NAME_TIME_FORMAT);
        timestamp_format.str = HdfsGetOptionValue(foreignOid, OPTION_NAME_TIMESTAMP_FORMAT);
        smalldatetime_format.str = HdfsGetOptionValue(foreignOid, OPTION_NAME_SMALLDATETIME_FORMAT);

        /* init chunk size. */
        char *chunk_size_str = HdfsGetOptionValue(foreignOid, OPTION_NAME_CHUNK_SIZE);

        if (chunk_size_str != NULL) {
            (void)parse_int(chunk_size_str, &chunk_size, 0, NULL);
            if (chunk_size >= (MAX_INT32 / 1024 / 1024) || chunk_size < 0) {
                ereport(ERROR,
                        (errcode(ERRCODE_INVALID_OBJECT_DEFINITION), errmsg("chunk_size :%d is illegal.", chunk_size)));
            }
            chunk_size = chunk_size * 1024 * 1024;
        }
    } else {
        DefElem *def_elem = NULL;
        /* encoding  */
        char *encoding_str = getFTOptionValue(fScanState->options->fOptions, OPTION_NAME_ENCODING);
        if (encoding_str != NULL) {
            file_encoding = pg_char_to_encoding(encoding_str);
        }

        def_elem = getFTOptionDefElemFromList(fScanState->options->fOptions, OPTION_NAME_FILL_MISSING_FIELDS);
        fill_missing_fields = def_elem ? defGetBoolean(def_elem) : false;
        def_elem = getFTOptionDefElemFromList(fScanState->options->fOptions, OPTION_NAME_IGNORE_EXTRA_DATA);
        ignore_extra_data = def_elem ? defGetBoolean(def_elem) : false;
        /* init_time_format */
        date_format.str = getFTOptionValue(fScanState->options->fOptions, OPTION_NAME_DATE_FORMAT);
        time_format.str = getFTOptionValue(fScanState->options->fOptions, OPTION_NAME_TIME_FORMAT);
        timestamp_format.str = getFTOptionValue(fScanState->options->fOptions, OPTION_NAME_TIMESTAMP_FORMAT);
        smalldatetime_format.str = getFTOptionValue(fScanState->options->fOptions, OPTION_NAME_SMALLDATETIME_FORMAT);

        /* init chunk size. */
        char *chunk_size_str = getFTOptionValue(fScanState->options->fOptions, OPTION_NAME_CHUNK_SIZE);

        if (chunk_size_str != NULL) {
            (void)parse_int(chunk_size_str, &chunk_size, 0, NULL);
            if (chunk_size >= (MAX_INT32 / 1024 / 1024) || chunk_size < 0) {
                ereport(ERROR,
                        (errcode(ERRCODE_INVALID_OBJECT_DEFINITION), errmsg("chunk_size :%d is illegal.", chunk_size)));
            }
            chunk_size = chunk_size * 1024 * 1024;
        }
    }

    initTimeFormat();
}

TextParserOption::TextParserOption(ForeignScanState *fScanState)
{
    /* fill the default value. */
    noescaping = false;
    null = "\\N";
    delimiter = "\t";
    ServerTypeOption srvType = T_INVALID;

    if (fScanState->options == NULL) {
        /* not computing pool, we get the information from system table.  */
        Relation rel = fScanState->ss.ss_currentRelation;
        Oid foreignTableId = RelationGetRelid(rel);
        srvType = getServerType(foreignTableId);
    } else {
        /* commputing pool, we get the information from foreignOptions struct that built in CN. */
        char *optionValue = getFTOptionValue(fScanState->options->fOptions, OPTION_NAME_SERVER_TYPE);
        if (unlikely(optionValue == nullptr)) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION), errmsg("No \"type\" option provided.")));
        }

        if (0 == pg_strcasecmp(optionValue, OBS_SERVER)) {
            srvType = T_OBS_SERVER;
        } else if (0 == pg_strcasecmp(optionValue, HDFS_SERVER)) {
            srvType = T_HDFS_SERVER;
        }
    }

    if (srvType == T_OBS_SERVER) {
        chunk_size = OBS_READ_BUFFER_LEN;
    } else {
        chunk_size = HDFS_READ_BUFFER_LEN;
    }

    this->fScanState = fScanState;
    initOptoinValue();
}
void TextParserOption::initOptoinValue()
{
    setCommonOptionValue();
    setOtherOptionValue();
}

void TextParserOption::setOtherOptionValue()
{
    if (fScanState->options == NULL) {
        /* not computing pool. */
        Oid foreignOid = RelationGetRelid(fScanState->ss.ss_currentRelation);

        char *null_str = HdfsGetOptionValue(foreignOid, OPTION_NAME_NULL);
        if (null_str != NULL) {
            null = null_str;
        }

        char *delimiter_str = HdfsGetOptionValue(foreignOid, OPTION_NAME_DELIMITER);
        if (delimiter_str != NULL) {
            delimiter = delimiter_str;
        }

        DefElem *def_elem = NULL;

        def_elem = HdfsGetOptionDefElem(foreignOid, OPTION_NAME_NOESCAPING);
        noescaping = def_elem ? defGetBoolean(def_elem) : false;
    } else {
        char *null_str = getFTOptionValue(fScanState->options->fOptions, OPTION_NAME_NULL);
        if (null_str != NULL) {
            null = null_str;
        }

        char *delimiter_str = getFTOptionValue(fScanState->options->fOptions, OPTION_NAME_DELIMITER);
        if (delimiter_str != NULL) {
            delimiter = delimiter_str;
        }

        DefElem *def_elem = NULL;

        def_elem = getFTOptionDefElemFromList(fScanState->options->fOptions, OPTION_NAME_NOESCAPING);
        noescaping = def_elem ? defGetBoolean(def_elem) : false;
    }
}

CsvParserOption::CsvParserOption(ForeignScanState *fScanState)
{
    /* fill the default value. */
    hasHeader = false;
    delimiter = ",";
    quote = '"';
    escape = '"';
    null = "";
    ServerTypeOption srvType = T_INVALID;

    if (fScanState->options == NULL) {
        /* not computing pool, we get the information from system table.  */
        Relation rel = fScanState->ss.ss_currentRelation;
        Oid foreignTableId = RelationGetRelid(rel);
        srvType = getServerType(foreignTableId);
    } else {
        /* commputing pool, we get the information from foreignOptions struct that built in CN. */
        char *optionValue = getFTOptionValue(fScanState->options->fOptions, OPTION_NAME_SERVER_TYPE);
        if (unlikely(optionValue == nullptr)) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION), errmsg("No \"type\" option provided.")));
        }

        if (0 == pg_strcasecmp(optionValue, OBS_SERVER)) {
            srvType = T_OBS_SERVER;
        } else if (0 == pg_strcasecmp(optionValue, HDFS_SERVER)) {
            srvType = T_HDFS_SERVER;
        }
    }

    if (srvType == T_OBS_SERVER) {
        chunk_size = OBS_READ_BUFFER_LEN;
    } else {
        chunk_size = HDFS_READ_BUFFER_LEN;
    }

    this->fScanState = fScanState;
    initOptoinValue();
}

void CsvParserOption::initOptoinValue()
{
    setCommonOptionValue();
    setOtherOptionValue();
}

void CsvParserOption::setOtherOptionValue()
{
    if (fScanState->options == NULL) {
        /* not computing pool. */
        Oid foreignOid = RelationGetRelid(fScanState->ss.ss_currentRelation);
        DefElem *def_elem = NULL;

        char *null_str = HdfsGetOptionValue(foreignOid, OPTION_NAME_NULL);

        if (null_str != NULL) {
            null = null_str;
        }

        char *delimiter_str = HdfsGetOptionValue(foreignOid, OPTION_NAME_DELIMITER);
        if (delimiter_str != NULL) {
            delimiter = delimiter_str;
        }

        def_elem = HdfsGetOptionDefElem(foreignOid, OPTION_NAME_HEADER);
        hasHeader = def_elem ? defGetBoolean(def_elem) : false;

        char *quote_str = HdfsGetOptionValue(foreignOid, OPTION_NAME_QUOTE);
        if (quote_str != NULL) {
            quote = *quote_str;
        }

        char *escape_str = HdfsGetOptionValue(foreignOid, OPTION_NAME_ESCAPE);
        if (escape_str != NULL) {
            escape = *escape_str;
        }

        if (escape == quote) {
            escape = '\0';
        }
    } else {
        DefElem *def_elem = NULL;

        char *null_str = getFTOptionValue(fScanState->options->fOptions, OPTION_NAME_NULL);

        if (null_str != NULL) {
            null = null_str;
        }

        char *delimiter_str = getFTOptionValue(fScanState->options->fOptions, OPTION_NAME_DELIMITER);
        if (delimiter_str != NULL) {
            delimiter = delimiter_str;
        }

        def_elem = getFTOptionDefElemFromList(fScanState->options->fOptions, OPTION_NAME_HEADER);
        hasHeader = def_elem ? defGetBoolean(def_elem) : false;

        char *quote_str = getFTOptionValue(fScanState->options->fOptions, OPTION_NAME_QUOTE);
        if (quote_str != NULL) {
            quote = *quote_str;
        }

        char *escape_str = getFTOptionValue(fScanState->options->fOptions, OPTION_NAME_ESCAPE);
        if (escape_str != NULL) {
            escape = *escape_str;
        }

        if (escape == quote) {
            escape = '\0';
        }
    }
}

class LineBufferImpl : public LineBuffer {
    // using StringInfo
    // without line header
public:
    LineBufferImpl();

    virtual ~LineBufferImpl() override;

    virtual void Destroy() override;

    virtual int appendLine(const char *buf, int buf_len, bool is_complete) override;

    virtual void reset() override
    {
        resetStringInfo(m_string_info);
        m_cur_line_completed = false;
    }

    virtual char *getLineBuffer() override
    {
        return m_string_info->data;
    }

    virtual int getLineLength() override
    {
        return m_string_info->len;
    }

    StringInfo m_string_info;

private:
    bool m_cur_line_completed;
};

LineBufferImpl::LineBufferImpl()
{
    m_string_info = makeStringInfo();
    m_cur_line_completed = false;
}

LineBufferImpl::~LineBufferImpl()
{
    m_string_info = NULL;
}

void LineBufferImpl::Destroy()
{
    resetStringInfo(m_string_info);
    pfree(m_string_info->data);
    m_string_info->data = NULL;
    pfree(m_string_info);
    m_string_info = NULL;
}

/*
 * @Description: add string buffer to line
 * @IN/OUT buf: string buffer
 * @IN/OUT buf_len: buffer len
 * @IN/OUT is_complete: is line  complete
 * @Return: appended buffer len
 * @See also:
 */
int LineBufferImpl::appendLine(const char *buf, int buf_len, bool is_complete)
{
    /* check > 1GB */
    if ((Size)((unsigned int)(m_string_info->len + buf_len)) >= MaxAllocSize)
        return -1;

    /* using appendBinaryStringInfo if buf has '\0' */
    appendBinaryStringInfo(m_string_info, buf, buf_len);
    m_cur_line_completed = is_complete;

    return buf_len;
}

uint64 BaseParser::fillReadBuffer(uint64 once_read_size)
{
    Assert(m_file_len >= m_file_read_len);

    /* read from file */
    uint64_t file_remain_len = m_file_len - m_file_read_len;

    if (file_remain_len == 0) {
        /* EOF */
        return 0;
    }

    /* read */
    uint64_t read_len = Min(once_read_size, file_remain_len);

    m_inputstream->read(m_buffer, read_len, m_file_read_len);

    m_file_read_len += read_len;

    m_buffer_pos = m_buffer;
    m_buffer_len = read_len;
    m_buffer[m_buffer_len] = '\0';

    return read_len;
}

BaseParser *createParser(DFSFileType Fileformat)
{
    BaseParser *baseParser = NULL;

    if (Fileformat == DFS_TEXT) {
        baseParser = New(CurrentMemoryContext) TextParserImpl();
    } else {
        baseParser = New(CurrentMemoryContext) CsvParserImpl();
    }

    return baseParser;
}

LineBuffer *createLineBuffer()
{
    return New(CurrentMemoryContext) LineBufferImpl();
}

BaseParserOption *createParserOption(DFSFileType file_format, reader::ReaderState *reader_state)
{
    ForeignScanState *fScanstate = (ForeignScanState *)(reader_state->scanstate);
    if (file_format == DFS_TEXT) {
        return New(CurrentMemoryContext) TextParserOption(fScanstate);
    } else {
        return New(CurrentMemoryContext) CsvParserOption(fScanstate);
    }
}
}  // namespace dfs
