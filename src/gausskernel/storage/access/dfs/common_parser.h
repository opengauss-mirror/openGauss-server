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
 * common_parser.h
 *    support the common class for parser.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/dfs/common_parser.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef COMMON_PARSER_H
#define COMMON_PARSER_H
#include "access/dfs/dfs_stream.h"
#include "access/dfs/dfs_am.h"

#include "postgres.h"
#include "knl/knl_variable.h"

#include "catalog/pg_type.h"
#include "commands/copy.h"
#include "foreign/foreign.h"
#include "lib/stringinfo.h"
#include "utils/formatting.h"
#include "utils/memutils.h"
#include "mb/pg_wchar.h"

namespace dfs {
typedef enum {
    RESULT_SUCCESS,
    RESULT_NEW_ONE,
    RESULT_EOF,
    RESULT_BUFFER_FULL
} ParserResult;

class BaseParserOption : public BaseObject {
public:
    BaseParserOption();
    virtual ~BaseParserOption()
    {
        null = NULL;
        fScanState = NULL;
        delimiter = NULL;
    }

    virtual void initOptoinValue() = 0;

    char *getDataTypeFmt(Oid typioparam);

    virtual bool getHeaderOption() = 0;

public:
    /* the delimiter, null option will be setted in child class. */
    char *delimiter;
    char *null;

    bool fill_missing_fields;
    bool ignore_extra_data;

    int chunk_size;

protected:
    void setCommonOptionValue();
    virtual void setOtherOptionValue() = 0;

    void initTimeFormat();

    ForeignScanState *fScanState;

private:
    /* the following option will be setted in current class. */
    int file_encoding;

    /* date format */
    user_time_format date_format;
    user_time_format time_format;
    user_time_format timestamp_format;
    user_time_format smalldatetime_format;
};

class TextParserOption : public BaseParserOption {
public:
    explicit TextParserOption(ForeignScanState *fScanState);
    virtual ~TextParserOption() override
    {
    }
    void initOptoinValue() override;

    bool getHeaderOption() override
    {
        return false;
    }

public:
    bool noescaping;

protected:
    void setOtherOptionValue() override;
};

class CsvParserOption : public BaseParserOption {
public:
    explicit CsvParserOption(ForeignScanState *fScanState);
    virtual ~CsvParserOption() override
    {
    }

    void initOptoinValue() override;

    bool getHeaderOption() override
    {
        return hasHeader;
    }

public:
    /* only signle character. */
    char quote;
    char escape;

private:
    bool hasHeader;

protected:
    void setOtherOptionValue() override;
};

/*
 * This class is used to store the each row data when the read row data from readbuffer.
 */
class LineBuffer : public BaseObject {
public:
    virtual ~LineBuffer()
    {
    }
    virtual void Destroy()
    {
    }

    virtual int appendLine(const char *buf, int buf_len, bool is_complete) = 0;
    virtual void reset() = 0;
    virtual char *getLineBuffer() = 0;
    virtual int getLineLength() = 0;
};

/*
 * This class is used to parse buffer data. we realize the some common function and
 * define the common variable.
 *
 */
class BaseParser : public BaseObject {
public:
    BaseParser()
        : m_inputstream(NULL),
          m_file_len(0),
          m_file_read_len(0),
          m_buffer(NULL),
          m_buffer_pos(NULL),
          m_buffer_len(0),
          m_attribute_buf(NULL),
          m_is_muti_byte_delim(false)
    {
    }
    virtual ~BaseParser()
    {
        m_buffer_pos = NULL;
        m_inputstream = NULL;
        m_attribute_buf = NULL;
        m_buffer = NULL;
    }
    virtual void Destroy()
    {
    }

    virtual void init(dfs::GSInputStream *input_stream, void *options) = 0;

    virtual int getFields(char *buf, int len, char **raw_fields, int max_fields) = 0;
    virtual bool skipLine() = 0;
    virtual uint64_t getReadOffset() = 0;
    /*
     * read one line data from readbuffer.
     */
    virtual int readLine(LineBuffer *line_buf) = 0;

    /*
     * read data from obs server and fill the data into read buffer(m_buffer).
     */
    uint64 fillReadBuffer(uint64 once_read_size);

protected:
    /* the interface */
    dfs::GSInputStream *m_inputstream;
    /* the file size. */
    uint64_t m_file_len;
    /* Currently, we has been read file size.  */
    uint64_t m_file_read_len;
    /* the readbuffer starting position, it keeps the starting position. */
    char *m_buffer;
    /* we have been read size in current readbuffer. */
    char *m_buffer_pos;
    /* current readbuffer size. */
    size_t m_buffer_len;

    /* store the all column data value for each line buffer string(row data). */
    StringInfo m_attribute_buf;
    bool m_is_muti_byte_delim;
};

BaseParser *createParser(DFSFileType fileFormat);

LineBuffer *createLineBuffer();

BaseParserOption *createParserOption(DFSFileType file_format, reader::ReaderState *reader_state);

}  // namespace dfs

#endif /* COMMON_PARSER_H */
