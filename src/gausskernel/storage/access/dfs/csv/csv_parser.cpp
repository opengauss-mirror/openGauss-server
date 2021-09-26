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
 * csv_parser.cpp
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/dfs/csv/csv_parser.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <string>
#include <sstream>

#include "postgres.h"
#include "knl/knl_variable.h"

#include "csv_parser.h"
#include "mb/pg_wchar.h"

namespace dfs {
void CsvParserImpl::Destroy()
{
}

/*
 * @Description: init text parser
 * @IN inputstream: input stream
 * @IN options: text parser options
 * @See also:
 */
void CsvParserImpl::init(dfs::GSInputStream *inputstream, void *options)
{
    Assert(inputstream && options);

    m_inputstream = inputstream;

    /* get the current file size. */
    m_file_len = m_inputstream->getLength();
    m_file_read_len = 0;

    /* options */
    m_options = (CsvParserOption *)options;

    if (m_buffer == NULL)
        m_buffer = (char *)palloc(m_options->chunk_size + 1);
    m_buffer[m_options->chunk_size] = '\0';

    /* reset read positon */
    m_buffer_pos = m_buffer;

    /* init read buffer */
    m_buffer_len = 0;

    /* attribute buf */
    if (!m_attribute_buf)
        m_attribute_buf = makeStringInfo();
    else
        resetStringInfo(m_attribute_buf);

    /* delimiter */
    size_t delimiter_len = strlen(m_options->delimiter);
    m_is_muti_byte_delim = (delimiter_len > 1);

    /* for csv format, we deal with the quote. */
    read_in_quote = false;

    prev_char_is_escape = false;
}

/*
 * @Description: read a line
 * @IN line_buf: line buffer
 * @Return: read result
 * @See also:
 */
int CsvParserImpl::readLine(dfs::LineBuffer *line_buf)
{
    int ret = this->splitLine<false>(line_buf);
    if (ret == RESULT_EOF && line_buf->getLineLength() != 0) {
        /* reach eof but not found eol */
        return RESULT_SUCCESS;
    }
    return ret;
}

/*
 * @Description: skip a line
 * @Return: true skiped, false when met eof
 * @See also:
 */
bool CsvParserImpl::skipLine()
{
    uint64_t read_pos_before = getReadOffset();
    (void)this->splitLine<true>(NULL);
    uint64_t read_pos_after = getReadOffset();
    return (read_pos_before != read_pos_after);
}

/*
 * @Description: split line to fields
 * @IN/OUT buf: buffer of line
 * @IN/OUT len: buffer length
 * @IN/OUT raw_fields: each fileds pointer
 * @IN/OUT max_fields: max fields
 * @Return: read field number
 * @See also:
 */
int CsvParserImpl::getFields(char *buf, int len, char **raw_fields, int max_fields)
{
    int fields = 0;

    if (!m_is_muti_byte_delim) {
        fields = this->splitFields<false>(buf, len, raw_fields, max_fields);
    } else {
        fields = this->splitFields<true>(buf, len, raw_fields, max_fields);
    }

    return fields;
}

/*
 * @Description: get a line
 * @IN line_buf: line buffer
 * @Return: read result
 * @See also:
 */
template <bool skip_data>
int CsvParserImpl::splitLine(LineBuffer *line_buf)
{
    bool need_data = false;
    char cur_char;
    char *temp_buffer_pos = NULL;

    /* the distance of the m_buffer_pos and  temp_buffer_pos will be one line data length.
     *
     * each comes in this function, we must reset the temp_buffer_pos to starting position of new line data.
     */
    temp_buffer_pos = m_buffer_pos;

    /*
     * we read data into readbuffer, only exists two case:
     * 1. the readbuffer has no any data, it means m_buffer_len =0;
     * 2. we do not find end of line in current buffer, we need copy buffer data into linebuffer
     * and fill read buffer.
     */
    if (m_buffer_len == 0) {
        need_data = true;
    }

    while (true) {
        if (need_data) {
            int read_len = fillReadBuffer(m_options->chunk_size);
            if (read_len == 0) {
                return RESULT_EOF;
            } else {
                need_data = false;
                /* read one row data from m_buffer_pos. */
                temp_buffer_pos = m_buffer_pos;
            }
        }

        /* get the current character from m_buffer. */
        cur_char = *temp_buffer_pos;

        /* the quote is not equal to escape forever. */
        if (unlikely(read_in_quote && cur_char == m_options->escape)) {
            prev_char_is_escape = !prev_char_is_escape;
        } else if (unlikely(cur_char == m_options->quote && !prev_char_is_escape)) {
            read_in_quote = !read_in_quote;
        }

        if (unlikely(cur_char != m_options->escape)) {
            prev_char_is_escape = false;
        }

        if (cur_char == '\r' && !read_in_quote) {
            /*
             * if this temp_buffer_pos points to end of buffer,  the next char may be one '\n',
             * we will copy current buffer data into linebuffer and fill readbuffer again.
             */
            if (temp_buffer_pos == m_buffer + m_buffer_len - 1) {
                m_buffer_len = 0;
                need_data = true;
                if (!skip_data) {
                    int ret = line_buf->appendLine(m_buffer_pos, (temp_buffer_pos - m_buffer_pos + 1), false);
                    if (ret < 0) {
                        /* need flash buffer */
                        return RESULT_BUFFER_FULL;
                    }
                }

                continue;
            } else if (*(temp_buffer_pos + 1) == '\n') {
                temp_buffer_pos++;
            }

            /* find line end, break it. */
            break;
        } else if (*temp_buffer_pos == '\n' && !read_in_quote) {
            /* fine line end, break it. */
            break;
        } else if (temp_buffer_pos == m_buffer + m_buffer_len - 1) {
            /*
             * if we do not find end of line until reach end buffer.
             * we copy data to linebuffer.
             */
            need_data = true;
            /*
             * if this is the last data in current file, we must set them.
             */
            m_buffer_len = 0;
            if (!skip_data) {
                int ret = line_buf->appendLine(m_buffer_pos, (temp_buffer_pos - m_buffer_pos + 1), false);
                if (ret < 0) {
                    /* need flash buffer */
                    return RESULT_BUFFER_FULL;
                }
            }
        } else {
            temp_buffer_pos++;
        }
    }

    if (!skip_data) {
        /*
         * line buffer includes the eol character.
         */
        int ret = line_buf->appendLine(m_buffer_pos, (temp_buffer_pos - m_buffer_pos + 1), true);
        if (ret < 0) {
            /* need flash buffer */
            return RESULT_BUFFER_FULL;
        }
    }

    /*
     * we find one row data.
     * the m_buffer_pos points to starting position of the next line.
     */
    if (temp_buffer_pos == m_buffer + m_buffer_len - 1) {
        /* if temp_buffer_pos == m_buffer + m_buffer_len - 1, we do not  operator "m_buffer_pos = temp_buffer_pos + 1"
         * in order to void to appear illegal pointer.
         */
        m_buffer_len = 0;
        m_buffer_pos = m_buffer;
    } else {
        m_buffer_pos = temp_buffer_pos + 1;
    }

    return RESULT_SUCCESS;
}

/*
 * @Description: split line to fields
 * @IN/OUT buf: buffer of line
 * @IN/OUT len: buffer length
 * @IN/OUT raw_fields: each fileds pointer
 * @IN/OUT max_fields: max fields
 * @Return: read field number
 * @See also:
 */
template <bool muti_byte_delim>
int CsvParserImpl::splitFields(char *buf, int len, char **raw_fields, int max_fields)
{
    /* clean up this buffer in order to store the all columns of one row data. */
    resetStringInfo(m_attribute_buf);

    char delimc = m_options->delimiter[0];
    char *delimiter = m_options->delimiter;
    int fieldno = 0;
    char *output_ptr = NULL;
    char *cur_ptr = NULL;
    char *line_end_ptr = NULL;
    char *line_begin_ptr = NULL;

    if (m_attribute_buf->maxlen <= len)
        enlargeStringInfo(m_attribute_buf, len);
    /* store each column data into output_ptr buffer. */
    output_ptr = m_attribute_buf->data;

    cur_ptr = buf;

    line_begin_ptr = buf;
    line_end_ptr = buf + len;

    /* As for outer loop, iterates the each column. */
    fieldno = 0;
    for (;;) {
        bool found_delim = false;
        char *start_ptr = NULL;
        char *end_ptr = NULL;
        size_t input_len;
        bool saw_quote = false;

        if (fieldno >= max_fields) {
            // extra column, check if ignore_extra_data
            if (!m_options->ignore_extra_data) {
                ereport(ERROR,
                        (errcode(ERRCODE_BAD_COPY_FILE_FORMAT), errmsg("extra data after last expected column")));
            }

            break;
        }

        /* Remember start of field on both input and output sides */
        start_ptr = cur_ptr;
        /* remember starting position of the each column in the m_attribute_buf buffer. */
        raw_fields[fieldno] = output_ptr;

        /*
         * Scan data for field,
         *
         * The loop starts in "not quote" mode and then toggles between that
         * and "in quote" mode. The loop exits normally if it is in "not
         * quote" mode and a delimiter or line end is seen.
         */
        for (;;) {
            char c;

            /* Not in quote */
            for (;;) {
                end_ptr = cur_ptr;
                if (cur_ptr >= line_end_ptr)
                    goto endfield;
                c = *cur_ptr++;
                /* unquoted field delimiter */
                if (c == delimc && (!muti_byte_delim || strncmp(cur_ptr - 1, delimiter, strlen(delimiter)) == 0)) {
                    if (muti_byte_delim)
                        cur_ptr += (strlen(delimiter) - 1);

                    found_delim = true;
                    goto endfield;
                }
                /* start of quoted field (or part of field) */
                if (c == m_options->quote) {
                    saw_quote = true;
                    break;
                }
                /* Add c to output string */
                *output_ptr++ = c;
            }

            /* In quote */
            for (;;) {
                end_ptr = cur_ptr;
                if (cur_ptr >= line_end_ptr)
                    ereport(ERROR, (errcode(ERRCODE_BAD_COPY_FILE_FORMAT), errmsg("unterminated CSV quoted field")));

                c = *cur_ptr++;

                /* escape within a quoted field */
                if (c == m_options->escape) {
                    /*
                     * peek at the next char if available, and escape it if it
                     * is an escape char or a quote char
                     */
                    if (cur_ptr < line_end_ptr) {
                        char nextc = *cur_ptr;

                        if (nextc == m_options->escape || nextc == m_options->quote) {
                            *output_ptr++ = nextc;
                            cur_ptr++;
                            continue;
                        }
                    }
                }

                /*
                 * end of quoted field. Must do this test after testing for
                 * escape in case quote char and escape char are the same
                 * (which is the common case).
                 */
                if (c == m_options->quote)
                    break;

                /* Add c to output string */
                *output_ptr++ = c;
            }
        }

    endfield:

        /* Terminate attribute value in output area */
        *output_ptr++ = '\0';

        /* Check whether raw input matched null marker */
        input_len = end_ptr - start_ptr;
        if (!saw_quote && input_len == strlen(m_options->null) && strncmp(start_ptr, m_options->null, input_len) == 0)
            raw_fields[fieldno] = NULL;

        fieldno++;
        /* Done if we hit EOL instead of a delim */
        if (!found_delim)
            break;
    }

    /* Clean up state of attribute_buf */
    output_ptr--;
    Assert(*output_ptr == '\0');
    m_attribute_buf->len = (output_ptr - m_attribute_buf->data);

    return fieldno;
}
}  // namespace dfs
