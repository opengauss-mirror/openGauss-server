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
 * text_parser.cpp
 *
 *
 * IDENTIFICATION
 *         src/gausskernel/storage/access/dfs/text/text_parser.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <string>
#include <sstream>

#include "postgres.h"
#include "knl/knl_variable.h"

#include "text_parser.h"
#include "mb/pg_wchar.h"

#define TEXT_SEARCH_CHUNK_SZ 128

#define ISOCTAL(c) (((c) >= '0') && ((c) <= '7'))
#define OCTVALUE(c) ((c) - '0')

namespace dfs {
/*
 * @Description: using memchr find eol from s  to len ,  allow '\0' in string buffer
 * @IN s: string buffer pointer
 * @IN len: string len
 * @Return: eol postion or NULL from not found
 * @See also:
 */
static inline char *FindEolChar(char *s, size_t len)
{
    char *end = NULL;
    char *cr = NULL;
    char *lf = NULL;
    end = s + len;
    while (s < end) {
        size_t chunk = (s + TEXT_SEARCH_CHUNK_SZ < end) ? TEXT_SEARCH_CHUNK_SZ : (end - s);
        cr = (char *)memchr(s, '\r', chunk);
        lf = (char *)memchr(s, '\n', chunk);
        if (cr != NULL) {
            if (lf != NULL && lf < cr) {
                return lf;
            }
            return cr;
        } else if (lf != NULL) {
            return lf;
        }
        s += TEXT_SEARCH_CHUNK_SZ;
    }
    return NULL;
}

TextParserImpl::~TextParserImpl()
{
    m_options = NULL;
}

void TextParserImpl::Destroy()
{
    if (m_buffer != NULL) {
        pfree(m_buffer);
        m_buffer = NULL;
    }

    if (m_attribute_buf) {
        resetStringInfo(m_attribute_buf);
        pfree(m_attribute_buf->data);
        m_attribute_buf->data = NULL;
        pfree(m_attribute_buf);
        m_attribute_buf = NULL;
    }
}

/*
 * @Description: init text parser
 * @IN inputstream: input stream
 * @IN options: text parser options
 * @See also:
 */
void TextParserImpl::init(dfs::GSInputStream *inputstream, void *options)
{
    Assert(inputstream && options);

    m_inputstream = inputstream;

    m_file_len = m_inputstream->getLength();
    m_file_read_len = 0;

    /* options */
    m_options = (TextParserOption *)options;

    /* init read buffer */
    m_buffer_len = m_options->chunk_size;
    if (m_buffer == NULL) {
        m_buffer = (char *)palloc(m_buffer_len + 1);
    }
    m_buffer[m_buffer_len] = '\0';

    /* reset read positon */
    m_buffer_pos = m_buffer;
    m_buffer_len = 0;

    /* attribute buf */
    if (m_attribute_buf == NULL) {
        m_attribute_buf = makeStringInfo();
    } else {
        resetStringInfo(m_attribute_buf);
    }

    /* delimiter */
    size_t delimiter_len = strlen(m_options->delimiter);
    m_is_muti_byte_delim = (delimiter_len > 1);
}

/*
 * @Description: read a line
 * @IN line_buf: line buffer
 * @Return: read result
 * @See also:
 */
int TextParserImpl::readLine(dfs::LineBuffer *line_buf)
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
bool TextParserImpl::skipLine()
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
int TextParserImpl::getFields(char *buf, int len, char **raw_fields, int max_fields)
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
int TextParserImpl::splitLine(LineBuffer *line_buf)
{
    bool need_data = false;
    char *eol = NULL;
    int remainLen = 0;

    while (true) {
        if (need_data) {
            int read_len = BaseParser::fillReadBuffer(m_options->chunk_size);
            if (read_len == 0) {
                return RESULT_EOF;
            } else {
                need_data = false;
            }
        }

        /*
         * we have already read to m_buffer_pos position in buffer.
         * m_buffer_pos is the first character in new line.
         * find new line position start from m_buffer_pos to m_buffer_pos
         * + remainLen.
         */
        remainLen = m_buffer_len - (m_buffer_pos - m_buffer);

        /* find eof */
        eol = FindEolChar(m_buffer_pos, remainLen);
        if (eol == NULL) {
            if (!skip_data) {
                /* buffer read over, need refill */
                int ret = line_buf->appendLine(m_buffer_pos, remainLen, false);
                if (ret < 0) {
                    /* need flash buffer */
                    return RESULT_BUFFER_FULL;
                }
            }
            need_data = true;
            m_buffer_pos += remainLen;
            continue;
        } else if (*eol == '\r') {
            if (eol == m_buffer + m_buffer_len - 1) {
                need_data = true;
                if (!skip_data) {
                    int ret = line_buf->appendLine(m_buffer_pos, (eol - m_buffer_pos), false);
                    if (ret < 0) {
                        /* need flash buffer */
                        return RESULT_BUFFER_FULL;
                    }
                }
                m_buffer_pos = eol;
                continue;
            } else if (*(eol + 1) == '\n') {
                eol++;
            }
            break;
        } else {
            break;
        }
    }

    if (!skip_data) {
        /*
         * line buffer includes the eol character.
         */
        int ret = line_buf->appendLine(m_buffer_pos, (eol - m_buffer_pos + 1), true);
        if (ret < 0) {
            /* need flash buffer */
            return RESULT_BUFFER_FULL;
        }
    }

    /*
     * the m_buffer_pos points to starting position of the next line.
     */
    m_buffer_pos = eol + 1;
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
int TextParserImpl::splitFields(char *buf, int len, char **raw_fields, int max_fields)
{
    /* clean up this buffer in order to store the all columns of one row data. */
    resetStringInfo(m_attribute_buf);

    size_t delimiter_len = strlen(m_options->delimiter);
    size_t null_print_len = strlen(m_options->null);

    char delimc = m_options->delimiter[0];
    const char *delimiter = m_options->delimiter;
    int fieldno = 0;
    char *output_ptr = NULL;
    char *cur_ptr = NULL;
    char *line_end_ptr = NULL;
    char *line_begin_ptr = NULL;

    if (m_attribute_buf->maxlen <= len) {
        enlargeStringInfo(m_attribute_buf, len);
    }
    output_ptr = m_attribute_buf->data;

    cur_ptr = buf;

    line_begin_ptr = buf;
    line_end_ptr = buf + len;

    /* is gbk */
    bool is_server_gbk = (PG_GBK == GetDatabaseEncoding());

    fieldno = 0;
    for (;;) {
        bool found_delim = false;
        char *start_ptr = NULL;
        char *end_ptr = NULL;
        size_t input_len;
        bool saw_non_ascii = false;

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
        raw_fields[fieldno] = output_ptr;

        /*
         * Scan data for field.
         *
         * Note that in this loop, we are scanning to locate the end of field
         * and also speculatively performing de-escaping.  Once we find the
         * end-of-field, we can match the raw field contents against the null
         * marker string.  Only after that comparison fails do we know that
         * de-escaping is actually the right thing to do; therefore we *must
         * not* throw any syntax errors before we've done the null-marker
         * check.
         */
        for (;;) {
            char c;
            end_ptr = cur_ptr;
            if (cur_ptr >= line_end_ptr) {
                break;
            }
            c = *cur_ptr++;
            if (c == delimc && (!muti_byte_delim || strncmp(delimiter, cur_ptr - 1, delimiter_len) == 0)) {
                if (muti_byte_delim) {
                    cur_ptr += (delimiter_len - 1);
                }

                found_delim = true;
                break;
            }

            if (c == '\\' && !m_options->noescaping) {
                if (cur_ptr >= line_end_ptr) {
                    break;
                }
                c = *cur_ptr++;
                switch (c) {
                    case '0':
                    case '1':
                    case '2':
                    case '3':
                    case '4':
                    case '5':
                    case '6':
                    /* fall-through */
                    case '7': {
                        /* handle \013 */
                        unsigned int val = (unsigned int)OCTVALUE(c);
                        if (cur_ptr < line_end_ptr) {
                            c = *cur_ptr;
                            if (ISOCTAL(c)) {
                                cur_ptr++;
                                val = (val << 3) + OCTVALUE(c);
                                if (cur_ptr < line_end_ptr) {
                                    c = *cur_ptr;
                                    if (ISOCTAL(c)) {
                                        cur_ptr++;
                                        val = (val << 3) + OCTVALUE(c);
                                    }
                                }
                            }
                        }
                        c = (char)(val & 0377);
                        if (c == '\0' || IS_HIGHBIT_SET(c)) {
                            saw_non_ascii = true;
                        }
                    } break;
                    case 'x':
                        /* Handle \x3F */
                        if (cur_ptr < line_end_ptr) {
                            char hexchar = *cur_ptr;

                            if (isxdigit((unsigned char)hexchar)) {
                                unsigned int val = (unsigned int)GetDecimalFromHex(hexchar);

                                cur_ptr++;
                                if (cur_ptr < line_end_ptr) {
                                    hexchar = *cur_ptr;
                                    if (isxdigit((unsigned char)hexchar)) {
                                        cur_ptr++;
                                        val = (val << 4) + GetDecimalFromHex(hexchar);
                                    }
                                }
                                c = val & 0xff;
                                if (c == '\0' || IS_HIGHBIT_SET(c)) {
                                    saw_non_ascii = true;
                                }
                            }
                        }
                        break;
                    case 'b':
                        c = '\b';
                        break;
                    case 'f':
                        c = '\f';
                        break;
                    case 'n':
                        c = '\n';
                        break;
                    case 'r':
                        c = '\r';
                        break;
                    case 't':
                        c = '\t';
                        break;
                    case 'v':
                        c = '\v';
                        break;
                    default:
                        break;

                        /*
                         * in all other cases, take the char after '\'
                         * literally
                         */
                }
            }

            if (is_server_gbk && IS_HIGHBIT_SET(c)) {
                /*
                 * We don't do encoding validation check here because we already went through
                 * the test in pg_any_to_server
                 */
                char sec;
                if (cur_ptr >= line_end_ptr) {
                    /*
                     * Unlikely, yet we deal with it anyway.
                     */
                    *output_ptr++ = c;
                    break;
                }
                sec = *cur_ptr++;
                *output_ptr++ = c;
                *output_ptr++ = sec;
                /*
                 * We don't mark saw_non_ascii because output here is not from de-escaping.
                 */
            } else {
                /* Add c to output string */
                *output_ptr++ = c;
            }
        }

        /* Check whether raw input matched null marker */
        input_len = end_ptr - start_ptr;
        if (input_len == null_print_len && strncmp(start_ptr, m_options->null, input_len) == 0) {
            raw_fields[fieldno] = NULL;
        } else {
            /*
             * At this point we know the field is supposed to contain data.
             *
             * If we de-escaped any non-7-bit-ASCII chars, make sure the
             * resulting string is valid data for the db encoding.
             */
            if (saw_non_ascii) {
                char *fld = raw_fields[fieldno];

                pg_verifymbstr(fld, output_ptr - fld, false);
            }
        }

        /* Terminate attribute value in output area */
        *output_ptr++ = '\0';

        fieldno++;

        /* Done if we hit EOL instead of a delim */
        if (!found_delim) {
            break;
        }
    }

    /* Clean up state of attribute_buf */
    output_ptr--;
    Assert(*output_ptr == '\0');
    m_attribute_buf->len = (output_ptr - m_attribute_buf->data);

    return fieldno;
}
}  // namespace dfs
