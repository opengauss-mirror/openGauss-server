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
 * csv_parser.h
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/dfs/csv/csv_parser.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef CSV_PARSER_H
#define CSV_PARSER_H

#include "../common_parser.h"

#include "access/dfs/dfs_stream.h"

#include "catalog/pg_type.h"
#include "commands/copy.h"
#include "lib/stringinfo.h"
#include "utils/formatting.h"
#include "utils/memutils.h"
#include "mb/pg_wchar.h"

namespace dfs {
class CsvParserImpl : public BaseParser {
public:
    CsvParserImpl() : m_options(NULL), read_in_quote(false), prev_char_is_escape(false){};
    ~CsvParserImpl()
    {
        m_options = NULL;
    }
    void Destroy();

    void init(dfs::GSInputStream *inputstream, void *options);
    int readLine(LineBuffer *line_buf);
    int getFields(char *buf, int len, char **raw_fields, int max_fields);
    bool skipLine();
    uint64_t getReadOffset()
    {
        return (m_file_read_len - (m_buffer_len - (m_buffer_pos - m_buffer)));
    }

private:
    template <bool skip_data>
    int splitLine(LineBuffer *line_buf);

    template <bool muti_byte_delim>
    int splitFields(char *buf, int len, char **raw_fields, int max_fields);

private:
    dfs::CsvParserOption *m_options;

    bool read_in_quote;
    bool prev_char_is_escape;
};
}  // namespace dfs
#endif /* CSV_PARSER_H */
