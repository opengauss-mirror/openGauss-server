/*
 * Copyright (c) 2021 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 * http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * func_hardcoded_values.h
 *
 * IDENTIFICATION
 * src\common\interfaces\libpq\client_logic_processor\func_hardcoded_values.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef FUNC_HARDCODED_VALUES_H
#define FUNC_HARDCODED_VALUES_H

#include "nodes/pg_list.h"
#include "client_logic_common/statement_data.h"

/*
 * replacing hardcoded values such as "WHERE col1 = 5" -> "WHERE col1 = <processed>"
 * in within FUNCTION blocks (procedures, functions, anonymous blocks)
 */
class FuncHardcodedValues {
public:
    static bool process(const List *options, StatementData *statement_data, bool is_do_stmt = false);

private:
    typedef struct Body {
        char *partial_body;
        size_t body_location;
        bool to_parse;
    } Body;

    typedef enum DelimiterType {
        DELIMITER_TYPE_NONE,
        DELIMITER_TYPE_HYPHEN,
        DELIMITER_TYPE_DOLLAR,
        DELIMITER_TYPE_AS
    } DelimiterType;

    typedef struct FuncParseInfo {
        bool found_begin;   /* found_begin - found "begin" keyword */
        bool found_if;      /* found_if - found "if " keyword */
        bool found_end;     /* found_end = found "end " keyword */
        bool while_loop;    /* while_loop - found "while " keyword */
        bool for_loop;      /* for_loop - found "for " keyword */
        bool skip_to_endline;
        bool in_for_loop;   /* in_for_loop - whether the query was split in the middle of a FOR ... LOOP syntax */
        bool is_split;      /*
                             * is_split - whether the query parsed until now should be considered as a separate query
                             * and therefore should be split aside
                             */
        bool is_parse; /* is_parse - whether the query should be passed to an SQL parser or it should just be ignored */
        bool in_string;
    } FuncParseInfo;

    static Body **make_sql_body(const char *body, size_t body_size, size_t body_location, size_t *bodies_size);
    static Body **split_body(char *body, const size_t body_size, size_t body_location, size_t *bodies_size);
    static bool parse_body(const char *language, Body *body, size_t delimiter_location, size_t delimiter_size,
        size_t *body_location_offset, StatementData *statement_data);
    static bool adjust_original_stmt(const char *language, StatementData *statement_data, const char *adjusted_body,
        size_t body_location, size_t delimiter_location, size_t delimiter_size, size_t partial_body_size);

    static bool handle_body_quote(StatementData *statement_data, const char *adjusted_body, size_t delimiter_location,
        bool is_dollar, size_t *new_size, char **new_str);
    static int replace_original_query(StatementData *statement_data, size_t new_size, const char *new_str, int location,
        size_t original_size_to_replace);
    static bool check_should_split(const char **str, FuncParseInfo *func_parse_info);
    static char *skip_return_query(char *str);
    static bool process_options(const List *options, bool is_do_stmt, char **body, size_t *delimiter_location,
        size_t *delimiter_size, size_t *body_location, size_t *body_size, const char **language,
        const StatementData *statement_data);

    static bool process_def_elem_options(DefElem *as_item, bool is_do_stmt, DefElem *language_item, char **body,
        size_t *delimiter_location, size_t *delimiter_size, size_t *body_location, size_t *body_size,
        const char **language, const StatementData *statement_data);
    static bool extract_def_elem_options(const List *options, bool is_do_stmt, DefElem **as_item,
        DefElem **language_item, size_t *delimiter_location, size_t *delimiter_size,
        const StatementData *statement_data);
    static const size_t count_semi_colons(const char *str);
    static const char *find_word(const char *sentence, const char *word);

    static const char *process_def_body_parser(DefElem *as_item);
    static const char *process_def_language(DefElem *language_item, bool is_do_stmt);
    static const char *process_def_skip_spaces(const char *query, size_t *location, size_t *size);
    static const char *process_def_skip_do_cmd(const char *query, size_t *location, size_t *size, bool is_do_stmt);
    static bool process_def_process_delimiter_hyphen(size_t *delimiter_size, const char *body_start,
        size_t *body_location, size_t *body_size);
    static bool process_def_process_delimiter_dollar(size_t *delimiter_size, const char *body_start,
        size_t *body_location, size_t *body_size);
    static bool process_def_process_delimiters(size_t *delimiter_size, const char *body_start, size_t *body_location,
        size_t *body_size, DelimiterType *delimiter_type);

    static const char *parse_character(const char *body, size_t body_size, const char *statement_begin,
        const char *statement_end, size_t body_location, Body **bodies, size_t body_idx,
        FuncParseInfo *func_parse_info);

    static const char *find_keyword_beginning(const char *str, FuncParseInfo *func_parse_info, bool *is_exit);

    static const char *find_keyword_ending(const char *str, FuncParseInfo *func_parse_info, bool *is_exit);

    static const int m_BODIES_EXTRA_SIZE = 64;
    static constexpr const char *EMPTY_DOLLAR_TAG = "$$";
};

#endif
