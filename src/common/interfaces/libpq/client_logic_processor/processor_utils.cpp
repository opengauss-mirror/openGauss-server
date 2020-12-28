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
 * processor_utils.cpp
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_processor\processor_utils.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <iostream>
#include "processor_utils.h"
#include "nodes/value.h"


/*
 * NameListToString
 * Utility routine to convert a qualified-name list into a c string.
 *
 * This is used primarily to form error messages, and so we do not quote
 * the list elements, for the sake of legibility.
 *
 * In most scenarios the list elements should always be Value strings,
 * but we also allow A_Star for the convenience of ColumnRef processing.
 */
bool name_list_to_cstring(const List *names, char *buffer, size_t buffer_len)
{
    if (buffer == NULL || buffer_len == 0) {
        return false;
    }
    ListCell *l = NULL;
    buffer[0] = 0;

    foreach (l, names) {
        Node *name = (Node *)lfirst(l);

        if (l != list_head(names)) {
            check_strncat_s(strncat_s(buffer, buffer_len, ".", 1));
        }
        if (IsA(name, String)) {
            const char *name_str = strVal(name);
            check_strncat_s(strncat_s(buffer, buffer_len, name_str, strlen(name_str)));
        } else if (IsA(name, A_Star)) {
            check_strncat_s(strncat_s(buffer, buffer_len, "*", 1));
        } else {
            fprintf(stderr, "unexpected node type in name list: %d\n", (int)nodeTag(name));
            return false;
        }
    }

    return true;
}
