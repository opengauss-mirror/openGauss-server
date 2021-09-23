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
 * convjson.h
 *      we need to construct HTTP packages to connect to servers with restful style interfaces.
 *      however, it's hard to construct json strings directly in the code.
 *      so, we provide this bin tool to convert json file into code file: 'convjson'.
 * 
 * IDENTIFICATION
 *	  src/common/interfaces/libpq/client_logic_hooks/cmk_entity_manager_hooks/convert_jsonfile/convjson.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef C_H
#define C_H

#include "../cmkem_comm.h"
#include "cjson/cJSON.h"

CmkemStr *conv_str_to_code(const char *var, CmkemStr *var_val);
CmkemStr *conv_jsontree_to_advstr(cJSON *json_tree);
CmkemStr *conv_jsontree_to_code(cJSON *json_tree);
cJSON *conv_jsonfile_to_jsontree(const char *json_file);
CmkemErrCode conv_jsonfile_to_headfile_with_resolve(const char *json_file, const char *header_file,
    const char *child_json_list[]);

#endif
