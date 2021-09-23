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
 * convjson_common.h
 *      functions to create/read/write/remove files, used for convert json file into code file.
 * 
 * IDENTIFICATION
 *	  src/common/interfaces/libpq/client_logic_hooks/cmk_entity_manager_hooks/convert_jsonfile/convjson_common.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef CONVJSON_COMMON_H
#define CONVJSON_COMMON_H

#include "../cmkem_comm.h"

CmkemErrCode check_file_path(const char *file_path);
CmkemErrCode check_and_realpath(const char *file_path, char *real_file_path, size_t real_file_path_len);
CmkemErrCode create_file(const char *file_path);
CmkemErrCode read_file_size(const char *file_path, size_t *file_size);
CmkemErrCode read_file_content(const char *file_path, size_t read_len, char *buf, size_t buf_len);
CmkemStr *read_file(const char *file_path);
CmkemErrCode write_content(const char *file_path, const char* buf, size_t buf_len);
CmkemErrCode write_content_with_create(const char *file_path, const char* buf, size_t buf_len);
CmkemErrCode write_content_to_tail(const char *file_path, const char* buf, size_t buf_len);
CmkemErrCode remove_file(const char *file_path);

#endif /* CONVJSON_COMMON_H */
