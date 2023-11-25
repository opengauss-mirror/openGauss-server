/* -------------------------------------------------------------------------
/*
 * Copyright (c) 2023 China Unicom Co.,Ltd.
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
 * gs_uwal_adaptor.h
 *
 *
 * IDENTIFICATION
 *    src/include/storage/gs_uwal/gs_uwal_adaptor.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef __GS_UWAL_ADAPTOR_H__
#define __GS_UWAL_ADAPTOR_H__
#include "uwal.h"
#include <unistd.h>

int uwal_init_symbols();

int ock_uwal_init(IN const char *path, IN const UwalCfgElem *elems, IN int cnt, IN const char *ulogPath);

void ock_uwal_exit(void);

int ock_uwal_create(IN UwalCreateParam *param, OUT UwalVector *uwals);

int ock_uwal_delete(IN UwalDeleteParam *param);

int ock_uwal_append(IN UwalAppendParam *param, OUT uint64_t *offset, OUT void* result);

int ock_uwal_read(IN UwalReadParam *param, OUT UwalBufferList *bufferList);

int ock_uwal_truncate(IN const UwalId *uwalId, IN uint64_t offset);

int ock_uwal_query(IN UwalQueryParam *param, OUT UwalInfo *info);

int ock_uwal_query_by_user(IN UwalUserType user, IN UwalRouteType route, OUT UwalVector *uwals);

int ock_uwal_set_rewind_point(IN UwalId *uwalId, IN uint64_t offset);

int ock_uwal_register_cert_verify_func(int32_t (*certVerify)(void* certStoreCtx, const char *crlPath),
    int32_t (*getKeyPass)(char *keyPassBuff, uint32_t keyPassBuffLen, char *keyPassPath));

int32_t ock_uwal_notify_nodelist_change(NodeStateList *nodeList, FinishCbFun cb, void *ctx);

uint32_t ock_uwal_ipv4_inet_to_int(char ipv4[16UL]);

#endif