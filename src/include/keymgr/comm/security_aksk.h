/*
 * Copyright (c) 2022 Huawei Technologies Co.,Ltd.
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
 * security_aksk.h
 *
 * IDENTIFICATION
 *	  src/gausskernel/security/keymgr/include/comm/security_aksk.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef KM_AKSK_H_
#define KM_AKSK_H_

#include <stdlib.h>

#define KM_MAX_HTTP_HDR_NUM 16

typedef struct {
    const char *ak;
    const char *sk;

    /* http header */
    const char *method;
    const char *host;
    const char *uri;

    /* http headers */
    const char *reqhdrs[KM_MAX_HTTP_HDR_NUM];
    size_t hdrcnt;

    /* http body */
    const char *reqbody;
} AkSkPara;

typedef struct {
    char *host;
    char *date;
    char *signature;
} AkSkSign;

void km_aksk_add_reqhdr(AkSkPara *para, const char *hdr);
AkSkSign km_aksk_sign(AkSkPara *para);

#endif