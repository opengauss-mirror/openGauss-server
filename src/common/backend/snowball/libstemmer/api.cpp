/*
 * Copyright (c) 2001, Dr Martin Porter
 * Copyright (c) 2004,2005, Richard Boulton
 * Copyright (c) 2013, Yoshiki Shibukawa
 * Copyright (c) 2006,2007, Olly Betts
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 * 3. Neither the name of the Snowball project nor the names of its contributors
 *    may be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
 * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
 * ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * SPDX-License-Identifier: BSD-3-Clause
 */

/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * openGauss-server is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#include "header.h"

extern struct SN_env* SN_create_env(int S_size, int I_size, int B_size)
{
    struct SN_env* z = (struct SN_env*)calloc(1, sizeof(struct SN_env));
    if (z == NULL) {
        return NULL;
    }
    z->p = create_s();
    if (z->p == NULL) {
        goto error;
    }
    if (S_size) {
        int i;
        z->S = (symbol**)calloc(S_size, sizeof(symbol*));
        if (z->S == NULL) {
            goto error;
        }
        for (i = 0; i < S_size; i++) {
            z->S[i] = create_s();
            if (z->S[i] == NULL) {
                goto error;
            }
        }
    }

    if (I_size) {
        z->I = (int*)calloc(I_size, sizeof(int));
        if (z->I == NULL) {
            goto error;
        }
    }

    if (B_size) {
        z->B = (unsigned char*)calloc(B_size, sizeof(unsigned char));
        if (z->B == NULL) {
            goto error;
        }
    }

    return z;
error:
    SN_close_env(z, S_size);
    return NULL;
}

extern void SN_close_env(struct SN_env* z, int S_size)
{
    if (z == NULL) {
        return;
    }
    if (S_size) {
        int i;
        for (i = 0; i < S_size; i++) {
            lose_s(z->S[i]);
        }
        free(z->S);
    }
    free(z->I);
    free(z->B);
    if (z->p) {
        lose_s(z->p);
    }
    free(z);
}

extern int SN_set_current(struct SN_env* z, int size, const symbol* s)
{
    int err = replace_s(z, 0, z->l, size, s, NULL);
    z->c = 0;
    return err;
}
