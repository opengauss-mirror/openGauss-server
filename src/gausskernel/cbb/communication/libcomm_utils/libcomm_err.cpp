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
 * sctp_err.cpp
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/communication/sctp_utils/sctp_err.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "libcomm_err.h"
#include "libcomm_errno-comlib.h"

void mc_err_abort(void)
{
    return;  // call abort () before, do nothing now
}

static const char unknown_error[] = "";
const char* mc_strerror(int errnum)
{
    if (errnum >= 1000) {
        return mc_comlib_strerror(errnum);  // communication layer defined errors
    } else if (errnum == 0)                                                                                   {
        return unknown_error;
    } else {
        return strerror(errnum);  // system defined errors
    }
}
