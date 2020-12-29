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
 * sctp_cont.h
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/communication/sctp_utils/sctp_cont.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef _MC_UTILS_CONT_H_
#define _MC_UTILS_CONT_H_

#include <stddef.h>

//  Takes a pointer to a member variable and computes pointer to the structure
//		that contains it. 'type' is type of the structure, not the member.
//
#define mc_cont(ptr, type, member) (((ptr) != NULL) ? ((type*)(((char*)ptr) - offsetof(type, member))) : NULL)

#endif  //_MC_UTILS_CONT_H_
