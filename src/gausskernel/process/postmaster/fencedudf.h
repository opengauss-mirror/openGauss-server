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
 * fencedudf.h
 *
 * IDENTIFICATION
 *	  src/gausskernel/process/postmaster/fencedudf.h
 *
 * ------------------------------------------------------------------------------
 */
#ifndef __FENCED_UDF__
#define __FENCED_UDF__

/*
 * Hashtable for fast lookup of external C functions
 */
typedef struct {
    /* fn_oid is the hash key and so must be first! */
    Oid fn_oid;         /* OID of an external function */
    Oid fn_languageId;  /* language OID */
    int argNum;         /* The number of argument value */
    PGFunction user_fn; /* the function's address */
    UDFInfoType udfInfo;
} UDFFuncHashTabEntry;

#define AppendBufFixedMsgVal(bufPtr, addr, size)    \
    do {                                            \
        appendBinaryStringInfo(bufPtr, addr, size); \
    } while (0)

#define AppendBufVarMsgVal(bufPtr, addr, size)             \
    do {                                                   \
        appendBinaryStringInfo(bufPtr, (char*)&(size), 4); \
        appendBinaryStringInfo(bufPtr, addr, size);        \
    } while (0)

#define GetFixedMsgVal(bufPtr, addr, size, addr_size)         \
    do {                                                      \
        errno_t rc;                                           \
        rc = memcpy_s((addr), (addr_size), (bufPtr), (size)); \
        securec_check_c(rc, "\0", "\0");                      \
        (bufPtr) += (size);                                   \
    } while (0)

#define GetFixedMsgValSafe(bufPtr, addr, size, addr_size, srcRemainLen)           \
    do {                                                                        \
        if (unlikely(srcRemainLen < size)) {                                     \
            ereport(ERROR, (errmodule(MOD_UDF), errcode(ERRCODE_PROTOCOL_VIOLATION), \
                errmsg("No data left in msg, left len:%u, desire len:%lu", srcRemainLen, (long unsigned int)size))); \
        }                                                                          \
        errno_t rc = memcpy_s((addr), (addr_size), (bufPtr), (size)); \
        securec_check_c(rc, "\0", "\0");                      \
        (bufPtr) += (size);                                   \
        (srcRemainLen) -= (size);                             \
    } while (0)                                               \

#define GetVarMsgValSafe(bufPtr, addr, size, addr_size, srcRemainLen)           \
        do {                                                      \
            if (unlikely(srcRemainLen < 4)) {                                     \
                ereport(ERROR, (errmodule(MOD_UDF), errcode(ERRCODE_PROTOCOL_VIOLATION), \
                    errmsg("No data left in msg, left len:%u", srcRemainLen))); \
            }                                                                           \
            (size) = *(int*)(bufPtr);                             \
            (bufPtr) += 4;                                        \
            (srcRemainLen) -= 4;                                  \
            if (unlikely(srcRemainLen < size)) {                                     \
                ereport(ERROR, (errmodule(MOD_UDF), errcode(ERRCODE_PROTOCOL_VIOLATION), \
                    errmsg("No data left in msg, left len:%u, desire len:%u", srcRemainLen, size))); \
            }                                                                          \
            errno_t rc = memcpy_s((addr), (addr_size), (bufPtr), (size)); \
            securec_check_c(rc, "\0", "\0");                      \
            (bufPtr) += (size);                                   \
            (srcRemainLen) -= (size);                             \
        } while (0)                                               \

#define Reserve4BytesMsgHeader(bufPtr)                             \
    do {                                                           \
        int _reserveSize = 0;                                      \
        appendBinaryStringInfo((bufPtr), (char*)&_reserveSize, 4); \
    } while (0)

#define Fill4BytesMsgHeader(bufPtr)                \
    do {                                           \
        Assert((bufPtr)->len >= 4);                \
        *(int*)(bufPtr)->data = (bufPtr)->len - 4; \
    } while (0)

#endif
