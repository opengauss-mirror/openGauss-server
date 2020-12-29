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
 * ---------------------------------------------------------------------------------------
 * 
 * dataqueuedefs.h
 *        Data replication queue pointer r definitions
 * 
 * 
 * IDENTIFICATION
 *        src/include/replication/dataqueuedefs.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef DATAQUEUE_DEFS_H
#define DATAQUEUE_DEFS_H

/* Pointer to a location in the queue. */
typedef struct DataQueuePtr {
    uint32 queueid;  /* data queue id, 0 based */
    uint32 queueoff; /* byte offset of location in current queue */
} DataQueuePtr;

/*
 * Macros for comparing DataQueuePtrs
 *
 * Beware of passing expressions with side-effects to these macros,
 * since the arguments may be evaluated multiple times.
 */
#define DQByteLT(a, b) ((a).queueid < (b).queueid || ((a).queueid == (b).queueid && (a).queueoff < (b).queueoff))
#define DQByteLE(a, b) ((a).queueid < (b).queueid || ((a).queueid == (b).queueid && (a).queueoff <= (b).queueoff))
#define DQByteEQ(a, b) ((a).queueid == (b).queueid && (a).queueoff == (b).queueoff)

#define DataQueuePtrIsInvalid(r) ((r).queueoff == 0)

/*
 * Macro for advancing a queue pointer by the specified number of bytes.
 */
#define DQByteAdvance(queueptr, nbytes)                                                                              \
    do {                                                                                                             \
        if (queueptr.queueoff > (uint32)g_instance.attr.attr_storage.DataQueueBufSize * 1024 - nbytes) {             \
            queueptr.queueid += 1;                                                                                   \
            queueptr.queueoff =                                                                                      \
                (uint32)(queueptr.queueoff + nbytes - (uint32)g_instance.attr.attr_storage.DataQueueBufSize * 1024); \
        } else                                                                                                       \
            queueptr.queueoff += nbytes;                                                                             \
    } while (0)

#endif /* DATAQUEU_DEFS_H */
