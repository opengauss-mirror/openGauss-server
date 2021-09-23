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
 * gstrace_infra.h
 * 
 * 
 * IDENTIFICATION
 *        src/include/gstrace/gstrace_infra.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef TRACE_INFRA_H_
#define TRACE_INFRA_H_

/* use low 16 bits for function name */
#define GS_TRC_COMP_SHIFT 16
#define GS_TRC_FUNC_SHIFT 0
#define GS_TRC_ID_PACK(comp, func) (func | (comp) << GS_TRC_COMP_SHIFT)

/* used for gstrace_data() to inform tool how to decode its probe data. */
typedef enum trace_data_fmt {
    TRC_DATA_FMT_NONE,  /* no data in this trace record */
    TRC_DATA_FMT_DFLT,  /* data is stored with HEX format */
    TRC_DATA_FMT_UINT32 /* data is stored as uint32 */
} trace_data_fmt;

#ifdef ENABLE_GSTRACE

/* Initialize context during startup process. */
extern int gstrace_init(int key);

/* write one ENTRY trace record */
extern void gstrace_entry(const uint32_t rec_id);

/* write one EXIT trace record */
extern void gstrace_exit(const uint32_t rec_id);

/* write on DATA trace record. If data_len is greater than 1920, no data is traced. */
extern void gstrace_data(
    const uint32_t probe, const uint32_t rec_id, const trace_data_fmt fmt_type, const char* pData, size_t data_len);

extern int* gstrace_tryblock_entry(int* newTryCounter);
extern void gstrace_tryblock_exit(bool inCatch, int* oldTryCounter);

#else

#define gstrace_init(key) (1)
#define gstrace_entry(rec_id)
#define gstrace_exit(rec_id)
#define gstrace_data(probe, rec_id, fmt_type, pData, data_len)
#define gstrace_tryblock_entry(curTryCounter) (curTryCounter)
#define gstrace_tryblock_exit(inCatch, oldTryCounter)

#endif

#endif
