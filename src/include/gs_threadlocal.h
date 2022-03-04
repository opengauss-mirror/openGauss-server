/* ---------------------------------------------------------------------------------------
 * 
 * gs_threadlocal.h
 * 
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * 
 * IDENTIFICATION
 *        src/include/gs_threadlocal.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef GS_THREADLOCAL_H_
#define GS_THREADLOCAL_H_

#ifdef PC_LINT
#define THR_LOCAL
#endif

#ifndef THR_LOCAL
#ifndef WIN32
#define THR_LOCAL __thread
#else
#define THR_LOCAL __declspec(thread)
#endif
#endif

#endif

