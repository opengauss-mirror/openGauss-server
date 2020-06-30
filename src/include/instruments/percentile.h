/* ---------------------------------------------------------------------------------------
 * 
 * percentile.h
 *     Definitions for the PostgreSQL statistics collector daemon.
 *
 *	Copyright (c) 2001-2012, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *        src/include/instruments/percentile.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef PERCENTILE_H
#define PERCENTILE_H
#include "gs_thread.h"

extern void PercentileMain();
extern void JobPercentileIAm(void);
extern bool IsJobPercentileProcess(void);

#endif
