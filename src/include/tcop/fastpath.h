/* -------------------------------------------------------------------------
 *
 * fastpath.h
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/tcop/fastpath.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef FASTPATH_H
#define FASTPATH_H

#include "lib/stringinfo.h"

#define MAX_ARG_SIZE 107374182

extern int HandleFunctionRequest(StringInfo msgBuf);

#endif /* FASTPATH_H */
