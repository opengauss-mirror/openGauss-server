/* -------------------------------------------------------------------------
 *
 * pg_dump.h
 *	  Common header file for the pg_dump utility
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/bin/pg_dump/pg_dump.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef GS_DUMPALL_H
#define GS_DUMPALL_H

void help(void);

#ifdef GSDUMP_LLT
void stopLLT();
void checkAllTestExecuted();
#endif

#endif /* PG_DUMPALL_H */
