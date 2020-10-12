/* -------------------------------------------------------------------------
 *
 * walwriter.h
 *	  Exports from postmaster/walwriter.c.
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 *
 * src/include/postmaster/walwriter.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef _WALWRITER_H
#define _WALWRITER_H

typedef void (*WALCallback)(void* arg);

extern void RegisterWALCallback(WALCallback callback, void* arg);
extern void CallWALCallback();

extern void WalWriterMain(void);

#endif /* _WALWRITER_H */
