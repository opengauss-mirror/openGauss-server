/* -------------------------------------------------------------------------
 *
 * copydir.h
 *	  Copy a directory.
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/copydir.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef COPYDIR_H
#define COPYDIR_H

extern bool copydir(char* fromdir, char* todir, bool recurse, int elevel);
extern void copy_file(char* fromfile, char* tofile);
extern void copy_file_internal(char* fromfile, char* tofile, bool trunc_file);

extern void fsync_fname(const char* fname, bool isdir);
extern int durable_rename(const char* oldfile, const char* newfile, int elevel);
extern int durable_link_or_rename(const char* oldfile, const char* newfile, int elevel);

#endif /* COPYDIR_H */
