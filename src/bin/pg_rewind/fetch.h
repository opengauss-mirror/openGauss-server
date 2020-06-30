/* -------------------------------------------------------------------------
 *
 * fetch.h
 *	  Fetching data from a local or remote data directory.
 *
 * This file includes the prototypes for functions used to copy files from
 * one data directory to another. The source to copy from can be a local
 * directory (copy method), or a remote PostgreSQL server (libpq fetch
 * method).
 *
 * Copyright (c) 2013-2015, PostgreSQL Global Development Group
 *
 * -------------------------------------------------------------------------
 */
#ifndef FETCH_H
#define FETCH_H

#include "c.h"

#include "access/xlogdefs.h"

#include "filemap.h"

#define NAMEDATALEN 64

/* save the source slot_name */
extern char source_slot_name[NAMEDATALEN];

/*
 * Common interface. Calls the copy or libpq method depending on global
 * config options.
 */
extern BuildErrorCode fetchSourceFileList();
extern char* fetchFile(char* filename, size_t* filesize);
extern BuildErrorCode executeWalDataMap(filemap_t* map);
extern BuildErrorCode executeFileMap(filemap_t* map, FILE *file);
extern BuildErrorCode libpqConnect(const char* connstr);
extern bool checkDummyStandbyConnection(void);
extern void libpqDisconnect(void);
extern BuildErrorCode libpqGetParameters(void);
extern XLogRecPtr libpqGetCurrentXlogInsertLocation(void);

extern void libpqGetSourceSlot(XLogRecPtr* recptr);
extern void libpqRequestCheckpoint(void);

extern char* libpqGetTargetSlotName();

typedef void (*process_file_callback_t)(const char* path, file_type_t type, size_t size, const char* link_target);
extern BuildErrorCode traverse_datadir(const char* datadir, process_file_callback_t callback);

extern void get_source_slotname(void);
extern BuildErrorCode backupFileMap(filemap_t* map, const char* lastoff);

extern bool checkDummyStandbyConnection(void);

#endif /* FETCH_H */

