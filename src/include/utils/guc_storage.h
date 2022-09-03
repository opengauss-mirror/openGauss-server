/* --------------------------------------------------------------------
 * guc_storage.h
 *
 * External declarations pertaining to backend/utils/misc/guc-file.l
 * and backend/utils/misc/guc/guc_storage.cpp
 *
 * Copyright (c) 2000-2012, PostgreSQL Global Development Group
 * Written by Peter Eisentraut <peter_e@gmx.net>.
 *
 * src/include/utils/guc_storage.h
 * --------------------------------------------------------------------
 */
#ifndef GUC_STORAGE_H
#define GUC_STORAGE_H

extern void InitStorageConfigureNames();
extern bool check_enable_gtm_free(bool* newval, void** extra, GucSource source);
extern void InitializeNumLwLockPartitions(void);
extern bool need_check_repl_uuid(GucContext ctx);

#endif /* GUC_STORAGE_H */
