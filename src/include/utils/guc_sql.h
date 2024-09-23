/* --------------------------------------------------------------------
 * guc_sql.h
 *
 * External declarations pertaining to backend/utils/misc/guc-file.l
 * and backend/utils/misc/guc/guc_sql.cpp
 *
 * Portions Copyright (c) 2021, openGauss Contributors
 * Copyright (c) 2000-2012, PostgreSQL Global Development Group
 * Written by Peter Eisentraut <peter_e@gmx.net>.
 *
 * src/include/utils/guc_sql.h
 * --------------------------------------------------------------------
 */
#ifndef GUC_SQL_H
#define GUC_SQL_H

typedef struct behavior_compat_entry {
    const char* name; /* name of behavior compat entry */
    int64 flag;         /* bit flag position */
} behavior_compat_entry;

extern void InitSqlConfigureNames();

#endif /* GUC_SQL_H */
