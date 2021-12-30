/* --------------------------------------------------------------------
 * guc_sql.h
 *
 * External declarations pertaining to backend/utils/misc/guc-file.l
 * and backend/utils/misc/guc/guc_sql.cpp
 *
 * Copyright (c) 2000-2012, PostgreSQL Global Development Group
 * Written by Peter Eisentraut <peter_e@gmx.net>.
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 * src/include/utils/guc_sql.h
 * --------------------------------------------------------------------
 */
#ifndef GUC_SQL_H
#define GUC_SQL_H

extern void InitSqlConfigureNames();
extern char* apply_num_width(double num);
extern char* apply_num_format(double num);

#endif /* GUC_SQL_H */
