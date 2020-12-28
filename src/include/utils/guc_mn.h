/* --------------------------------------------------------------------
 * guc.h
 *
 * External declarations pertaining to backend/utils/misc/guc.c and
 * backend/utils/misc/guc-file.l
 *
 * Copyright (c) 2000-2012, PostgreSQL Global Development Group
 * Written by Peter Eisentraut <peter_e@gmx.net>.
 *
 * src/include/utils/guc.h
 * --------------------------------------------------------------------
 */
#ifndef GUC_MN_H
#define GUC_MN_H

extern void InitConfigureNamesBoolMultipleNode();
extern void InitConfigureNamesIntMultipleNode();
extern void InitConfigureNamesRealMultipleNode();
extern void InitConfigureNamesStringMultipleNode();
extern bool check_enable_gtm_free(bool* newval, void** extra, GucSource source);

#endif /* GUC_MN_H */
