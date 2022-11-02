/* --------------------------------------------------------------------
 * guc_securit.h
 *
 * External declarations pertaining to backend/utils/misc/guc-file.l
 * and backend/utils/misc/guc/guc_securit.cpp
 *
 * Copyright (c) 2000-2012, PostgreSQL Global Development Group
 * Written by Peter Eisentraut <peter_e@gmx.net>.
 *
 * src/include/utils/guc_securit.h
 * --------------------------------------------------------------------
 */
#ifndef GUC_SECURIT_H
#define GUC_SECURIT_H

extern void InitSecurityConfigureNames();
extern const int MAX_PASSWORD_LENGTH;

#endif /* GUC_SECURIT_H */
