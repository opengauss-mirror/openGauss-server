/* -------------------------------------------------------------------------
 *
 * view.h
 *
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/view.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef VIEW_H
#define VIEW_H

#include "nodes/parsenodes.h"

extern void DefineView(ViewStmt* stmt, const char* queryString, bool isFirstNode = true);
extern bool IsViewTemp(ViewStmt* stmt, const char* queryString);
extern void StoreViewQuery(Oid viewOid, Query* viewParse, bool replace);

#endif /* VIEW_H */
