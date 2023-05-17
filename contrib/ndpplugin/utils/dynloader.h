/* -------------------------------------------------------------------------
 *
 * dynamic_loader.h
 *
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/dynloader.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef DYNAMIC_LOADER_H
#define DYNAMIC_LOADER_H

#include "common.h"

Status LoadSymbol(void *libHandle, char *symbol, void **symbolHandle);

Status OpenDl(void **libHandle, char *symbol);

void CloseDl(void *libHandle);

#endif /* DYNAMIC_LOADER_H */
