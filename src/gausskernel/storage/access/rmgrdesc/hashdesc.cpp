/* -------------------------------------------------------------------------
 *
 * hashdesc.cpp
 *	  rmgr descriptor routines for access/hash/hash.cpp
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/access/rmgrdesc/hashdesc.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/hash.h"

void hash_desc(StringInfo buf, XLogReaderState *record)
{
    /* nothing to do */
}