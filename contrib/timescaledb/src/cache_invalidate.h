/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef CACHE_INVALIDATE_H
#define CACHE_INVALIDATE_H
#include <postgres.h>
#include <access/xact.h>
#include <utils/lsyscache.h>
#include <utils/inval.h>
#include <catalog/namespace.h>
#include <nodes/nodes.h>
#include <miscadmin.h>

#include "catalog.h"
#include "compat.h"
#include "extension.h"
#include "hypertable_cache.h"

#include "bgw/scheduler.h"

extern void _cache_invalidate_init(void);
extern void _cache_invalidate_fini(void);

#endif /* CACHE_INVALIDATE_H */