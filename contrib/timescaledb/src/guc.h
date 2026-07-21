/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_GUC_H
#define TIMESCALEDB_GUC_H

#include <postgres.h>
#include "export.h"

extern bool ts_telemetry_on(void);


void _guc_init(void);
void _guc_fini(void);

#endif /* TIMESCALEDB_GUC_H */
