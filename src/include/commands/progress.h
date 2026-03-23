/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 *
 * progress.h
 *	  Constants used with the progress reporting facilities defined in
 *	  backend_status.h.  These are possibly interesting to extensions, so we
 *	  expose them via this header file.  Note that if you update these
 *	  constants, you probably also need to update the views based on them
 *	  in system_views.sql.

 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2025-2025, Huawei Technologies Co., Ltd. All rights reserved.
 *
 * src/include/commands/progress.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef PROGRESS_H
#define PROGRESS_H

/* Progress parameters for PROGRESS_COPY */
#define PROGRESS_COPY_BYTES_PROCESSED 0 // param1
#define PROGRESS_COPY_BYTES_TOTAL 1
#define PROGRESS_COPY_TUPLES_PROCESSED 2
#define PROGRESS_COPY_TUPLES_EXCLUDED 3
#define PROGRESS_COPY_COMMAND 4
#define PROGRESS_COPY_TYPE 5

/* Commands of COPY (as advertised via PROGRESS_COPY_COMMAND) */
#define PROGRESS_COPY_COMMAND_FROM 1
#define PROGRESS_COPY_COMMAND_TO 2

/* Types of COPY commands (as advertised via PROGRESS_COPY_TYPE) */
#define PROGRESS_COPY_TYPE_FILE 1
#define PROGRESS_COPY_TYPE_PIPE 2
#define PROGRESS_COPY_TYPE_CALLBACK 3

#endif
