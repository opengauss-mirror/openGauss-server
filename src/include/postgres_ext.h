/* -------------------------------------------------------------------------
 *
 * postgres_ext.h
 *
 *	   This file contains declarations of things that are visible everywhere
 *	in openGauss *and* are visible to clients of frontend interface libraries.
 *	For example, the Oid type is part of the API of libpq and other libraries.
 *
 *	   Declarations which are specific to a particular interface should
 *	go in the header file for that interface (such as libpq-fe.h).	This
 *	file is only for fundamental openGauss declarations.
 *
 *	   User-written C functions don't count as "external to openGauss."
 *	Those function much as local modifications to the backend itself, and
 *	use header files that are otherwise internal to openGauss to interface
 *	with the backend.
 *
 * src/include/postgres_ext.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef POSTGRES_EXT_H
#define POSTGRES_EXT_H

#include "gs_thread.h"
/*
 * Object ID is a fundamental type in openGauss.
 */
typedef unsigned int Oid;

#ifdef __cplusplus
#define InvalidOid (Oid(0))
#else
#define InvalidOid ((Oid)0)
#endif

#define VirtualBktOid     (Oid(1))
#define VirtualSegmentOid (Oid(2))
#define InvalidBktId      (-1)
#define SegmentBktId      (16384)

#define BUCKET_NODE_IS_VALID(bucket_node) ((bucket_node) > InvalidBktId && (bucket_node) < SegmentBktId)
#define BUCKET_OID_IS_VALID(bucketOid) ((bucketOid) >= FirstNormalObjectId)

#define OID_MAX  UINT_MAX

/* you will need to include <limits.h> to use the above #define */

/*
 * Identifiers of error message fields.  Kept here to keep common
 * between frontend and backend, and also to export them to libpq
 * applications.
 */
#define PG_DIAG_SEVERITY 'S'
#define PG_DIAG_SQLSTATE 'C'
#define PG_DIAG_INTERNEL_ERRCODE 'c'
#define PG_DIAG_MESSAGE_PRIMARY 'M'
#define PG_DIAG_MESSAGE_DETAIL 'D'
#define PG_DIAG_MESSAGE_HINT 'H'
#define PG_DIAG_STATEMENT_POSITION 'P'
#define PG_DIAG_INTERNAL_POSITION 'p'
#define PG_DIAG_INTERNAL_QUERY 'q'
#define PG_DIAG_CONTEXT 'W'
#define PG_DIAG_SOURCE_FILE 'F'
#define PG_DIAG_SOURCE_LINE 'L'
#define PG_DIAG_SOURCE_FUNCTION 'R'
#define PG_DIAG_MESSAGE_ONLY 'm'
#define PG_DIAG_MODULE_ID 'd'

#endif
