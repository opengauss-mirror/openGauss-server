/* ---------------------------------------------------------------------------------------
 * 
 * segment_test.h
 *		Routines for segment test framework.
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * 
 * 
 * IDENTIFICATION
 *        src/include/utils/segment_test.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef SEGMENT_TEST_H
#define SEGMENT_TEST_H

#include "utils/guc.h"
#include "utils/elog.h"

// work as a switch to enable the macro TEST_STUB
#ifdef USE_ASSERT_CHECKING
#ifdef ENABLE_MULTIPLE_NODES
#define ENABLE_SEGMENT_TEST
#endif

#define MAX_NAME_STR_LEN (256)
typedef struct SegmentTestParam {
    /* use as the guc-control white-box-fault probability
     *  range [0, 100000], probability 0, 1/100000 -> 1
     */
    int guc_probability;

    /* act as the white-box checkpoint tag,
     * eg. "WHITE_BOX_ALL_RANDOM_FAILED"
     */
    char test_stub_name[MAX_NAME_STR_LEN];

    /* fault level, PANIC */
    int elevel;
} SegmentTestParam;

extern bool segment_test_stub_activator(const char* name);
extern void segment_default_error_emit(void);
extern void assign_segment_test_param(const char* newval, void* extra);
extern SegmentTestParam* get_segment_test_param();
extern bool check_segment_test_param(char** newval, void** extra, GucSource source);

#define SEGMETN_TEST_STUB(_stub_name) segment_test_stub_activator((_stub_name))


#define SEGMENT_INVALID "SEGMENT_INVALID"
#define EXTENT_GROUP_INIT_DATA "EXTENT_GROUP_INIT_DATA"
#define EXTENT_GROUP_CREATE_EXTENT "EXTENT_GROUP_CREATE_EXTENT"
#define EXTENT_GROUP_ADD_NEW_GROUP "EXTENT_GROUP_ADD_NEW_GROUP"
#define EXTENT_GROUP_ADD_NEW_GROUP_XLOG "EXTENT_GROUP_ADD_NEW_GROUP_XLOG"
#define EXTENT_GROUP_CRITICAL_SECTION "EXTENT_GROUP_CRITICAL_SECTION"
#define SEG_STORE_EXTEND_EXTENT "SEG_STORE_EXTEND_EXTENT"
#define FREE_EXTENT_ADD_FSM_FORK "FREE_EXTENT_ADD_FSM_FORK"
#define FREE_EXTENT_DROP_EXTENTS "FREE_EXTENT_DROP_EXTENTS"
#define FREE_EXTENT_DROP_BUCKETS "FREE_EXTENT_DROP_BUCKETS"
#define FREE_EXTENT_CREATE_SEGMENT_UNCOMMIT "FREE_EXTENT_CREATE_SEGMENT_UNCOMMIT"
#define FREE_SEGMENT_DROP_SEGMENT "FREE_SEGMENT_DROP_SEGMENT"
#define FREE_EXTENT_INSERT_ONE_BUCKET "FREE_EXTENT_INSERT_ONE_BUCKET"
#define SEGMENT_COPY_BLOCK "SEGMENT_COPY_BLOCK"
#define SEGMENT_COPY_EXTENT "SEGMENT_COPY_EXTENT"
#define SEGMENT_COPY_UPDATE_SEGHEAD "SEGMENT_COPY_UPDATE_SEGHEAD"
#define SEGMENT_SHRINK_INVALIDATE_BUFFER "SEGMENT_SHRINK_INVALIDATE_BUFFER"
#define SEGMENT_SHRINK_FILE_XLOG "SEGMENT_SHRINK_FILE_XLOG"
#define SEGMENT_REPLAY_ATOMIC_OP "SEGMENT_REPLAY_ATOMIC_OP"
#define SEGMENT_FLUSH_MOVED_EXTENT_BUFFER "SEGMENT_FLUSH_MOVED_EXTENT_BUFFER"
#define SEGMENT_REDO_UPDATE_SEGHEAD "SEGMENT_REDO_UPDATE_SEGHEAD"

#endif /* DEBUG */
#endif /* SEGMENT_TEST_H */
