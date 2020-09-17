/* -------------------------------------------------------------------------
 *
 * dsm.h
 * manage dynamic shared memory segments
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/dsm.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef DSM_H
#define DSM_H

#define DSM_MAX_ITEM_PER_QUERY 8

/* Startup and shutdown functions. */
#define dsm_cleanup_using_control_segment(oldControlHandle)
#define dsm_postmaster_startup(shmemHeader)
#define dsm_backend_shutdown
#define dsm_detach_all
#define dsm_set_control_handle(dsmHandle)

/* Functions that create or remove mappings. */
extern void *dsm_create(void);
#define dsm_attach(dsmHandle)
extern void dsm_detach(void **seg);

/* Resource management functions. */
#define dsm_pin_mapping(dsmSegment)
#define dsm_unpin_mapping(dsmSegment)
#define dsm_pin_segment(dsmSegment)
#define dsm_unpin_segment(dsmHandle)
#define dsm_find_mapping(dsmHandle)

/* Informational functions. */
#define dsm_segment_address(dsmSegment)
#define dsm_segment_map_length(dsmSegment)
#define dsm_segment_handle(dsmSegment)

/* Cleanup hooks. */
#define on_dsm_detach(dsmSegment, callbackFunc, arg)
#define cancel_on_dsm_detach(dsmSegment, callbackFunc, arg)
#define reset_on_dsm_detach

#endif /* DSM_H */