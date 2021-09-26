/* ---------------------------------------------------------------------------------------
 * 
 * checksum.h
 *	  Checksum implementation for data pages.
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * 
 * 
 * IDENTIFICATION
 *        src/include/storage/checksum.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef CHECKSUM_H
#define CHECKSUM_H

#include "storage/buf/block.h"

/*
 * Compute the checksum for a openGauss page.  The page must be aligned on a
 * 4-byte boundary.
 */
extern uint16 pg_checksum_page(char* page, BlockNumber blkno);

#endif /* CHECKSUM_H */
