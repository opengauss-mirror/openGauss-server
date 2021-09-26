/* ---------------------------------------------------------------------------------------
 * 
 * relfilenodemap.h
 *        relfilenode to oid mapping cache.
 *
 * Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * 
 * IDENTIFICATION
 *        src/include/utils/relfilenodemap.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef RELFILENODEMAP_H
#define RELFILENODEMAP_H

extern Oid RelidByRelfilenode(Oid reltablespace, Oid relfilenode, bool segment);
extern Oid PartitionRelidByRelfilenode(Oid reltablespace, Oid relfilenode, Oid &partationReltoastrelid,
                                       Oid *partitionOid, bool segment);
extern Oid UHeapRelidByRelfilenode(Oid reltablespace, Oid relfilenode);
extern Oid UHeapPartitionRelidByRelfilenode(Oid reltablespace, Oid relfilenode, Oid& partationReltoastrelid);

#endif /* RELFILENODEMAP_H */
