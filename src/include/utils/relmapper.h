/* -------------------------------------------------------------------------
 *
 * relmapper.h
 *	  Catalog-to-filenode mapping
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/relmapper.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef RELMAPPER_H
#define RELMAPPER_H

#include "access/xlogreader.h"
#include "lib/stringinfo.h"

/*
 * The map file is critical data: we have no automatic method for recovering
 * from loss or corruption of it.  We use a CRC so that we can detect
 * corruption.	To minimize the risk of failed updates, the map file should
 * be kept to no more than one standard-size disk sector (ie 512 bytes),
 * and we use overwrite-in-place rather than playing renaming games.
 * The struct layout below is designed to occupy exactly 512 bytes, which
 * might make filesystem updates a bit more efficient.
 *
 * Entries in the mappings[] array are in no particular order.	We could
 * speed searching by insisting on OID order, but it really shouldn't be
 * worth the trouble given the intended size of the mapping sets.
 */
#define RELMAPPER_FILENAME "pg_filenode.map"
#define RELMAPPER_FILENAME_BAK "pg_filenode.map.backup"

#define RELMAPPER_FILEMAGIC 0x592717 /* version ID value */
#define RELMAPPER_FILEMAGIC_4K 0x592718 /* version ID value */

#define IS_NEW_RELMAP(magic) (magic == RELMAPPER_FILEMAGIC_4K)
#define IS_MAGIC_EXIST(magic) (magic == RELMAPPER_FILEMAGIC_4K || magic == RELMAPPER_FILEMAGIC)

#define MAX_MAPPINGS 62         /* old relmap 62 * 8 + 16 = 512 */
#define MAX_MAPPINGS_4K 510     /* new relmap 510 * 8 + 16 = 4096 */

#define MAPPING_LEN_OLDMAP_HEAD (offsetof(RelMapFile, mappings) + MAX_MAPPINGS * sizeof(RelMapping))
#define MAPPING_LEN_TAIL (8)

#define RELMAP_SIZE_OLD (MAPPING_LEN_OLDMAP_HEAD + MAPPING_LEN_TAIL)
#define RELMAP_SIZE_NEW (sizeof(RelMapFile))

typedef struct RelMapping {
    Oid mapoid;      /* OID of a catalog */
    Oid mapfilenode; /* its filenode number */
} RelMapping;

typedef struct RelMapFile {
    int32 magic;        /* always RELMAPPER_FILEMAGIC */
    int32 num_mappings; /* number of valid RelMapping entries */
    RelMapping mappings[MAX_MAPPINGS_4K];  /* old relmap only use MAX_MAPPINGS */
    int32 crc; /* CRC of all above */
    int32 pad; /* to make the struct size be 512 exactly */
} RelMapFile;

typedef struct RelMapVerTag {
    int32 relMagic;
    int32 relMaxMappings;
    int32 relSize;
} RelMapVerTag;
/* ----------------
 *		relmap-related XLOG entries
 * ----------------
 */

#define XLOG_RELMAP_UPDATE 0x00

typedef struct xl_relmap_update {
    Oid dbid;                         /* database ID, or 0 for shared map */
    Oid tsid;                         /* database's tablespace, or pg_global */
    int32 nbytes;                     /* size of relmap data */
    char data[FLEXIBLE_ARRAY_MEMBER]; /* VARIABLE LENGTH ARRAY */
} xl_relmap_update;

#define MinSizeOfRelmapUpdate offsetof(xl_relmap_update, data)

extern Oid RelationMapOidToFilenode(Oid relationId, bool shared);
extern Oid RelationMapFilenodeToOid(Oid relationId, bool shared);

extern void RelationMapUpdateMap(Oid relationId, Oid fileNode, bool shared, bool immediate);

extern void RelationMapRemoveMapping(Oid relationId);

extern void RelationMapInvalidate(bool shared);
extern void RelationMapInvalidateAll(void);

extern void AtCCI_RelationMap(void);
extern void AtEOXact_RelationMap(bool isCommit);
extern void AtPrepare_RelationMap(void);

extern void CheckPointRelationMap(void);

extern void RelationMapFinishBootstrap(void);

extern void RelationMapInitialize(void);
extern void RelationMapInitializePhase2(void);
extern void RelationMapInitializePhase3(void);

extern void relmap_redo(XLogReaderState* record);
extern void relmap_desc(StringInfo buf, XLogReaderState* record);
extern const char* relmap_type_name(uint8 subtype);

extern void load_relmap_file(bool shared, RelMapFile *map);
#endif /* RELMAPPER_H */
