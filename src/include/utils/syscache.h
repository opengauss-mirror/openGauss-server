/* -------------------------------------------------------------------------
 *
 * syscache.h
 *	  System catalog cache definitions.
 *
 * See also lsyscache.h, which provides convenience routines for
 * common cache-lookup operations.
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * Portions Copyright (c) 2021, openGauss Contributors
 * src/include/utils/syscache.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef SYSCACHE_H
#define SYSCACHE_H

#include "utils/catcache.h"

/*
 *		SysCache identifiers.
 *
 *		The order of these identifiers must match the order
 *		of the entries in the array cacheinfo[] in syscache.c.
 *		Keep them in alphabetical order (renumbering only costs a
 *		backend rebuild).
 */
#define STATRELATTINH STATRELKINDATTINH

enum SysCacheIdentifier {
    AGGFNOID = 0,
    AMNAME,
    AMOID,
    AMOPOPID,
    AMOPSTRATEGY,
    AMPROCNUM,
    ATTNAME,
    ATTNUM,
    AUTHMEMMEMROLE,
    AUTHMEMROLEMEM,
    AUTHNAME,
    AUTHOID,
    BUCKETRELID,
    CASTSOURCETARGET,
    CEOID,
    CERELIDCOUMNNAME,
    CLAAMNAMENSP,
    CLAOID,
    COLLNAMEENCNSP,
    COLLOID,
    COLUMNSETTINGDISTID,
    COLUMNSETTINGNAME,
    COLUMNSETTINGOID,
    CONDEFAULT,
    CONNAMENSP,
    CONSTROID,
    CONSTRRELID,
    CONVOID,
    DATABASEOID,
    DATASOURCENAME,
    DATASOURCEOID,
    SUBSCRIPTIONRELMAP,
    DB4AI_MODEL,
    DEFACLROLENSPOBJ,
    DIRECTORYNAME,
    DIRECTORYOID,
    ENUMOID,
    ENUMTYPOIDNAME,
    FOREIGNDATAWRAPPERNAME,
    FOREIGNDATAWRAPPEROID,
    FOREIGNSERVERNAME,
    FOREIGNSERVEROID,
    FOREIGNTABLEREL,
    GLOBALSETTINGNAME,
    GLOBALSETTINGOID,
    GSCLPROCID,
    GSCLPROCOID,
    JOBARGUMENTNAME,
    JOBARGUMENTPOSITION,
    JOBATTRIBUTENAME,
    INDEXRELID,
    LANGNAME,
    LANGOID,
    OPTMODEL,
    NAMESPACENAME,
    NAMESPACEOID,
    OPERNAMENSP,
    OPEROID,
    OPFAMILYAMNAMENSP,
    OPFAMILYOID,
    PARTRELID,
    PARTPARTOID,
    PARTINDEXTBLPARENTOID,
    PGJOBID,
    PGJOBPROCID,
    PGOBJECTID,
#ifdef PGXC
    PGXCCLASSRELID,
    PGXCGROUPNAME,
    PGXCGROUPOID,
    PGXCNODENAME,
    PGXCNODEOID,
    PGXCNODEIDENTIFIER,
    PGXCRESOURCEPOOLNAME,
    PGXCRESOURCEPOOLOID,
    PGXCWORKLOADGROUPNAME,
    PGXCWORKLOADGROUPOID,
    PGXCAPPWGMAPPINGNAME,
    PGXCAPPWGMAPPINGOID,
    PGXCSLICERELID,
#endif
    SUBSCRIPTIONNAME,
    SUBSCRIPTIONOID,
    PROCNAMEARGSNSP,
#ifndef ENABLE_MULTIPLE_NODES
    PROCALLARGS,
#endif
    PROCOID,
    RANGETYPE,
    RELNAMENSP,
    RELOID,
    RULERELNAME,
    STATRELKINDATTINH, /* single column statistics */
    STATRELKINDKEYINH, /* multi column statistics */
    STREAMCQID,
#ifdef ENABLE_MULTIPLE_NODES
    STREAMCQLOOKUPID,
    STREAMCQMATRELID,
    STREAMCQOID,
#endif
#ifndef ENABLE_MULTIPLE_NODES
    OBJECTTYPEOID,
    OBJECTTYPE,
#endif
#ifdef ENABLE_MULTIPLE_NODES
    STREAMCQOID,
    STREAMCQRELID,
    STREAMCQSCHEMACHANGE,
#endif
#ifndef ENABLE_MULTIPLE_NODES
    PROCEDUREEXTENSIONOID,
    EVENTTRIGGERNAME,
    EVENTTRIGGEROID,
#endif
    STREAMOID,
    STREAMRELID,
    REAPERCQOID,
    PUBLICATIONRELMAP,
    SYNOID,
    SYNONYMNAMENSP,
    TABLESPACEOID,
    TSCONFIGMAP,
    TSCONFIGNAMENSP,
    TSCONFIGOID,
    TSDICTNAMENSP,
    TSDICTOID,
    TSPARSERNAMENSP,
    TSPARSEROID,
    TSTEMPLATENAMENSP,
    TSTEMPLATEOID,
    TYPENAMENSP,
    TYPEOID,
    USERMAPPINGOID,
    USERMAPPINGUSERSERVER,
    USERSTATUSOID,
    USERSTATUSROLEID,
    STREAMINGGATHERAGGOID,
    PACKAGEOID,
    PKGNAMENSP,
    PUBLICATIONNAME,
    PUBLICATIONOID,
    UIDRELID,
    DBPRIVOID,
    DBPRIVROLE,
    DBPRIVROLEPRIV,
    SETTYPOIDNAME,
    GSSQLLIMITRULE
};
struct cachedesc {
    Oid reloid;   /* OID of the relation being cached */
    Oid indoid;   /* OID of index relation for this cache */
    int nkeys;    /* # of keys needed for cache lookup */
    int key[CATCACHE_MAXKEYS];   /* attribute numbers of key attrs */
    int nbuckets; /* number of hash buckets for this cache */
};
extern const cachedesc cacheinfo[];
extern int SysCacheSize;

extern void InitCatalogCache(void);
extern void InitCatalogCachePhase2(void);

extern HeapTuple SearchSysCache(int cacheId, Datum key1, Datum key2, Datum key3, Datum key4, int level = DEBUG2);
/*
 * The use of argument specific numbers is encouraged. They're faster, andTENAMENSP,TENAMENSP,
 * insulates the caller from changes in the maximum number of keys.
 */
extern HeapTuple SearchSysCache1(int cacheId, Datum key1);
extern HeapTuple SearchSysCache2(int cacheId, Datum key1, Datum key2);
extern HeapTuple SearchSysCache3(int cacheId, Datum key1, Datum key2, Datum key3);
extern HeapTuple SearchSysCache4(int cacheId, Datum key1, Datum key2, Datum key3, Datum key4);

extern void ReleaseSysCache(HeapTuple tuple);
extern void ReleaseSysCacheList(catclist *cl);

/* convenience routines */
extern HeapTuple SearchSysCacheCopy(int cacheId, Datum key1, Datum key2, Datum key3, Datum key4, int level = DEBUG2);
extern bool SearchSysCacheExists(int cacheId, Datum key1, Datum key2, Datum key3, Datum key4);

extern Oid GetSysCacheOid(int cacheId, Datum key1, Datum key2, Datum key3, Datum key4);

extern HeapTuple SearchSysCacheAttName(Oid relid, const char* attname);
extern HeapTuple SearchSysCacheCopyAttName(Oid relid, const char* attname);
extern HeapTuple SearchSysCacheAttNum(Oid relid, int16 attnum);
extern HeapTuple SearchSysCacheCopyAttNum(Oid relid, int16 attnum);
extern bool SearchSysCacheExistsAttName(Oid relid, const char* attname);

extern Datum SysCacheGetAttr(int cacheId, HeapTuple tup, AttrNumber attributeNumber, bool* isNull);

extern uint32 GetSysCacheHashValue(int cacheId, Datum key1, Datum key2, Datum key3, Datum key4);

/* list-search interface.  Users of this must import catcache.h too */
extern struct catclist* SearchSysCacheList(int cacheId, int nkeys, Datum key1, Datum key2, Datum key3, Datum key4);

#ifndef ENABLE_MULTIPLE_NODES
extern bool SearchSysCacheExistsForProcAllArgs(Datum key1, Datum key2, Datum key3, Datum key4, Datum proArgModes);
#endif

/*
 * The use of the macros below rather than direct calls to the corresponding
 * functions is encouraged, as it insulates the caller from changes in the
 * maximum number of keys.
 */
#define SearchSysCache1WithLogLevel(cacheId, key1, level) SearchSysCache(cacheId, key1, 0, 0, 0, level)

#define SearchSysCacheCopy1(cacheId, key1) SearchSysCacheCopy(cacheId, key1, 0, 0, 0)
#define SearchSysCacheCopy2(cacheId, key1, key2) SearchSysCacheCopy(cacheId, key1, key2, 0, 0)
#define SearchSysCacheCopy3(cacheId, key1, key2, key3) SearchSysCacheCopy(cacheId, key1, key2, key3, 0)
#define SearchSysCacheCopy4(cacheId, key1, key2, key3, key4) SearchSysCacheCopy(cacheId, key1, key2, key3, key4)
#define SearchSysCacheCopyWithLogLevel(cacheId, key1, level) SearchSysCacheCopy(cacheId, key1, 0, 0, 0, level)

#define SearchSysCacheExists1(cacheId, key1) SearchSysCacheExists(cacheId, key1, 0, 0, 0)
#define SearchSysCacheExists2(cacheId, key1, key2) SearchSysCacheExists(cacheId, key1, key2, 0, 0)
#define SearchSysCacheExists3(cacheId, key1, key2, key3) SearchSysCacheExists(cacheId, key1, key2, key3, 0)
#define SearchSysCacheExists4(cacheId, key1, key2, key3, key4) SearchSysCacheExists(cacheId, key1, key2, key3, key4)

#define GetSysCacheOid1(cacheId, key1) GetSysCacheOid(cacheId, key1, 0, 0, 0)
#define GetSysCacheOid2(cacheId, key1, key2) GetSysCacheOid(cacheId, key1, key2, 0, 0)
#define GetSysCacheOid3(cacheId, key1, key2, key3) GetSysCacheOid(cacheId, key1, key2, key3, 0)
#define GetSysCacheOid4(cacheId, key1, key2, key3, key4) GetSysCacheOid(cacheId, key1, key2, key3, key4)

#define GetSysCacheHashValue1(cacheId, key1) GetSysCacheHashValue(cacheId, key1, 0, 0, 0)
#define GetSysCacheHashValue2(cacheId, key1, key2) GetSysCacheHashValue(cacheId, key1, key2, 0, 0)
#define GetSysCacheHashValue3(cacheId, key1, key2, key3) GetSysCacheHashValue(cacheId, key1, key2, key3, 0)
#define GetSysCacheHashValue4(cacheId, key1, key2, key3, key4) GetSysCacheHashValue(cacheId, key1, key2, key3, key4)

#define SearchSysCacheList1(cacheId, key1) SearchSysCacheList(cacheId, 1, key1, 0, 0, 0)
#define SearchSysCacheList2(cacheId, key1, key2) SearchSysCacheList(cacheId, 2, key1, key2, 0, 0)
#define SearchSysCacheList3(cacheId, key1, key2, key3) SearchSysCacheList(cacheId, 3, key1, key2, key3, 0)
#define SearchSysCacheList4(cacheId, key1, key2, key3, key4) SearchSysCacheList(cacheId, 4, key1, key2, key3, key4)

#endif /* SYSCACHE_H */
