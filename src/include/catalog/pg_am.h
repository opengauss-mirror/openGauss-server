/* -------------------------------------------------------------------------
 *
 * pg_am.h
 *	  definition of the system "access method" relation (pg_am)
 *	  along with the relation's initial contents.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_am.h
 *
 * NOTES
 *		the genbki.pl script reads this file and generates .bki
 *		information from the DATA() statements.
 *
 *		XXX do NOT break up DATA() statements into multiple lines!
 *			the scripts are not as smart as you might think...
 *
 * -------------------------------------------------------------------------
 */
#ifndef PG_AM_H
#define PG_AM_H

#include "catalog/genbki.h"

/* ----------------
 *		pg_am definition.  cpp turns this into
 *		typedef struct FormData_pg_am
 * ----------------
 */
#define AccessMethodRelationId	2601
#define AccessMethodRelation_Rowtype_Id 10164

CATALOG(pg_am,2601) BKI_SCHEMA_MACRO
{
	NameData	amname;			/* access method name */
	int2		amstrategies;	/* total number of strategies (operators) by
								 * which we can traverse/search this AM. Zero
								 * if AM does not have a fixed set of strategy
								 * assignments. */
	int2		amsupport;		/* total number of support functions that this
								 * AM uses */
	bool		amcanorder;		/* does AM support order by column value? */
	bool		amcanorderbyop; /* does AM support order by operator result? */
	bool		amcanbackward;	/* does AM support backward scan? */
	bool		amcanunique;	/* does AM support UNIQUE indexes? */
	bool		amcanmulticol;	/* does AM support multi-column indexes? */
	bool		amoptionalkey;	/* can query omit key for the first column? */
	bool		amsearcharray;	/* can AM handle ScalarArrayOpExpr quals? */
	bool		amsearchnulls;	/* can AM search for NULL/NOT NULL entries? */
	bool		amstorage;		/* can storage type differ from column type? */
	bool		amclusterable;	/* does AM support cluster command? */
	bool		ampredlocks;	/* does AM handle predicate locks? */
	Oid			amkeytype;		/* type of data in index, or InvalidOid */
	regproc		aminsert;		/* "insert this tuple" function */
	regproc		ambeginscan;	/* "prepare for index scan" function */
	regproc		amgettuple;		/* "next valid tuple" function, or 0 */
	regproc		amgetbitmap;	/* "fetch all valid tuples" function, or 0 */
	regproc		amrescan;		/* "(re)start index scan" function */
	regproc		amendscan;		/* "end index scan" function */
	regproc		ammarkpos;		/* "mark current scan position" function */
	regproc		amrestrpos;		/* "restore marked scan position" function */
	regproc		ammerge;		/* "merge several index relation into one" function */
	regproc		ambuild;		/* "build new index" function */
	regproc		ambuildempty;	/* "build empty index" function */
	regproc		ambulkdelete;	/* bulk-delete function */
	regproc		amvacuumcleanup;	/* post-VACUUM cleanup function */
	regproc		amcanreturn;	/* can indexscan return IndexTuples? */
	regproc		amcostestimate; /* estimate cost of an indexscan */
	regproc		amoptions;		/* parse AM-specific parameters */
	regproc		amhandler;		/* handler function */
	regproc		amdelete;		/* index delete function */
} FormData_pg_am;

/* ----------------
 *		Form_pg_am corresponds to a pointer to a tuple with
 *		the format of pg_am relation.
 * ----------------
 */
typedef FormData_pg_am *Form_pg_am;

/* ----------------
 *		compiler constants for pg_am
 * ----------------
 */
#define Natts_pg_am						33
#define Anum_pg_am_amname				1
#define Anum_pg_am_amstrategies			2
#define Anum_pg_am_amsupport			3
#define Anum_pg_am_amcanorder			4
#define Anum_pg_am_amcanorderbyop		5
#define Anum_pg_am_amcanbackward		6
#define Anum_pg_am_amcanunique			7
#define Anum_pg_am_amcanmulticol		8
#define Anum_pg_am_amoptionalkey		9
#define Anum_pg_am_amsearcharray		10
#define Anum_pg_am_amsearchnulls		11
#define Anum_pg_am_amstorage			12
#define Anum_pg_am_amclusterable		13
#define Anum_pg_am_ampredlocks			14
#define Anum_pg_am_amkeytype			15
#define Anum_pg_am_aminsert				16
#define Anum_pg_am_ambeginscan			17
#define Anum_pg_am_amgettuple			18
#define Anum_pg_am_amgetbitmap			19
#define Anum_pg_am_amrescan				20
#define Anum_pg_am_amendscan			21
#define Anum_pg_am_ammarkpos			22
#define Anum_pg_am_amrestrpos			23
#define Anum_pg_am_ammerge				24
#define Anum_pg_am_ambuild				25
#define Anum_pg_am_ambuildempty			26
#define Anum_pg_am_ambulkdelete			27
#define Anum_pg_am_amvacuumcleanup		28
#define Anum_pg_am_amcanreturn			29
#define Anum_pg_am_amcostestimate		30
#define Anum_pg_am_amoptions			31
#define Anum_pg_am_amhandler			32
#define Anum_pg_am_amdelete				33

/* ----------------
 *		initial contents of pg_am
 * ----------------
 */

DATA(insert OID = 403 (  btree		5 3 t f t t t t t t f t t 0 btinsert btbeginscan btgettuple btgetbitmap btrescan btendscan btmarkpos btrestrpos btmerge btbuild btbuildempty btbulkdelete btvacuumcleanup btcanreturn btcostestimate btoptions - -));
DESCR("b-tree index access method");
#define BTREE_AM_OID 403
DATA(insert OID = 405 (  hash		1 1 f f t f f f f f f f f 23 hashinsert hashbeginscan hashgettuple hashgetbitmap hashrescan hashendscan hashmarkpos hashrestrpos hashmerge hashbuild hashbuildempty hashbulkdelete hashvacuumcleanup - hashcostestimate hashoptions - -));
DESCR("hash index access method");
#define HASH_AM_OID 405
DATA(insert OID = 783 (  gist		0 8 f t f f t t f t t t f 0 gistinsert gistbeginscan gistgettuple gistgetbitmap gistrescan gistendscan gistmarkpos gistrestrpos gistmerge gistbuild gistbuildempty gistbulkdelete gistvacuumcleanup - gistcostestimate gistoptions - -));
DESCR("GiST index access method");
#define GIST_AM_OID 783
DATA(insert OID = 2742 (  gin		0 6 f f f f t t f f t f f 0 gininsert ginbeginscan - gingetbitmap ginrescan ginendscan ginmarkpos ginrestrpos ginmerge ginbuild ginbuildempty ginbulkdelete ginvacuumcleanup - gincostestimate ginoptions - -));
DESCR("GIN index access method");
#define GIN_AM_OID 2742
DATA(insert OID = 4000 (  spgist	0 5 f f f f f t f t f f f 0 spginsert spgbeginscan spggettuple spggetbitmap spgrescan spgendscan spgmarkpos spgrestrpos spgmerge spgbuild spgbuildempty spgbulkdelete spgvacuumcleanup spgcanreturn spgcostestimate spgoptions - -));
DESCR("SP-GiST index access method");
#define SPGIST_AM_OID 4000

DATA(insert OID = 4039 (  psort		5 1 f f f f t t f t f f f 0 - - psortgettuple psortgetbitmap - - - - - psortbuild - - - psortcanreturn psortcostestimate psortoptions - -));
DESCR("psort index access method");
#define PSORT_AM_OID 4039

DATA(insert OID = 4239 (  cbtree		5 1 f f f t t t f t f f t 0 btinsert btbeginscan cbtreegettuple cbtreegetbitmap btrescan btendscan - - - cbtreebuild btbuildempty - - cbtreecanreturn cbtreecostestimate cbtreeoptions - -));
DESCR("cstore btree index access method");
#define CBTREE_AM_OID 4239

DATA(insert OID = 4444 (  cgin		0 6 f f f f t t f f t f f 0 gininsert ginbeginscan - cgingetbitmap ginrescan ginendscan ginmarkpos ginrestrpos ginmerge cginbuild ginbuildempty ginbulkdelete ginvacuumcleanup - gincostestimate ginoptions - -));
DESCR("cstore GIN index access method");
#define CGIN_AM_OID 4444

DATA(insert OID = 4439 (  ubtree		5 3 t f t t t t t t f t t 0 ubtinsert ubtbeginscan ubtgettuple ubtgetbitmap ubtrescan ubtendscan ubtmarkpos ubtrestrpos ubtmerge ubtbuild ubtbuildempty ubtbulkdelete ubtvacuumcleanup ubtcanreturn ubtcostestimate ubtoptions - -));
DESCR("ustore b-tree index access method");
#define UBTREE_AM_OID 4439

DATA(insert OID = 8300 (  hnsw		0 4 f t f f f t f f f f f 0 hnswinsert hnswbeginscan hnswgettuple - hnswrescan hnswendscan - - - hnswbuild hnswbuildempty hnswbulkdelete hnswvacuumcleanup - hnswcostestimate hnswoptions - hnswdelete));
DESCR("hnsw index access method");
#define HNSW_AM_OID 8300

DATA(insert OID = 8301 (  ivfflat		0 5 f t f f f t f f f f f 0 ivfflatinsert ivfflatbeginscan ivfflatgettuple - ivfflatrescan ivfflatendscan - - - ivfflatbuild ivfflatbuildempty ivfflatbulkdelete ivfflatvacuumcleanup - ivfflatcostestimate ivfflatoptions - -));
DESCR("ivfflat index access method");
#define IVFFLAT_AM_OID 8301

DATA(insert OID = 8302 (  bm25		0 2 f t f f f t f f f f f 0 bm25insert bm25beginscan bm25gettuple - bm25rescan bm25endscan - - - bm25build bm25buildempty bm25bulkdelete bm25vacuumcleanup - bm25costestimate bm25options - -));
DESCR("bm25 index access method");
#define BM25_AM_OID 8302

DATA(insert OID = 8303 (  diskann		0 4 f t f f f t f f f f f 0 diskanninsert diskannbeginscan diskanngettuple - diskannrescan diskannendscan - - - diskannbuild diskannbuildempty diskannbulkdelete diskannvacuumcleanup - diskanncostestimate diskannoptions - -));
DESCR("diskann index access method");
#define DISKANN_AM_OID 8303

#define OID_IS_BTREE(oid) ((oid) == BTREE_AM_OID || (oid) == UBTREE_AM_OID)

#endif   /* PG_AM_H */
