/* -------------------------------------------------------------------------
 *
 * pg_type.h
 *	  definition of the system "type" relation (pg_type)
 *	  along with the relation's initial contents.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 * src/include/catalog/pg_type.h
 *
 * NOTES
 *	  the genbki.pl script reads this file and generates .bki
 *	  information from the DATA() statements.
 *
 * -------------------------------------------------------------------------
 */
#ifndef PG_TYPE_H
#define PG_TYPE_H

#include "catalog/genbki.h"

/* ----------------
 *		pg_type definition.  cpp turns this into
 *		typedef struct FormData_pg_type
 *
 *		Some of the values in a pg_type instance are copied into
 *		pg_attribute instances.  Some parts of Postgres use the pg_type copy,
 *		while others use the pg_attribute copy, so they must match.
 *		See struct FormData_pg_attribute for details.
 * ----------------
 */
#define TypeRelationId	1247
#define TypeRelation_Rowtype_Id  71

CATALOG(pg_type,1247) BKI_BOOTSTRAP BKI_ROWTYPE_OID(71) BKI_SCHEMA_MACRO
{
	NameData	typname;		/* type name */
	Oid			typnamespace;	/* OID of namespace containing this type */
	Oid			typowner;		/* type owner */

	/*
	 * For a fixed-size type, typlen is the number of bytes we use to
	 * represent a value of this type, e.g. 4 for an int4.	But for a
	 * variable-length type, typlen is negative.  We use -1 to indicate a
	 * "varlena" type (one that has a length word), -2 to indicate a
	 * null-terminated C string.
	 */
	int2		typlen;

	/*
     * typbyval determines whether internal openGauss routines pass a value of
     * this type by value or by reference.  typbyval had better be FALSE if
     * the length is not 1, 2, or 4 (or 8 on 8-byte-Datum machines).
	 * Variable-length types are always passed by reference. Note that
	 * typbyval can be false even if the length would allow pass-by-value;
	 * this is currently true for type float4, for example.
	 */
	bool		typbyval;

	/*
	 * typtype is 'b' for a base type, 'c' for a composite type (e.g., a
	 * table's rowtype), 'd' for a domain, 'e' for an enum type, 'p' for a
	 * pseudo-type, or 'r' for a range type. (Use the TYPTYPE macros below.)
	 *
	 * If typtype is 'c', typrelid is the OID of the class' entry in pg_class.
	 * typtype is 'o' for a table of type.
	 */
	char		typtype;

	/*
	 * typcategory and typispreferred help the parser distinguish preferred
	 * and non-preferred coercions.  The category can be any single ASCII
	 * character (but not \0).	The categories used for built-in types are
	 * identified by the TYPCATEGORY macros below.
	 */
	char		typcategory;	/* arbitrary type classification */

	bool		typispreferred; /* is type "preferred" within its category? */

	/*
	 * If typisdefined is false, the entry is only a placeholder (forward
	 * reference).	We know the type name, but not yet anything else about it.
	 */
	bool		typisdefined;

	char		typdelim;		/* delimiter for arrays of this type */

	Oid			typrelid;		/* 0 if not a composite type */

	/*
	 * If typelem is not 0 then it identifies another row in pg_type. The
	 * current type can then be subscripted like an array yielding values of
	 * type typelem. A non-zero typelem does not guarantee this type to be a
	 * "real" array type; some ordinary fixed-length types can also be
	 * subscripted (e.g., name, point). Variable-length types can *not* be
	 * turned into pseudo-arrays like that. Hence, the way to determine
	 * whether a type is a "true" array type is if:
	 *
	 * typelem != 0 and typlen == -1.
	 * 
	 * if typtype is 'o' (e.g table of A),  typelem means the Oid of A in pg_type
	 */
	Oid			typelem;

	/*
	 * If there is a "true" array type having this type as element type,
	 * typarray links to it.  Zero if no associated "true" array type.
	 */
	Oid			typarray;

	/*
	 * I/O conversion procedures for the datatype.
	 */
	regproc		typinput;		/* text format (required) */
	regproc		typoutput;
	regproc		typreceive;		/* binary format (optional) */
	regproc		typsend;

	/*
	 * I/O functions for optional type modifiers.
	 */
	regproc		typmodin;
	regproc		typmodout;

	/*
	 * Custom ANALYZE procedure for the datatype (0 selects the default).
	 */
	regproc		typanalyze;

	/* ----------------
	 * typalign is the alignment required when storing a value of this
	 * type.  It applies to storage on disk as well as most
	 * representations of the value inside openGauss.  When multiple values
	 * are stored consecutively, such as in the representation of a
	 * complete row on disk, padding is inserted before a datum of this
	 * type so that it begins on the specified boundary.  The alignment
	 * reference is the beginning of the first datum in the sequence.
	 *
	 * 'c' = CHAR alignment, ie no alignment needed.
	 * 's' = SHORT alignment (2 bytes on most machines).
	 * 'i' = INT alignment (4 bytes on most machines).
	 * 'd' = DOUBLE alignment (8 bytes on many machines, but by no means all).
	 *
	 * See include/access/tupmacs.h for the macros that compute these
	 * alignment requirements.	Note also that we allow the nominal alignment
	 * to be violated when storing "packed" varlenas; the TOAST mechanism
	 * takes care of hiding that from most code.
	 *
	 * NOTE: for types used in system tables, it is critical that the
	 * size and alignment defined in pg_type agree with the way that the
	 * compiler will lay out the field in a struct representing a table row.
	 * ----------------
	 */
	char		typalign;

	/* ----------------
	 * typstorage tells if the type is prepared for toasting and what
	 * the default strategy for attributes of this type should be.
	 *
	 * 'p' PLAIN	  type not prepared for toasting
	 * 'e' EXTERNAL   external storage possible, don't try to compress
	 * 'x' EXTENDED   try to compress and store external if required
	 * 'm' MAIN		  like 'x' but try to keep in main tuple
	 * ----------------
	 */
	char		typstorage;

	/*
	 * This flag represents a "NOT NULL" constraint against this datatype.
	 *
	 * If true, the attnotnull column for a corresponding table column using
	 * this datatype will always enforce the NOT NULL constraint.
	 *
	 * Used primarily for domain types.
	 */
	bool		typnotnull;

	/*
	 * Domains use typbasetype to show the base (or domain) type that the
	 * domain is based on.	Zero if the type is not a domain.
	 */
	Oid			typbasetype;

	/*
	 * Domains use typtypmod to record the typmod to be applied to their base
	 * type (-1 if base type does not use a typmod).  -1 if this type is not a
	 * domain.
	 */
	int4		typtypmod;

	/*
	 * typndims is the declared number of dimensions for an array domain type
	 * (i.e., typbasetype is an array type).  Otherwise zero.
	 */
	int4		typndims;

	/*
	 * Collation: 0 if type cannot use collations, DEFAULT_COLLATION_OID for
	 * collatable base types, possibly other OID for domains
	 */
	Oid			typcollation;

#ifdef CATALOG_VARLEN			/* variable-length fields start here */

	/*
	 * If typdefaultbin is not NULL, it is the nodeToString representation of
	 * a default expression for the type.  Currently this is only used for
	 * domains.
	 */
	pg_node_tree typdefaultbin;

	/*
	 * typdefault is NULL if the type has no associated default value. If
	 * typdefaultbin is not NULL, typdefault must contain a human-readable
	 * version of the default expression represented by typdefaultbin. If
	 * typdefaultbin is NULL and typdefault is not, then typdefault is the
	 * external representation of the type's default value, which may be fed
	 * to the type's input converter to produce a constant.
	 */
	text		typdefault;

	/*
	 * Access permissions
	 */
	aclitem		typacl[1];
#endif
} FormData_pg_type;

/* ----------------
 *		Form_pg_type corresponds to a pointer to a row with
 *		the format of pg_type relation.
 * ----------------
 */
typedef FormData_pg_type *Form_pg_type;

/* ----------------
 *		compiler constants for pg_type
 * ----------------
 */
#define Natts_pg_type					30
#define Anum_pg_type_typname			1
#define Anum_pg_type_typnamespace		2
#define Anum_pg_type_typowner			3
#define Anum_pg_type_typlen				4
#define Anum_pg_type_typbyval			5
#define Anum_pg_type_typtype			6
#define Anum_pg_type_typcategory		7
#define Anum_pg_type_typispreferred		8
#define Anum_pg_type_typisdefined		9
#define Anum_pg_type_typdelim			10
#define Anum_pg_type_typrelid			11
#define Anum_pg_type_typelem			12
#define Anum_pg_type_typarray			13
#define Anum_pg_type_typinput			14
#define Anum_pg_type_typoutput			15
#define Anum_pg_type_typreceive			16
#define Anum_pg_type_typsend			17
#define Anum_pg_type_typmodin			18
#define Anum_pg_type_typmodout			19
#define Anum_pg_type_typanalyze			20
#define Anum_pg_type_typalign			21
#define Anum_pg_type_typstorage			22
#define Anum_pg_type_typnotnull			23
#define Anum_pg_type_typbasetype		24
#define Anum_pg_type_typtypmod			25
#define Anum_pg_type_typndims			26
#define Anum_pg_type_typcollation		27
#define Anum_pg_type_typdefaultbin		28
#define Anum_pg_type_typdefault			29
#define Anum_pg_type_typacl				30


/* ----------------
 *		initial contents of pg_type
 * ----------------
 */

/*
 * Keep the following ordered by OID so that later changes can be made more
 * easily.
 *
 * For types used in the system catalogs, make sure the values here match
 * TypInfo[] in bootstrap.c.
 */

/* OIDS 1 - 99 */
DATA(insert OID = 16 (	bool	   PGNSP PGUID	1 t b B t t \054 0	 0 1000 boolin boolout boolrecv boolsend - - - c p f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("boolean, 'true'/'false'");
#define BOOLOID			16

DATA(insert OID = 17 (	bytea	   PGNSP PGUID -1 f b U f t \054 0	0 1001 byteain byteaout bytearecv byteasend - - - i x f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("variable-length string, binary values escaped");
#define BYTEAOID		17

DATA(insert OID = 18 (	char	   PGNSP PGUID	1 t b S f t \054 0	 0 1002 charin charout charrecv charsend - - - c p f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("single character");
#define CHAROID			18

DATA(insert OID = 19 (	name	   PGNSP PGUID NAMEDATALEN f b S f t \054 0 18 1003 namein nameout namerecv namesend - - - c p f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("63-byte type for storing system identifiers");
#define NAMEOID			19

DATA(insert OID = 20 (	int8	   PGNSP PGUID	8 FLOAT8PASSBYVAL b N f t \054 0	 0 1016 int8in int8out int8recv int8send - - - d p f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("~18 digit integer, 8-byte storage");
#define INT8OID			20

DATA(insert OID = 21 (	int2	   PGNSP PGUID	2 t b N f t \054 0	 0 1005 int2in int2out int2recv int2send - - - s p f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("-32 thousand to 32 thousand, 2-byte storage");
#define INT2OID			21

DATA(insert OID = 5545 (	int1	   PGNSP PGUID	1 t b N f t \054 0	 0 5546 int1in int1out int1recv int1send - - - c p f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("0 to 255, 1-byte storage");
#define INT1OID			5545
DATA(insert OID = 22 (	int2vector PGNSP PGUID -1 f b A f t \054 0	21 1006 int2vectorin int2vectorout int2vectorrecv int2vectorsend - - - i p f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("array of int2, used in system tables");
#define INT2VECTOROID	22

DATA(insert OID = 23 (	int4	   PGNSP PGUID	4 t b N f t \054 0	 0 1007 int4in int4out int4recv int4send - - - i p f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("-2 billion to 2 billion integer, 4-byte storage");
#define INT4OID			23

DATA(insert OID = 24 (	regproc    PGNSP PGUID	4 t b N f t \054 0	 0 1008 regprocin regprocout regprocrecv regprocsend - - - i p f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("registered procedure");
#define REGPROCOID		24

DATA(insert OID = 25 (	text	   PGNSP PGUID -1 f b S t t \054 0	0 1009 textin textout textrecv textsend - - - i x f 0 -1 0 100 _null_ _null_ _null_ ));
DESCR("variable-length string, no limit specified");
#define TEXTOID			25

DATA(insert OID = 26 (	oid		   PGNSP PGUID	4 t b N t t \054 0	 0 1028 oidin oidout oidrecv oidsend - - - i p f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("object identifier(oid), maximum 4 billion");
#define OIDOID			26

DATA(insert OID = 27 (	tid		   PGNSP PGUID	6 f b U f t \054 0	 0 1010 tidin tidout tidrecv tidsend - - - s p f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("(block, offset), physical location of tuple");
#define TIDOID		27
#define TID_TYPE_LEN		6

DATA(insert OID = 28 (	xid		   PGNSP PGUID	8 FLOAT8PASSBYVAL b U f t \054 0	 0 1011 xidin xidout xidrecv xidsend - - - d p f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("transaction id");
#define XIDOID 28

DATA(insert OID = 31 (	xid32	PGNSP PGUID	 4 t b U f t \054 0	 0 1029 xidin4 xidout4 xidrecv4 xidsend4 - - - i p f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("short transaction id");
#define SHORTXIDOID 31

DATA(insert OID = 29 (	cid		   PGNSP PGUID	4 t b U f t \054 0	 0 1012 cidin cidout cidrecv cidsend - - - i p f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("command identifier type, sequence in transaction id");
#define CIDOID 29

DATA(insert OID = 30 (	oidvector  PGNSP PGUID -1 f b A f t \054 0	26 1013 oidvectorin oidvectorout oidvectorrecv oidvectorsend - - - i p f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("array of oids, used in system tables");
#define OIDVECTOROID	30

DATA(insert OID = 32 (	oidvector_extend  PGNSP PGUID -1 f b A f t \054 0	26 1013 oidvectorin_extend oidvectorout_extend oidvectorrecv_extend oidvectorsend_extend - - - i x f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("array of oids, used in system tables and support toast storage");
#define OIDVECTOREXTENDOID	32

DATA(insert OID = 33 (	int2vector_extend PGNSP PGUID -1 f b A f t \054 0	21 1004 int2vectorin_extend int2vectorout_extend int2vectorrecv_extend int2vectorsend_extend - - - i x f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("array of int2, used in system tables and support toast storage");
#define INT2VECTOREXTENDOID	33

DATA(insert OID = 34 (	int16	   PGNSP PGUID	16 f b N f t \054 0	 0 1234 int16in int16out int16recv int16send - - - d p f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("~38 digit integer, 16-byte storage");
#define INT16OID			34

DATA(insert OID = 86 (	raw		PGNSP PGUID -1 f b U f t \054 0	0  87 rawin rawout rawrecv rawsend - - - i x f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("variable-length string, binary values escaped");
#define RAWOID  86
DATA(insert OID = 87 (	_raw		PGNSP PGUID -1 f b A f t \054 0 86 0 array_in array_out array_recv array_send - - array_typanalyze i x f 0 -1 0 0 _null_ _null_ _null_ ));

DATA(insert OID = 88 ( blob	   PGNSP PGUID -1 f b U f t \054 0	0  3201 rawin rawout bytearecv byteasend - - - i x f 0 -1 0 0 _null_ _null_ _null_ ));
#define BLOBOID  88
DATA(insert OID = 3201 ( _blob    PGNSP PGUID -1 f b A f t \054 0 88 0 array_in array_out array_recv array_send - - array_typanalyze i x f 0 -1 0 0 _null_ _null_ _null_ ));

DATA(insert OID = 90 ( clob	   PGNSP PGUID -1 f b S t t \054 0	0  3202 textin textout textrecv textsend - - - i x f 0 -1 0 100 _null_ _null_ _null_ ));
#define CLOBOID  90
DATA(insert OID = 3202 ( _clob    PGNSP PGUID -1 f b A f t \054 0 90 0 array_in array_out array_recv array_send - - array_typanalyze i x f 0 -1 0 100 _null_ _null_ _null_ ));

/* hand-built rowtype entries for bootstrapped catalogs */
/* NB: OIDs assigned here must match the BKI_ROWTYPE_OID declarations */

DATA(insert OID = 71 (	pg_type			PGNSP PGUID -1 f c C f t \054 1247 0 0 record_in record_out record_recv record_send - - - d x f 0 -1 0 0 _null_ _null_ _null_ ));
DATA(insert OID = 75 (	pg_attribute	PGNSP PGUID -1 f c C f t \054 1249 0 0 record_in record_out record_recv record_send - - - d x f 0 -1 0 0 _null_ _null_ _null_ ));
DATA(insert OID = 81 (	pg_proc			PGNSP PGUID -1 f c C f t \054 1255 0 0 record_in record_out record_recv record_send - - - d x f 0 -1 0 0 _null_ _null_ _null_ ));
DATA(insert OID = 83 (	pg_class		PGNSP PGUID -1 f c C f t \054 1259 0 0 record_in record_out record_recv record_send - - - d x f 0 -1 0 0 _null_ _null_ _null_ ));
DATA(insert OID = 9745 (	gs_package			PGNSP PGUID -1 f c C f t \054 7815 0 0 record_in record_out record_recv record_send - - - d x f 0 -1 0 0 _null_ _null_ _null_ ));

/* OIDS 100 - 199 */
DATA(insert OID = 114 ( json		   PGNSP PGUID -1 f b U f t \054 0 0 199 json_in json_out json_recv json_send - - - i x f 0 -1 0 0 _null_ _null_ _null_ ));
#define JSONOID 114
DATA(insert OID = 142 ( xml		   PGNSP PGUID -1 f b U f t \054 0 0 143 xml_in xml_out xml_recv xml_send - - - i x f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("XML content");
#define XMLOID 142
DATA(insert OID = 143 ( _xml	   PGNSP PGUID -1 f b A f t \054 0 142 0 array_in array_out array_recv array_send - - array_typanalyze i x f 0 -1 0 0 _null_ _null_ _null_ ));
DATA(insert OID = 199 ( _json	   PGNSP PGUID -1 f b A f t \054 0 114 0 array_in array_out array_recv array_send - - array_typanalyze i x f 0 -1 0 0 _null_ _null_ _null_ ));

DATA(insert OID = 194 ( pg_node_tree	PGNSP PGUID -1 f b S f t \054 0 0 0 pg_node_tree_in pg_node_tree_out pg_node_tree_recv pg_node_tree_send - - - i x f 0 -1 0 100 _null_ _null_ _null_ ));
DESCR("string representing an internal node tree");
#define PGNODETREEOID	194

/* OIDS 200 - 299 */

DATA(insert OID = 210 (  smgr	   PGNSP PGUID 2 t b U f t \054 0 0 0 smgrin smgrout - - - - - s p f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("storage manager");
#define SMGROID  210

/* OIDS 300 - 399 */

/* OIDS 400 - 499 */

/* OIDS 500 - 599 */

/* OIDS 600 - 699 */
DATA(insert OID = 600 (  point	   PGNSP PGUID 16 f b G f t \054 0 701 1017 point_in point_out point_recv point_send - - - d p f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("geometric point '(x, y)'");
#define POINTOID		600
DATA(insert OID = 601 (  lseg	   PGNSP PGUID 32 f b G f t \054 0 600 1018 lseg_in lseg_out lseg_recv lseg_send - - - d p f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("geometric line segment '(pt1,pt2)'");
#define LSEGOID			601
DATA(insert OID = 602 (  path	   PGNSP PGUID -1 f b G f t \054 0 0 1019 path_in path_out path_recv path_send - - - d x f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("geometric path '(pt1,...)'");
#define PATHOID			602
DATA(insert OID = 603 (  box	   PGNSP PGUID 32 f b G f t \073 0 600 1020 box_in box_out box_recv box_send - - - d p f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("geometric box '(lower left,upper right)'");
#define BOXOID			603
DATA(insert OID = 604 (  polygon   PGNSP PGUID -1 f b G f t \054 0	 0 1027 poly_in poly_out poly_recv poly_send - - - d x f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("geometric polygon '(pt1,...)'");
#define POLYGONOID		604

DATA(insert OID = 628 (  line	   PGNSP PGUID 32 f b G f t \054 0 701 629 line_in line_out line_recv line_send - - - d p f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("geometric line (not implemented)");
#define LINEOID			628
DATA(insert OID = 629 (  _line	   PGNSP PGUID	-1 f b A f t \054 0 628 0 array_in array_out array_recv array_send - - array_typanalyze d x f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("");

/* OIDS 700 - 799 */

DATA(insert OID = 700 (  float4    PGNSP PGUID	4 FLOAT4PASSBYVAL b N f t \054 0	 0 1021 float4in float4out float4recv float4send - - - i p f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("single-precision floating point number, 4-byte storage");
#define FLOAT4OID 700
DATA(insert OID = 701 (  float8    PGNSP PGUID	8 FLOAT8PASSBYVAL b N t t \054 0	 0 1022 float8in float8out float8recv float8send - - - d p f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("double-precision floating point number, 8-byte storage");
#define FLOAT8OID 701
DATA(insert OID = 702 (  abstime   PGNSP PGUID	4 t b D f t \054 0	 0 1023 abstimein abstimeout abstimerecv abstimesend - - - i p f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("absolute, limited-range date and time (Unix system time)");
#define ABSTIMEOID		702
DATA(insert OID = 703 (  reltime   PGNSP PGUID	4 t b T f t \054 0	 0 1024 reltimein reltimeout reltimerecv reltimesend - - - i p f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("relative, limited-range time interval (Unix delta time)");
#define RELTIMEOID		703
DATA(insert OID = 704 (  tinterval PGNSP PGUID 12 f b T f t \054 0	 0 1025 tintervalin tintervalout tintervalrecv tintervalsend - - - i p f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("(abstime,abstime), time interval");
#define TINTERVALOID	704
DATA(insert OID = 705 (  unknown   PGNSP PGUID -2 f b X f t \054 0	 0 0 unknownin unknownout unknownrecv unknownsend - - - c p f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("");
#define UNKNOWNOID		705

DATA(insert OID = 718 (  circle    PGNSP PGUID	24 f b G f t \054 0 0 719 circle_in circle_out circle_recv circle_send - - - d p f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("geometric circle '(center,radius)'");
#define CIRCLEOID		718
DATA(insert OID = 719 (  _circle   PGNSP PGUID	-1 f b A f t \054 0  718 0 array_in array_out array_recv array_send - - array_typanalyze d x f 0 -1 0 0 _null_ _null_ _null_ ));
DATA(insert OID = 790 (  money	   PGNSP PGUID	 8 FLOAT8PASSBYVAL b N f t \054 0 0 791 cash_in cash_out cash_recv cash_send - - - d p f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("monetary amounts, $d,ddd.cc");
#define CASHOID 790
DATA(insert OID = 791 (  _money    PGNSP PGUID	-1 f b A f t \054 0  790 0 array_in array_out array_recv array_send - - array_typanalyze d x f 0 -1 0 0 _null_ _null_ _null_ ));
#define CASHARRAYOID 791

/* OIDS 800 - 899 */
DATA(insert OID = 829 ( macaddr    PGNSP PGUID	6 f b U f t \054 0 0 1040 macaddr_in macaddr_out macaddr_recv macaddr_send - - - i p f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("XX:XX:XX:XX:XX:XX, MAC address");
#define MACADDROID 829
DATA(insert OID = 869 ( inet	   PGNSP PGUID	-1 f b I t t \054 0 0 1041 inet_in inet_out inet_recv inet_send - - - i m f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("IP address/netmask, host address, netmask optional");
#define INETOID 869
DATA(insert OID = 650 ( cidr	   PGNSP PGUID	-1 f b I f t \054 0 0 651 cidr_in cidr_out cidr_recv cidr_send - - - i m f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("network IP address/netmask, network address");
#define CIDROID 650

/* OIDS 900 - 999 */

/* OIDS 1000 - 1099 */
DATA(insert OID = 1000 (  _bool		 PGNSP PGUID -1 f b A f t \054 0	16 0 array_in array_out array_recv array_send - - array_typanalyze i x f 0 -1 0 0 _null_ _null_ _null_ ));
#define BOOLARRAYOID 1000
DATA(insert OID = 1001 (  _bytea	 PGNSP PGUID -1 f b A f t \054 0	17 0 array_in array_out array_recv array_send - - array_typanalyze i x f 0 -1 0 0 _null_ _null_ _null_ ));
#define BYTEARRAYOID 1001
DATA(insert OID = 1002 (  _char		 PGNSP PGUID -1 f b A f t \054 0	18 0 array_in array_out array_recv array_send - - array_typanalyze i x f 0 -1 0 0 _null_ _null_ _null_ ));
#define CHARARRAYOID 1002
DATA(insert OID = 1003 (  _name		 PGNSP PGUID -1 f b A f t \054 0	19 0 array_in array_out array_recv array_send - - array_typanalyze i x f 0 -1 0 0 _null_ _null_ _null_ ));
#define NAMEARRAYOID 1003
DATA(insert OID = 1004 (  _int2vector_extend PGNSP PGUID -1 f b A f t \054 0	33 0 array_in array_out array_recv array_send - - array_typanalyze i x f 0 -1 0 0 _null_ _null_ _null_ ));
DATA(insert OID = 1005 (  _int2		 PGNSP PGUID -1 f b A f t \054 0	21 0 array_in array_out array_recv array_send - - array_typanalyze i x f 0 -1 0 0 _null_ _null_ _null_ ));
#define INT2ARRAYOID 1005
DATA(insert OID = 5546 (  _int1		 PGNSP PGUID -1 f b A f t \054 0	5545 0 array_in array_out array_recv array_send - - array_typanalyze i x f 0 -1 0 0 _null_ _null_ _null_ ));
#define INT1ARRAYOID 5546
DATA(insert OID = 1006 (  _int2vector PGNSP PGUID -1 f b A f t \054 0	22 0 array_in array_out array_recv array_send - - array_typanalyze i x f 0 -1 0 0 _null_ _null_ _null_ ));
DATA(insert OID = 1007 (  _int4		 PGNSP PGUID -1 f b A f t \054 0	23 0 array_in array_out array_recv array_send - - array_typanalyze i x f 0 -1 0 0 _null_ _null_ _null_ ));
#define INT4ARRAYOID 1007
DATA(insert OID = 1008 (  _regproc	 PGNSP PGUID -1 f b A f t \054 0	24 0 array_in array_out array_recv array_send - - array_typanalyze i x f 0 -1 0 0 _null_ _null_ _null_ ));
DATA(insert OID = 1009 (  _text		 PGNSP PGUID -1 f b A f t \054 0	25 0 array_in array_out array_recv array_send - - array_typanalyze i x f 0 -1 0 100 _null_ _null_ _null_ ));
#define TEXTARRAYOID 1009
DATA(insert OID = 1028 (  _oid		 PGNSP PGUID -1 f b A f t \054 0	26 0 array_in array_out array_recv array_send - - array_typanalyze i x f 0 -1 0 0 _null_ _null_ _null_ ));
DATA(insert OID = 1010 (  _tid		 PGNSP PGUID -1 f b A f t \054 0	27 0 array_in array_out array_recv array_send - - array_typanalyze i x f 0 -1 0 0 _null_ _null_ _null_ ));
DATA(insert OID = 1011 (  _xid		 PGNSP PGUID -1 f b A f t \054 0	28 0 array_in array_out array_recv array_send - - array_typanalyze d x f 0 -1 0 0 _null_ _null_ _null_ ));
DATA(insert OID = 1029 (  _xid32	 PGNSP PGUID -1 f b A f t \054 0	31 0 array_in array_out array_recv array_send - - array_typanalyze i x f 0 -1 0 0 _null_ _null_ _null_ ));
DATA(insert OID = 1012 (  _cid		 PGNSP PGUID -1 f b A f t \054 0	29 0 array_in array_out array_recv array_send - - array_typanalyze i x f 0 -1 0 0 _null_ _null_ _null_ ));
DATA(insert OID = 1013 (  _oidvector PGNSP PGUID -1 f b A f t \054 0	30 0 array_in array_out array_recv array_send - - array_typanalyze i x f 0 -1 0 0 _null_ _null_ _null_ ));
DATA(insert OID = 1014 (  _bpchar	 PGNSP PGUID -1 f b A f t \054 0 1042 0 array_in array_out array_recv array_send bpchartypmodin bpchartypmodout array_typanalyze i x f 0 -1 0 100 _null_ _null_ _null_ ));
#define BPCHARARRAYOID  1014
DATA(insert OID = 1015 (  _varchar	 PGNSP PGUID -1 f b A f t \054 0 1043 0 array_in array_out array_recv array_send varchartypmodin varchartypmodout array_typanalyze i x f 0 -1 0 100 _null_ _null_ _null_ ));
#define VARCHARARRAYOID	1015
DATA(insert OID = 1016 (  _int8		 PGNSP PGUID -1 f b A f t \054 0	20 0 array_in array_out array_recv array_send - - array_typanalyze d x f 0 -1 0 0 _null_ _null_ _null_ ));
#define INT8ARRAYOID 1016
DATA(insert OID = 1017 (  _point	 PGNSP PGUID -1 f b A f t \054 0 600 0 array_in array_out array_recv array_send - - array_typanalyze d x f 0 -1 0 0 _null_ _null_ _null_ ));
DATA(insert OID = 1018 (  _lseg		 PGNSP PGUID -1 f b A f t \054 0 601 0 array_in array_out array_recv array_send - - array_typanalyze d x f 0 -1 0 0 _null_ _null_ _null_ ));
DATA(insert OID = 1019 (  _path		 PGNSP PGUID -1 f b A f t \054 0 602 0 array_in array_out array_recv array_send - - array_typanalyze d x f 0 -1 0 0 _null_ _null_ _null_ ));
DATA(insert OID = 1020 (  _box		 PGNSP PGUID -1 f b A f t \073 0 603 0 array_in array_out array_recv array_send - - array_typanalyze d x f 0 -1 0 0 _null_ _null_ _null_ ));
DATA(insert OID = 1021 (  _float4	 PGNSP PGUID -1 f b A f t \054 0 700 0 array_in array_out array_recv array_send - - array_typanalyze i x f 0 -1 0 0 _null_ _null_ _null_ ));
#define FLOAT4ARRAYOID 1021
DATA(insert OID = 1022 (  _float8	 PGNSP PGUID -1 f b A f t \054 0 701 0 array_in array_out array_recv array_send - - array_typanalyze d x f 0 -1 0 0 _null_ _null_ _null_ ));
#define FLOAT8ARRAYOID 1022
DATA(insert OID = 1023 (  _abstime	 PGNSP PGUID -1 f b A f t \054 0 702 0 array_in array_out array_recv array_send - - array_typanalyze i x f 0 -1 0 0 _null_ _null_ _null_ ));
#define ABSTIMEARRAYOID 1023
DATA(insert OID = 1024 (  _reltime	 PGNSP PGUID -1 f b A f t \054 0 703 0 array_in array_out array_recv array_send - - array_typanalyze i x f 0 -1 0 0 _null_ _null_ _null_ ));
#define RELTIMEARRAYOID 1024
DATA(insert OID = 1025 (  _tinterval PGNSP PGUID -1 f b A f t \054 0 704 0 array_in array_out array_recv array_send - - array_typanalyze i x f 0 -1 0 0 _null_ _null_ _null_ ));
#define ARRAYTINTERVALOID 1025
DATA(insert OID = 1027 (  _polygon	 PGNSP PGUID -1 f b A f t \054 0 604 0 array_in array_out array_recv array_send - - array_typanalyze d x f 0 -1 0 0 _null_ _null_ _null_ ));
DATA(insert OID = 1033 (  aclitem	 PGNSP PGUID 12 f b U f t \054 0 0 1034 aclitemin aclitemout - - - - - i p f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("access control list");
#define ACLITEMOID		1033
DATA(insert OID = 1034 (  _aclitem	 PGNSP PGUID -1 f b A f t \054 0 1033 0 array_in array_out array_recv array_send - - array_typanalyze i x f 0 -1 0 0 _null_ _null_ _null_ ));
#define ACLITEMARRAYOID		1034
DATA(insert OID = 1040 (  _macaddr	 PGNSP PGUID -1 f b A f t \054 0  829 0 array_in array_out array_recv array_send - - array_typanalyze i x f 0 -1 0 0 _null_ _null_ _null_ ));
DATA(insert OID = 1041 (  _inet		 PGNSP PGUID -1 f b A f t \054 0  869 0 array_in array_out array_recv array_send - - array_typanalyze i x f 0 -1 0 0 _null_ _null_ _null_ ));
#define INETARRAYOID    1041
DATA(insert OID = 651  (  _cidr		 PGNSP PGUID -1 f b A f t \054 0  650 0 array_in array_out array_recv array_send - - array_typanalyze i x f 0 -1 0 0 _null_ _null_ _null_ ));
#define CIDRARRAYOID    651
DATA(insert OID = 1263 (  _cstring	 PGNSP PGUID -1 f b A f t \054 0 2275 0 array_in array_out array_recv array_send - - array_typanalyze i x f 0 -1 0 0 _null_ _null_ _null_ ));
#define CSTRINGARRAYOID		1263

DATA(insert OID = 1042 ( bpchar		 PGNSP PGUID -1 f b S f t \054 0	0 1014 bpcharin bpcharout bpcharrecv bpcharsend bpchartypmodin bpchartypmodout - i x f 0 -1 0 100 _null_ _null_ _null_ ));
DESCR("char(length), blank-padded string, fixed storage length");
#define BPCHAROID		1042
DATA(insert OID = 1043 ( varchar	 PGNSP PGUID -1 f b S f t \054 0	0 1015 varcharin varcharout varcharrecv varcharsend varchartypmodin varchartypmodout - i x f 0 -1 0 100 _null_ _null_ _null_ ));
DESCR("varchar(length), non-blank-padded string, variable storage length");
#define VARCHAROID		1043

DATA(insert OID = 3969 ( nvarchar2	 PGNSP PGUID -1 f b S f t \054 0	0 3968 nvarchar2in nvarchar2out nvarchar2recv nvarchar2send nvarchar2typmodin nvarchar2typmodout - i x f 0 -1 0 100 _null_ _null_ _null_ ));
DESCR("nvarchar2(length), non-blank-padded string, variable storage length");
#define NVARCHAR2OID		3969
DATA(insert OID = 3968 ( _nvarchar2	 PGNSP PGUID -1 f b A f t \054 0 3969 0 array_in array_out array_recv array_send nvarchar2typmodin nvarchar2typmodout array_typanalyze i x f 0 -1 0 100 _null_ _null_ _null_ ));
#define NVARCHAR2ARRAYOID   3968

DATA(insert OID = 1082 ( date		 PGNSP PGUID	4 t b D f t \054 0	0 1182 date_in date_out date_recv date_send - - - i p f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("date");
#define DATEOID			1082
DATA(insert OID = 1083 ( time		 PGNSP PGUID	8 FLOAT8PASSBYVAL b D f t \054 0	0 1183 time_in time_out time_recv time_send timetypmodin timetypmodout - d p f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("time of day");
#define TIMEOID			1083

/* OIDS 1100 - 1199 */
DATA(insert OID = 1114 ( timestamp	 PGNSP PGUID	8 FLOAT8PASSBYVAL b D f t \054 0	0 1115 timestamp_in timestamp_out timestamp_recv timestamp_send timestamptypmodin timestamptypmodout - d p f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("date and time");
#define TIMESTAMPOID	1114
DATA(insert OID = 1115 ( _timestamp  PGNSP PGUID	-1 f b A f t \054 0 1114 0 array_in array_out array_recv array_send timestamptypmodin timestamptypmodout array_typanalyze d x f 0 -1 0 0 _null_ _null_ _null_ ));
#define TIMESTAMPARRAYOID 1115
DATA(insert OID = 1182 ( _date		 PGNSP PGUID	-1 f b A f t \054 0 1082 0 array_in array_out array_recv array_send - - array_typanalyze i x f 0 -1 0 0 _null_ _null_ _null_ ));
#define DATEARRAYOID 1182
DATA(insert OID = 1183 ( _time		 PGNSP PGUID	-1 f b A f t \054 0 1083 0 array_in array_out array_recv array_send timetypmodin timetypmodout array_typanalyze d x f 0 -1 0 0 _null_ _null_ _null_ ));
#define TIMEARRAYOID 1183
DATA(insert OID = 1184 ( timestamptz PGNSP PGUID	8 FLOAT8PASSBYVAL b D t t \054 0	0 1185 timestamptz_in timestamptz_out timestamptz_recv timestamptz_send timestamptztypmodin timestamptztypmodout - d p f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("date and time with time zone");
#define TIMESTAMPTZOID	1184
DATA(insert OID = 1185 ( _timestamptz PGNSP PGUID -1 f b A f t \054 0	1184 0 array_in array_out array_recv array_send timestamptztypmodin timestamptztypmodout array_typanalyze d x f 0 -1 0 0 _null_ _null_ _null_ ));
#define TIMESTAMPTZARRAYOID 1185
DATA(insert OID = 1186 ( interval	 PGNSP PGUID 16 f b T t t \054 0	0 1187 interval_in interval_out interval_recv interval_send intervaltypmodin intervaltypmodout - d p f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("@ <number> <units>, time interval");
#define INTERVALOID		1186
DATA(insert OID = 1187 ( _interval	 PGNSP PGUID	-1 f b A f t \054 0 1186 0 array_in array_out array_recv array_send intervaltypmodin intervaltypmodout array_typanalyze d x f 0 -1 0 0 _null_ _null_ _null_ ));
#define ARRAYINTERVALOID	1187
/* OIDS 1200 - 1299 */
DATA(insert OID = 1231 (  _numeric	 PGNSP PGUID -1 f b A f t \054 0	1700 0 array_in array_out array_recv array_send numerictypmodin numerictypmodout array_typanalyze i x f 0 -1 0 0 _null_ _null_ _null_ ));
#define ARRAYNUMERICOID 1231
DATA(insert OID = 1234 (  _int16	 PGNSP PGUID -1 f b A f t \054 0	34 0 array_in array_out array_recv array_send - - array_typanalyze d x f 0 -1 0 0 _null_ _null_ _null_ ));


DATA(insert OID = 1266 ( timetz		 PGNSP PGUID 12 f b D f t \054 0	0 1270 timetz_in timetz_out timetz_recv timetz_send timetztypmodin timetztypmodout - d p f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("time of day with time zone");
#define TIMETZOID		1266
DATA(insert OID = 1270 ( _timetz	 PGNSP PGUID -1 f b A f t \054 0	1266 0 array_in array_out array_recv array_send timetztypmodin timetztypmodout array_typanalyze d x f 0 -1 0 0 _null_ _null_ _null_ ));
#define ARRAYTIMETZOID	1270

/* OIDS 1500 - 1599 */
DATA(insert OID = 1560 ( bit		 PGNSP PGUID -1 f b V f t \054 0	0 1561 bit_in bit_out bit_recv bit_send bittypmodin bittypmodout - i x f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("fixed-length bit string");
#define BITOID	 1560
DATA(insert OID = 1561 ( _bit		 PGNSP PGUID -1 f b A f t \054 0	1560 0 array_in array_out array_recv array_send bittypmodin bittypmodout array_typanalyze i x f 0 -1 0 0 _null_ _null_ _null_ ));
#define BITARRAYOID     1561
DATA(insert OID = 1562 ( varbit		 PGNSP PGUID -1 f b V t t \054 0	0 1563 varbit_in varbit_out varbit_recv varbit_send varbittypmodin varbittypmodout - i x f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("variable-length bit string");
#define VARBITOID	  1562
DATA(insert OID = 1563 ( _varbit	 PGNSP PGUID -1 f b A f t \054 0	1562 0 array_in array_out array_recv array_send varbittypmodin varbittypmodout array_typanalyze i x f 0 -1 0 0 _null_ _null_ _null_ ));
#define VARBITARRAYOID  1563

/* OIDS 1600 - 1699 */

/* OIDS 1700 - 1799 */
DATA(insert OID = 1700 ( numeric	   PGNSP PGUID -1 f b N f t \054 0	0 1231 numeric_in numeric_out numeric_recv numeric_send numerictypmodin numerictypmodout - i m f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("numeric(precision, decimal), arbitrary precision number");
#define NUMERICOID		1700

DATA(insert OID = 1790 ( refcursor	   PGNSP PGUID -1 f b U f t \054 0	0 2201 textin textout textrecv textsend - - - i x f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("reference to cursor (portal name)");
#define REFCURSOROID	1790

/* OIDS 2200 - 2299 */
DATA(insert OID = 2201 ( _refcursor    PGNSP PGUID -1 f b A f t \054 0 1790 0 array_in array_out array_recv array_send - - array_typanalyze i x f 0 -1 0 0 _null_ _null_ _null_ ));

DATA(insert OID = 2202 ( regprocedure  PGNSP PGUID	4 t b N f t \054 0	 0 2207 regprocedurein regprocedureout regprocedurerecv regproceduresend - - - i p f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("registered procedure (with args)");
#define REGPROCEDUREOID 2202

DATA(insert OID = 2203 ( regoper	   PGNSP PGUID	4 t b N f t \054 0	 0 2208 regoperin regoperout regoperrecv regopersend - - - i p f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("registered operator");
#define REGOPEROID		2203

DATA(insert OID = 2204 ( regoperator   PGNSP PGUID	4 t b N f t \054 0	 0 2209 regoperatorin regoperatorout regoperatorrecv regoperatorsend - - - i p f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("registered operator (with args)");
#define REGOPERATOROID	2204

DATA(insert OID = 2205 ( regclass	   PGNSP PGUID	4 t b N f t \054 0	 0 2210 regclassin regclassout regclassrecv regclasssend - - - i p f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("registered class");
#define REGCLASSOID		2205

DATA(insert OID = 2206 ( regtype	   PGNSP PGUID	4 t b N f t \054 0	 0 2211 regtypein regtypeout regtyperecv regtypesend - - - i p f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("registered type");
#define REGTYPEOID		2206

DATA(insert OID = 2207 ( _regprocedure PGNSP PGUID -1 f b A f t \054 0 2202 0 array_in array_out array_recv array_send - - array_typanalyze i x f 0 -1 0 0 _null_ _null_ _null_ ));
DATA(insert OID = 2208 ( _regoper	   PGNSP PGUID -1 f b A f t \054 0 2203 0 array_in array_out array_recv array_send - - array_typanalyze i x f 0 -1 0 0 _null_ _null_ _null_ ));
DATA(insert OID = 2209 ( _regoperator  PGNSP PGUID -1 f b A f t \054 0 2204 0 array_in array_out array_recv array_send - - array_typanalyze i x f 0 -1 0 0 _null_ _null_ _null_ ));
DATA(insert OID = 2210 ( _regclass	   PGNSP PGUID -1 f b A f t \054 0 2205 0 array_in array_out array_recv array_send - - array_typanalyze i x f 0 -1 0 0 _null_ _null_ _null_ ));
DATA(insert OID = 2211 ( _regtype	   PGNSP PGUID -1 f b A f t \054 0 2206 0 array_in array_out array_recv array_send - - array_typanalyze i x f 0 -1 0 0 _null_ _null_ _null_ ));
#define REGTYPEARRAYOID 2211

/* uuid */
DATA(insert OID = 2950 ( uuid			PGNSP PGUID 16 f b U f t \054 0 0 2951 uuid_in uuid_out uuid_recv uuid_send - - - c p f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("UUID datatype");
#define UUIDOID 2950

DATA(insert OID = 2951 ( _uuid			PGNSP PGUID -1 f b A f t \054 0 2950 0 array_in array_out array_recv array_send - - array_typanalyze i x f 0 -1 0 0 _null_ _null_ _null_ ));

/* text search */
DATA(insert OID = 3614 ( tsvector		PGNSP PGUID -1 f b U f t \054 0 0 3643 tsvectorin tsvectorout tsvectorrecv tsvectorsend - - ts_typanalyze i x f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("text representation for text search");
#define TSVECTOROID		3614
DATA(insert OID = 3642 ( gtsvector		PGNSP PGUID -1 f b U f t \054 0 0 3644 gtsvectorin gtsvectorout - - - - - i p f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("GiST index internal text representation for text search");
#define GTSVECTOROID	3642
DATA(insert OID = 3615 ( tsquery		PGNSP PGUID -1 f b U f t \054 0 0 3645 tsqueryin tsqueryout tsqueryrecv tsquerysend - - - i p f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("query representation for text search");
#define TSQUERYOID		3615
DATA(insert OID = 3734 ( regconfig		PGNSP PGUID 4 t b N f t \054 0 0 3735 regconfigin regconfigout regconfigrecv regconfigsend - - - i p f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("registered text search configuration");
#define REGCONFIGOID	3734
DATA(insert OID = 3769 ( regdictionary	PGNSP PGUID 4 t b N f t \054 0 0 3770 regdictionaryin regdictionaryout regdictionaryrecv regdictionarysend - - - i p f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("registered text search dictionary");
#define REGDICTIONARYOID	3769

DATA(insert OID = 3643 ( _tsvector		PGNSP PGUID -1 f b A f t \054 0 3614 0 array_in array_out array_recv array_send - - array_typanalyze i x f 0 -1 0 0 _null_ _null_ _null_ ));
DATA(insert OID = 3644 ( _gtsvector		PGNSP PGUID -1 f b A f t \054 0 3642 0 array_in array_out array_recv array_send - - array_typanalyze i x f 0 -1 0 0 _null_ _null_ _null_ ));
DATA(insert OID = 3645 ( _tsquery		PGNSP PGUID -1 f b A f t \054 0 3615 0 array_in array_out array_recv array_send - - array_typanalyze i x f 0 -1 0 0 _null_ _null_ _null_ ));
DATA(insert OID = 3735 ( _regconfig		PGNSP PGUID -1 f b A f t \054 0 3734 0 array_in array_out array_recv array_send - - array_typanalyze i x f 0 -1 0 0 _null_ _null_ _null_ ));
DATA(insert OID = 3770 ( _regdictionary PGNSP PGUID -1 f b A f t \054 0 3769 0 array_in array_out array_recv array_send - - array_typanalyze i x f 0 -1 0 0 _null_ _null_ _null_ ));

/* jsonb */
DATA(insert OID = 3802 ( jsonb         PGNSP PGUID -1 f b C f t \054 0 0 3807 jsonb_in jsonb_out jsonb_recv jsonb_send - - - i x f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("Binary JSON");
#define JSONBOID 3802
DATA(insert OID = 3807 ( _jsonb            PGNSP PGUID -1 f b A f t \054 0 3802 0 array_in array_out array_recv array_send - - array_typanalyze i x f 0 -1 0 0 _null_ _null_ _null_ ));

DATA(insert OID = 2970 ( txid_snapshot	PGNSP PGUID -1 f b U f t \054 0 0 2949 txid_snapshot_in txid_snapshot_out txid_snapshot_recv txid_snapshot_send - - - d x f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("txid snapshot");
DATA(insert OID = 2949 ( _txid_snapshot PGNSP PGUID -1 f b A f t \054 0 2970 0 array_in array_out array_recv array_send - - array_typanalyze d x f 0 -1 0 0 _null_ _null_ _null_ ));

/* range types */
DATA(insert OID = 3904 ( int4range		PGNSP PGUID  -1 f r R f t \054 0 0 3905 range_in range_out range_recv range_send - - range_typanalyze i x f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("range of integers");
#define INT4RANGEOID		3904
DATA(insert OID = 3905 ( _int4range		PGNSP PGUID  -1 f b A f t \054 0 3904 0 array_in array_out array_recv array_send - - array_typanalyze i x f 0 -1 0 0 _null_ _null_ _null_ ));
DATA(insert OID = 3906 ( numrange		PGNSP PGUID  -1 f r R f t \054 0 0 3907 range_in range_out range_recv range_send - - range_typanalyze i x f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("range of numerics");
#define NUMRANGEOID 3906
DATA(insert OID = 3907 ( _numrange		PGNSP PGUID  -1 f b A f t \054 0 3906 0 array_in array_out array_recv array_send - - array_typanalyze i x f 0 -1 0 0 _null_ _null_ _null_ ));
DATA(insert OID = 3908 ( tsrange		PGNSP PGUID  -1 f r R f t \054 0 0 3909 range_in range_out range_recv range_send - - range_typanalyze d x f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("range of timestamps without time zone");
#define TSRANGEOID 3908
DATA(insert OID = 3909 ( _tsrange		PGNSP PGUID  -1 f b A f t \054 0 3908 0 array_in array_out array_recv array_send - - array_typanalyze d x f 0 -1 0 0 _null_ _null_ _null_ ));
DATA(insert OID = 3910 ( tstzrange		PGNSP PGUID  -1 f r R f t \054 0 0 3911 range_in range_out range_recv range_send - - range_typanalyze d x f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("range of timestamps with time zone");
#define TSTZRANGEOID 3910
DATA(insert OID = 3911 ( _tstzrange		PGNSP PGUID  -1 f b A f t \054 0 3910 0 array_in array_out array_recv array_send - - array_typanalyze d x f 0 -1 0 0 _null_ _null_ _null_ ));
DATA(insert OID = 3912 ( daterange		PGNSP PGUID  -1 f r R f t \054 0 0 3913 range_in range_out range_recv range_send - - range_typanalyze i x f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("range of dates");
#define DATERANGEOID 3912
DATA(insert OID = 3913 ( _daterange		PGNSP PGUID  -1 f b A f t \054 0 3912 0 array_in array_out array_recv array_send - - array_typanalyze i x f 0 -1 0 0 _null_ _null_ _null_ ));
DATA(insert OID = 3926 ( int8range		PGNSP PGUID  -1 f r R f t \054 0 0 3927 range_in range_out range_recv range_send - - range_typanalyze d x f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("range of bigints");
#define INT8RANGEOID 3926
DATA(insert OID = 3927 ( _int8range		PGNSP PGUID  -1 f b A f t \054 0 3926 0 array_in array_out array_recv array_send - - array_typanalyze d x f 0 -1 0 0 _null_ _null_ _null_ ));
/*
 * pseudo-types
 *
 * types with typtype='p' represent various special cases in the type system.
 *
 * These cannot be used to define table columns, but are valid as function
 * argument and result types (if supported by the function's implementation
 * language).
 *
 * Note: cstring is a borderline case; it is still considered a pseudo-type,
 * but there is now support for it in records and arrays.  Perhaps we should
 * just treat it as a regular base type?
 */
DATA(insert OID = 2249 ( record			PGNSP PGUID -1 f p P f t \054 0 0 2287 record_in record_out record_recv record_send - - - d x f 0 -1 0 0 _null_ _null_ _null_ ));
#define RECORDOID		2249
DATA(insert OID = 2287 ( _record		PGNSP PGUID -1 f p P f t \054 0 2249 0 array_in array_out array_recv array_send - - array_typanalyze d x f 0 -1 0 0 _null_ _null_ _null_ ));
#define RECORDARRAYOID	2287
DATA(insert OID = 2275 ( cstring		PGNSP PGUID -2 f p P f t \054 0 0 1263 cstring_in cstring_out cstring_recv cstring_send - - - c p f 0 -1 0 0 _null_ _null_ _null_ ));
#define CSTRINGOID		2275
DATA(insert OID = 2276 ( any			PGNSP PGUID  4 t p P f t \054 0 0 0 any_in any_out - - - - - i p f 0 -1 0 0 _null_ _null_ _null_ ));
#define ANYOID			2276
DATA(insert OID = 2277 ( anyarray		PGNSP PGUID -1 f p P f t \054 0 0 0 anyarray_in anyarray_out anyarray_recv anyarray_send - - - d x f 0 -1 0 0 _null_ _null_ _null_ ));
#define ANYARRAYOID		2277
DATA(insert OID = 2278 ( void			PGNSP PGUID  4 t p P f t \054 0 0 0 void_in void_out void_recv void_send - - - i p f 0 -1 0 0 _null_ _null_ _null_ ));
#define VOIDOID			2278
DATA(insert OID = 2279 ( trigger		PGNSP PGUID  4 t p P f t \054 0 0 0 trigger_in trigger_out - - - - - i p f 0 -1 0 0 _null_ _null_ _null_ ));
#define TRIGGEROID		2279
DATA(insert OID = 2280 ( language_handler	PGNSP PGUID  4 t p P f t \054 0 0 0 language_handler_in language_handler_out - - - - - i p f 0 -1 0 0 _null_ _null_ _null_ ));
#define LANGUAGE_HANDLEROID		2280
DATA(insert OID = 2281 ( internal		PGNSP PGUID  SIZEOF_POINTER t p P f t \054 0 0 0 internal_in internal_out - - - - - ALIGNOF_POINTER p f 0 -1 0 0 _null_ _null_ _null_ ));
#define INTERNALOID		2281
DATA(insert OID = 2282 ( opaque			PGNSP PGUID  4 t p P f t \054 0 0 0 opaque_in opaque_out - - - - - i p f 0 -1 0 0 _null_ _null_ _null_ ));
#define OPAQUEOID		2282
DATA(insert OID = 2283 ( anyelement		PGNSP PGUID  4 t p P f t \054 0 0 0 anyelement_in anyelement_out - - - - - i p f 0 -1 0 0 _null_ _null_ _null_ ));
#define ANYELEMENTOID	2283
DATA(insert OID = 2776 ( anynonarray	PGNSP PGUID  4 t p P f t \054 0 0 0 anynonarray_in anynonarray_out - - - - - i p f 0 -1 0 0 _null_ _null_ _null_ ));
#define ANYNONARRAYOID	2776
DATA(insert OID = 3500 ( anyenum		PGNSP PGUID  4 t p P f t \054 0 0 0 anyenum_in anyenum_out - - - - - i p f 0 -1 0 0 _null_ _null_ _null_ ));
#define ANYENUMOID		3500
DATA(insert OID = 3115 ( fdw_handler	PGNSP PGUID  4 t p P f t \054 0 0 0 fdw_handler_in fdw_handler_out - - - - - i p f 0 -1 0 0 _null_ _null_ _null_ ));
#define FDW_HANDLEROID	3115
DATA(insert OID = 3831 ( anyrange		PGNSP PGUID  -1 f p P f t \054 0 0 0 anyrange_in anyrange_out - - - - - d x f 0 -1 0 0 _null_ _null_ _null_ ));
#define ANYRANGEOID		3831
DATA(insert OID = 3838 ( event_trigger     PGNSP PGUID  4 t p P f t \054 0 0 0 event_trigger_in event_trigger_out - - - - - i p f 0 -1 0 0 _null_ _null_ _null_ ));
#define EVTTRIGGEROID      3838
DATA(insert OID = 9003 ( smalldatetime   PGNSP PGUID    8 FLOAT8PASSBYVAL b D f t \054 0        0 9005 smalldatetime_in smalldatetime_out smalldatetime_recv smalldatetime_send timestamptypmodin timestamptypmodout - d p f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("date and time");
#define SMALLDATETIMEOID        9003
DATA(insert OID = 9005 ( _smalldatetime  PGNSP PGUID    -1 f b A f t \054 0 9003 0 array_in array_out array_recv array_send timestamptypmodin timestamptypmodout array_typanalyze d x f 0 -1 0 0 _null_ _null_ _null_ ));
#define SMALLDATETIMEARRAYOID   9005
DATA(insert OID = 4301 ( hll   PGNSP PGUID   -1 f b U f t \054 0  0 4302 hll_in hll_out hll_recv hll_send hll_typmod_in hll_typmod_out - i e f 0 -1 0 0 _null_ _null_ _null_ ));
#define HLL_OID	4301
DESCR("hypper log log type");
DATA(insert OID = 4302 ( _hll   PGNSP PGUID   -1 f p P f t \054 0 4301 0  array_in array_out array_recv array_send hll_typmod_in hll_typmod_out array_typanalyze i x f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("hypper log log array");
#define HLL_ARRAYOID    4302
DATA(insert OID = 4303 ( hll_hashval   PGNSP PGUID  8 t p P f t \054 0  0 4304 hll_hashval_in hll_hashval_out - - - - - d p f 0 -1 0 0 _null_ _null_ _null_ ));
#define HLL_HASHVAL_OID	4303
DESCR("hypper log log hashval");
DATA(insert OID = 4304 ( _hll_hashval   PGNSP PGUID  -1 f p P f t \054 0 4303 0 array_in array_out array_recv array_send - - array_typanalyze d x f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("hypper log log hashval array");
#define HLL_HASHVAL_ARRAYOID    4304
DATA(insert OID = 4370 ( hll_trans_type  PGNSP PGUID  -1 f p P f t \054 0  0 4371 hll_trans_in hll_trans_out hll_trans_recv hll_trans_send -  - - i e f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("hypper log log internal type");
#define HLL_TRANS_OID    4370
DATA(insert OID = 4371 ( _hll_trans_type  PGNSP PGUID  -1 f p P f t \054 0  4370 0 array_in array_out array_recv array_send - - array_typanalyze i x f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("hypper log log internal type");

DATA(insert OID = 4402 ( byteawithoutorderwithequalcol   PGNSP PGUID -1 f b U f t \054 0 0 4404 byteawithoutorderwithequalcolin byteawithoutorderwithequalcolout byteawithoutorderwithequalcolrecv byteawithoutorderwithequalcolsend byteawithoutorderwithequalcoltypmodin byteawithoutorderwithequalcoltypmodout - i x f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("encrypted data variable-length string, binary values escaped");
#define BYTEAWITHOUTORDERWITHEQUALCOLOID		4402

DATA(insert OID = 5801 ( hash16 PGNSP PGUID 8 t b U f t \054 0 0 5803 hash16in hash16out - - - - - d p f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("hash16 id");
#define HASH16OID 5801

/* uuid */
DATA(insert OID = 5802 ( hash32 PGNSP PGUID 16 f b U f t \054 0 0 5804 hash32in hash32out - - - - - c p f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("hash32 id");
#define HASH32OID 5802

DATA(insert OID = 5803 ( _hash16 PGNSP PGUID -1 f b A f t \054 0 5801 0 array_in array_out array_recv array_send - - array_typanalyze d x f 0 -1 0 0 _null_ _null_ _null_ ));
DATA(insert OID = 5804 ( _hash32 PGNSP PGUID -1 f b A f t \054 0 5802 0 array_in array_out array_recv array_send - - array_typanalyze i x f 0 -1 0 0 _null_ _null_ _null_ ));

DATA(insert OID = 4403 ( byteawithoutordercol   PGNSP PGUID -1 f b U f t \054 0 0 4405 byteawithoutordercolin byteawithoutordercolout byteawithoutordercolrecv byteawithoutordercolsend byteawithoutorderwithequalcoltypmodin byteawithoutorderwithequalcoltypmodout - i x f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("encrypted data variable-length string, binary values escaped");
#define BYTEAWITHOUTORDERCOLOID		4403

DATA(insert OID = 4404 ( _byteawithoutorderwithequalcol   PGNSP PGUID -1 f b A f t \054 0 4402 0 array_in array_out array_recv array_send byteawithoutorderwithequalcoltypmodin byteawithoutorderwithequalcoltypmodout array_typanalyze i x f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("encrypted data variable-length string, binary values escaped");
#define BYTEAWITHOUTORDERWITHEQUALCOLARRAYOID   4404

DATA(insert OID = 4405 ( _byteawithoutordercol   PGNSP PGUID -1 f b A f t \054 0 4403 0 array_in array_out array_recv array_send byteawithoutorderwithequalcoltypmodin byteawithoutorderwithequalcoltypmodout array_typanalyze i x f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("encrypted data variable-length string, binary values escaped");
#define BYTEAWITHOUTORDERCOLARRAYOID    4405

DATA(insert OID = 4406 ( TdigestData		PGNSP PGUID -1 f b U f t \054 0 0 4407 tdigest_in tdigest_out 0 0 - - - i m f 0 -1 0 0 _null_ _null_ _null_ ));
#define TDIGESTGOID		4406

DATA(insert OID = 4407 ( _TdigestData		PGNSP PGUID -1 f b A f t \054 0 4406 0 array_in array_out array_recv array_send - - array_typanalyze i x f 0 -1 0 0 _null_ _null_ _null_ ));
#define TDIGESTGARRAYOID		4407
/* pg_ddl_command type */
DATA(insert OID = 5729 ( pg_ddl_command   PGNSP PGUID    SIZEOF_POINTER t p P f t \054 0        0 0 pg_ddl_command_in pg_ddl_command_out pg_ddl_command_recv pg_ddl_command_send - - - d p f 0 -1 0 0 _null_ _null_ _null_ ));
#define PGDDLCOMMANDOID 5729
DATA(insert OID = 3272 ( anyset		PGNSP PGUID  -1 f s H t t \054 0 0 0 anyset_in anyset_out - - - - - i p f 0 -1 0 0 _null_ _null_ _null_ ));
#define ANYSETOID		3272

DATA(insert OID = 4408 ( undefined		PGNSP PGUID  -2 f u W f t \054 0 0 0 undefinedin undefinedout undefinedrecv undefinedsend - - - c p f 0 -1 0 0 _null_ _null_ _null_ ));
DESCR("undefined objects at PLSQL compilation time");
#define UNDEFINEDOID		4408

DATA(insert OID = 8305 (vector		PGNSP PGUID  -1 f b U f t \054 0 0 8308 vector_in vector_out vector_recv vector_send vector_typmod_in - - i e f 0 -1 0 0 _null_ _null_ _null_));
#define VECTOROID 8305

DATA(insert OID = 8307 (sparsevec	PGNSP PGUID  -1 f b U f t \054 0 0 8310 sparsevec_in sparsevec_out sparsevec_recv sparsevec_send sparsevec_typmod_in - - i e f 0 -1 0 0 _null_ _null_ _null_));
#define SPARSEVECTOROID 8307

DATA(insert OID = 8308 (  _vector	PGNSP PGUID -1 f b A f t \054 0	8305 0 array_in array_out array_recv array_send vector_typmod_in - array_typanalyze i x f 0 -1 0 0 _null_ _null_ _null_ ));
#define VECTORARRAYOID 8308

DATA(insert OID = 8310 (  _sparsevec	PGNSP PGUID -1 f b A f t \054 0	8307 0 array_in array_out array_recv array_send sparsevec_typmod_in - array_typanalyze i x f 0 -1 0 0 _null_ _null_ _null_ ));
#define SPARSEVECARRAYOID 8310

/*
 * macros
 */
#define  TYPTYPE_INVALID	'\0'	/* not an allowed type */
#define  TYPTYPE_BASE		'b' /* base type (ordinary scalar type) */
#define  TYPTYPE_COMPOSITE	'c' /* composite (e.g., table's rowtype) */
#define  TYPTYPE_DOMAIN		'd' /* domain over another type */
#define  TYPTYPE_ENUM		'e' /* enumerated type */
#define  TYPTYPE_PSEUDO		'p' /* pseudo-type */
#define  TYPTYPE_RANGE		'r' /* range type */
#define  TYPTYPE_TABLEOF    'o' /* table of type */
#define  TYPTYPE_SET		's' /* set type */
#define  TYPTYPE_UNDEFINE   'u' /* undefine type */

#define  TYPCATEGORY_INVALID	'\0'	/* not an allowed category */
#define  TYPCATEGORY_ARRAY		'A'
#define  TYPCATEGORY_BOOLEAN	'B'
#define  TYPCATEGORY_COMPOSITE	'C'
#define  TYPCATEGORY_DATETIME	'D'
#define  TYPCATEGORY_ENUM		'E'
#define  TYPCATEGORY_GEOMETRIC	'G'
#define  TYPCATEGORY_NETWORK	'I'		/* think INET */
#define  TYPCATEGORY_NUMERIC	'N'
#define  TYPCATEGORY_PSEUDOTYPE 'P'
#define  TYPCATEGORY_RANGE		'R'
#define  TYPCATEGORY_STRING		'S'
#define  TYPCATEGORY_TIMESPAN	'T'
#define  TYPCATEGORY_USER		'U'
#define  TYPCATEGORY_BITSTRING	'V'		/* er ... "varbit"? */
#define  TYPCATEGORY_UNKNOWN	'X'
#define  TYPCATEGORY_TABLEOF    'O'     /* table of type */
#define  TYPCATEGORY_TABLEOF_VARCHAR  'Q' /* table of type, index by varchar */
#define  TYPCATEGORY_TABLEOF_INTEGER  'F' /* table of type, index by integer */
#define  TYPCATEGORY_SET		'H' 	/* for set type */

#define  TYPALIGN_CHAR			'c' /* char alignment (i.e. unaligned) */
#define  TYPALIGN_SHORT			's' /* short alignment (typically 2 bytes) */
#define  TYPALIGN_INT			'i' /* int alignment (typically 4 bytes) */
#define  TYPALIGN_DOUBLE		'd' /* double alignment (often 8 bytes) */

/* Is a type OID a polymorphic pseudotype?	(Beware of multiple evaluation) */
#define IsPolymorphicType(typid)  \
	((typid) == ANYELEMENTOID || \
	 (typid) == ANYARRAYOID || \
	 (typid) == ANYNONARRAYOID || \
	 (typid) == ANYENUMOID || \
	 (typid) == ANYRANGEOID)
#define IsClientLogicType(typid) \
((typid) == BYTEAWITHOUTORDERCOLOID || \
(typid) == BYTEAWITHOUTORDERWITHEQUALCOLOID)

#define IsSupportCharsetType(typid) \
	(((typid) == TEXTOID) || \
	 ((typid) == VARCHAROID) || \
	 ((typid) == BPCHAROID) || \
	 ((typid) == NVARCHAR2OID) || \
	 (type_is_set(typid)))

#endif   /* PG_TYPE_H */
