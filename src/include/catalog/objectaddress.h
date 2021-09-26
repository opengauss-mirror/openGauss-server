/* -------------------------------------------------------------------------
 *
 * objectaddress.h
 *	  functions for working with object addresses
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/objectaddress.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef OBJECTADDRESS_H
#define OBJECTADDRESS_H

#include "nodes/parsenodes.h"
#include "storage/lock/lock.h"
#include "utils/relcache.h"

typedef enum TrDropMode {
    RB_DROP_MODE_INVALID = 0,
    RB_DROP_MODE_LOGIC = 1,
    RB_DROP_MODE_PHYSICAL = 2,
} TrDropMode;

#define TrDropModeIsAlreadySet(obj) ((obj)->rbDropMode != RB_DROP_MODE_INVALID)
#define TrObjIsEqual(objA, objB) ((objA)->classId == (objB)->classId && (objA)->objectId == (objB)->objectId)
#define TrObjIsEqualEx(_classId, _objectId, objB) ((_classId) == (objB)->classId && (_objectId) == (objB)->objectId)

/*
 * An ObjectAddress represents a database object of any type.
 */
typedef struct ObjectAddress
{
	Oid			classId;		/* Class Id from pg_class */
	Oid			objectId;		/* OID of the object */
	int32		objectSubId;	/* Subitem within object (eg column), or 0 */

    /* Used to tag Drop-Mode in recyclebin-based timecapsule. */
    TrDropMode  rbDropMode;     /* logic drop or physical drop */
    char        deptype;        /* Indicates the deptype that the object is referenced by other object. */
} ObjectAddress;

extern ObjectAddress get_object_address(ObjectType objtype, List *objname,
				   List *objargs, Relation *relp,
				   LOCKMODE lockmode, bool missing_ok);

extern void check_object_ownership(Oid roleid,
					   ObjectType objtype, ObjectAddress address,
					   List *objname, List *objargs, Relation relation);

extern Oid	get_object_namespace(const ObjectAddress *address);

#endif   /* PARSE_OBJECT_H */
