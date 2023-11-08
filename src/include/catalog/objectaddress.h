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
#include "utils/acl.h"
typedef enum TrDropMode {
    RB_DROP_MODE_INVALID = 0,
    RB_DROP_MODE_LOGIC = 1,
    RB_DROP_MODE_PHYSICAL = 2,
} TrDropMode;

typedef struct {
    Oid class_oid;               /* oid of catalog */
    Oid oid_index_oid;           /* oid of index on system oid column */
    int oid_catcache_id;         /* id of catcache on system oid column	*/
    int         name_catcache_id;   /* id of catcache on (name,namespace), or
                                     * (name) if the object does not live in a
                                     * namespace */
    AttrNumber  attnum_name;    /* attnum of name field */
    AttrNumber  attnum_namespace;   /* attnum of namespace field */
    AttrNumber  attnum_owner;   /* attnum of owner field */
    AttrNumber  attnum_acl;     /* attnum of acl field */
    ObjectType  objtype;        /* OBJECT_* of this object type */
    bool        is_nsp_name_unique; /* can the nsp/name combination (or name
                                     * alone, if there's no namespace) be
                                     * considered a unique identifier for an
                                     * object of this class? */
    
} ObjectPropertyType;

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

#define ObjectAddressSubSet(addr, class_id, object_id, object_sub_id) \
	do { \
		(addr).classId = (class_id); \
		(addr).objectId = (object_id); \
		(addr).objectSubId = (object_sub_id); \
	} while (0)

#define ObjectAddressSet(addr, class_id, object_id) \
	ObjectAddressSubSet(addr, class_id, object_id, 0)

 extern const ObjectAddress InvalidObjectAddress;
 
#define ObjectAddressSubSet(addr, class_id, object_id, object_sub_id) \
    do { \
        (addr).classId = (class_id); \
        (addr).objectId = (object_id); \
        (addr).objectSubId = (object_sub_id); \
    } while (0)
 
#define ObjectAddressSet(addr, class_id, object_id) \
    ObjectAddressSubSet(addr, class_id, object_id, 0)

extern ObjectAddress get_object_address(ObjectType objtype, List *objname,
				   List *objargs, Relation *relp,
				   LOCKMODE lockmode, bool missing_ok);

extern void check_object_ownership(Oid roleid,
					   ObjectType objtype, ObjectAddress address,
					   List *objname, List *objargs, Relation relation);

extern int get_object_catcache_oid(Oid class_id); 
extern char *getObjectTypeDescription(const ObjectAddress *object);
extern Oid  get_object_oid_index(Oid class_id);
extern int  get_object_catcache_oid(Oid class_id);
extern int  get_object_catcache_name(Oid class_id);
extern AttrNumber get_object_attnum_name(Oid class_id);
extern AttrNumber get_object_attnum_namespace(Oid class_id);
extern AttrNumber get_object_attnum_owner(Oid class_id);
extern AttrNumber get_object_attnum_acl(Oid class_id);
extern AclObjectKind get_object_aclkind(Oid class_id);
extern ObjectType get_object_type(Oid class_id, Oid object_id);
extern bool get_object_namensp_unique(Oid class_id);

extern ObjectType get_relkind_objtype(char relkind);
extern Oid get_object_namespace(const ObjectAddress *address);
extern char *getObjectTypeDescription(const ObjectAddress *object);
extern bool is_objectclass_supported(Oid class_id);
extern HeapTuple       get_catalog_object_by_oid(Relation catalog,
                         Oid objectId);
extern HeapTuple get_catalog_object_by_oid(Relation catalog, AttrNumber oidcol, Oid objectId);
extern char *getObjectIdentity(const ObjectAddress *address);
extern int read_objtype_from_string(const char *objtype);
extern char *getObjectIdentityParts(const ObjectAddress *address,
                                    List **objname, List **objargs);
extern ArrayType *strlist_to_textarray(List *list);
extern Oid get_object_package(const ObjectAddress* address);

#endif   /* PARSE_OBJECT_H */
