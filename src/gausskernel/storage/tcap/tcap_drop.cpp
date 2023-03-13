/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 * http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * ---------------------------------------------------------------------------------------
 *
 * tcap_drop.cpp
 * Routines to support Timecapsule `Recyclebin-based query, restore`.
 * We use Tr prefix to indicate it in following coding.
 *
 * IDENTIFICATION
 * src/gausskernel/storage/tcap/tcap_drop.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "postgres.h"

#include "pgstat.h"
#include "access/reloptions.h"
#include "access/sysattr.h"
#include "access/xlog.h"
#include "catalog/dependency.h"
#include "catalog/heap.h"
#include "catalog/index.h"
#include "catalog/indexing.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_collation_fn.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_conversion_fn.h"
#include "catalog/pg_conversion.h"
#include "catalog/pg_depend.h"
#include "catalog/pg_extension_data_source.h"
#include "catalog/pg_extension.h"
#include "catalog/pg_foreign_data_wrapper.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_job.h"
#include "catalog/pg_language.h"
#include "catalog/pg_largeobject.h"
#include "catalog/pg_object.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_opfamily.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_recyclebin.h"
#include "catalog/pg_rewrite.h"
#include "catalog/pg_rlspolicy.h"
#include "catalog/pg_synonym.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_trigger.h"
#include "catalog/pg_ts_config.h"
#include "catalog/pg_ts_dict.h"
#include "catalog/pg_ts_parser.h"
#include "catalog/pg_ts_template.h"
#include "catalog/pgxc_class.h"
#include "catalog/storage.h"
#include "client_logic/client_logic.h"
#include "commands/comment.h"
#include "commands/dbcommands.h"
#include "commands/directory.h"
#include "commands/extension.h"
#include "commands/proclang.h"
#include "commands/publicationcmds.h"
#include "commands/schemacmds.h"
#include "commands/seclabel.h"
#include "commands/sec_rls_cmds.h"
#include "commands/tablecmds.h"
#include "commands/tablespace.h"
#include "commands/trigger.h"
#include "commands/typecmds.h"
#include "executor/node/nodeModifyTable.h"
#include "rewrite/rewriteRemove.h"
#include "storage/lmgr.h"
#include "storage/predicate.h"
#include "storage/smgr/relfilenode.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/relcache.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"

#include "storage/tcap.h"
#include "storage/tcap_impl.h"

static void TrRenameClass(TrObjDesc *baseDesc, ObjectAddress *object, const char *newName)
{
    Relation rel;
    HeapTuple tup;
    HeapTuple newtup;
    char rbname[NAMEDATALEN];
    Datum values[Natts_pg_class] = { 0 };
    bool nulls[Natts_pg_class] = { false };
    bool replaces[Natts_pg_class] = { false };
    Oid relid = object->objectId;
    errno_t rc = EOK;

    if (newName) {
        rc = strncpy_s(rbname, NAMEDATALEN, newName, strlen(newName));
        securec_check(rc, "\0", "\0");
    } else {
        TrGenObjName(rbname, object->classId, relid);
    }

    replaces[Anum_pg_class_relname - 1] = true;
    values[Anum_pg_class_relname - 1] = CStringGetDatum(rbname);

    rel = heap_open(RelationRelationId, RowExclusiveLock);

    tup = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if (!HeapTupleIsValid(tup)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for relation %u", relid)));
    }

    newtup = heap_modify_tuple(tup, RelationGetDescr(rel), values, nulls, replaces);

    simple_heap_update(rel, &newtup->t_self, newtup);

    CatalogUpdateIndexes(rel, newtup);

    ReleaseSysCache(tup);

    heap_freetuple_ext(newtup);

    heap_close(rel, RowExclusiveLock);
}

static void TrRenameCommon(TrObjDesc *baseDesc, ObjectAddress *object, Oid relid, int natts, int oidAttrNum,
    Oid oidIndexId, char *objTag)
{
    Relation rel;
    HeapTuple tup;
    HeapTuple newtup;
    char rbname[NAMEDATALEN];
    Datum *values = (Datum *)palloc0(sizeof(Datum) * natts);
    bool *nulls = (bool *)palloc0(sizeof(bool) * natts);
    bool *replaces = (bool *)palloc0(sizeof(bool) * natts);
    ScanKeyData skey[1];
    SysScanDesc sd;

    TrGenObjName(rbname, object->classId, object->objectId);

    replaces[oidAttrNum - 1] = true;
    values[oidAttrNum - 1] = CStringGetDatum(rbname);

    rel = heap_open(relid, RowExclusiveLock);

    ScanKeyInit(&skey[0], ObjectIdAttributeNumber, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(object->objectId));
    sd = systable_beginscan(rel, oidIndexId, true, NULL, 1, skey);

    tup = systable_getnext(sd);
    if (!HeapTupleIsValid(tup)) {
        pfree(values);
        pfree(nulls);
        pfree(replaces);
        ereport(ERROR,
            (errcode(ERRCODE_NO_DATA_FOUND), errmsg("could not find tuple for %s %u", objTag, object->objectId)));
    }

    newtup = heap_modify_tuple(tup, RelationGetDescr(rel), values, nulls, replaces);

    simple_heap_update(rel, &newtup->t_self, newtup);

    CatalogUpdateIndexes(rel, newtup);

    heap_freetuple_ext(newtup);

    systable_endscan(sd);

    heap_close(rel, RowExclusiveLock);

    pfree(values);
    pfree(nulls);
    pfree(replaces);
}

static void TrDeleteBaseid(Oid baseid)
{
    Relation rbRel;
    SysScanDesc sd;
    ScanKeyData skey[1];
    HeapTuple tup;

    rbRel = heap_open(RecyclebinRelationId, RowExclusiveLock);

    ScanKeyInit(&skey[0], Anum_pg_recyclebin_rcybaseid, BTEqualStrategyNumber, F_INT8EQ, ObjectIdGetDatum(baseid));

    sd = systable_beginscan(rbRel, RecyclebinBaseidIndexId, true, NULL, 1, skey);
    while (HeapTupleIsValid(tup = systable_getnext(sd))) {
        simple_heap_delete(rbRel, &tup->t_self);
    }

    systable_endscan(sd);
    heap_close(rbRel, RowExclusiveLock);

    /*
     * CommandCounterIncrement here to ensure that preceding changes are all
     * visible to the next deletion step.
     */
    CommandCounterIncrement();
}

static void TrDeleteId(Oid id)
{
    Relation rbRel;
    SysScanDesc sd;
    ScanKeyData skey[1];
    HeapTuple tup;

    rbRel = heap_open(RecyclebinRelationId, RowExclusiveLock);

    ScanKeyInit(&skey[0], ObjectIdAttributeNumber, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(id));

    sd = systable_beginscan(rbRel, RecyclebinIdIndexId, true, NULL, 1, skey);
    if (HeapTupleIsValid(tup = systable_getnext(sd))) {
        simple_heap_delete(rbRel, &tup->t_self);
    }

    systable_endscan(sd);
    heap_close(rbRel, RowExclusiveLock);

    /*
     * CommandCounterIncrement here to ensure that preceding changes are all
     * visible to the next deletion step.
     */
    CommandCounterIncrement();
}

static inline bool TrNeedLogicDrop(const ObjectAddress *object)
{
    return object->rbDropMode == RB_DROP_MODE_LOGIC;
}

static bool TrCanPurge(const TrObjDesc *baseDesc, const ObjectAddress *object, char relKind)
{
    Relation depRel;
    SysScanDesc sd;
    HeapTuple tuple;
    ScanKeyData key[3];
    int nkeys;
    bool found = false;

    if (relKind != RELKIND_INDEX && relKind != RELKIND_GLOBAL_INDEX && relKind != RELKIND_RELATION) {
        return false;
    }

    depRel = heap_open(DependRelationId, AccessShareLock);

    ScanKeyInit(&key[0], Anum_pg_depend_classid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(object->classId));
    ScanKeyInit(&key[1], Anum_pg_depend_objid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(object->objectId));
    nkeys = 2;
    if (object->objectSubId != 0) {
        ScanKeyInit(&key[2], Anum_pg_depend_objsubid, BTEqualStrategyNumber, F_INT4EQ,
            Int32GetDatum(object->objectSubId));
        nkeys = 3;
    }

    sd = systable_beginscan(depRel, DependDependerIndexId, true, NULL, nkeys, key);
    while (HeapTupleIsValid(tuple = systable_getnext(sd))) {
        Form_pg_depend depForm = (Form_pg_depend)GETSTRUCT(tuple);
        if (depForm->refclassid == RelationRelationId && depForm->refobjid == baseDesc->relid) {
            if (depForm->deptype != DEPENDENCY_AUTO) {
                found = false;
                break;
            }
            found = true;
        }
    }

    systable_endscan(sd);
    heap_close(depRel, AccessShareLock);
    return found;
}

static void TrDoDropIndex(TrObjDesc *baseDesc, ObjectAddress *object)
{
    Assert(object->objectSubId == 0);

    if (TrNeedLogicDrop(object)) {
        TrObjDesc desc = *baseDesc;

        if (!TR_IS_BASE_OBJ(baseDesc, object)) {
            /* Deletion lock already accquired before single object drop. */
            Relation rel = relation_open(object->objectId, NoLock);

            TrDescInit(rel, &desc, RB_OPER_DROP, TrGetObjType(RelationGetNamespace(rel), RELKIND_INDEX),
                TrCanPurge(baseDesc, object, RelationGetRelkind(rel)));
            relation_close(rel, NoLock);

            TrDescWrite(&desc);
        }

        TrRenameClass(baseDesc, object, desc.name);
    } else {
        index_drop(object->objectId, false);
    }

    return;
}

static void TrDoDropTable(TrObjDesc *baseDesc, ObjectAddress *object, char relKind)
{
    if (TrNeedLogicDrop(object)) {
        TrObjDesc desc;

        if (object->objectSubId != 0 || relKind == RELKIND_VIEW || relKind == RELKIND_COMPOSITE_TYPE ||
            relKind == RELKIND_FOREIGN_TABLE) {
            TrRenameClass(baseDesc, object, NULL);
            return;
        }

        desc = *baseDesc;
        if (!TR_IS_BASE_OBJ(baseDesc, object)) {
            /* Deletion lock already accquired before single object drop. */
            Relation rel = relation_open(object->objectId, NoLock);

            TrDescInit(rel, &desc, RB_OPER_DROP, TrGetObjType(InvalidOid, relKind),
                TrCanPurge(baseDesc, object, relKind));
            relation_close(rel, NoLock);

            TrDescWrite(&desc);
        }

        TrRenameClass(baseDesc, object, desc.name);

        return;
    }

    /*
     * relation_open() must be before the heap_drop_with_catalog(). If you reload
     * relation after drop, it may cause other exceptions during the drop process.
     */
    if (object->objectSubId != 0)
        RemoveAttributeById(object->objectId, object->objectSubId);
    else
        heap_drop_with_catalog(object->objectId);

    /*
     * IMPORANT: The relation must not be reloaded after heap_drop_with_catalog()
     * is executed to drop this relation.If you reload relation after drop, it may
     * cause other exceptions during the drop process
     */

    return;
}

static void TrDoDropType(TrObjDesc *baseDesc, ObjectAddress *object)
{
    if (TrNeedLogicDrop(object)) {
        TrRenameCommon(baseDesc, object, TypeRelationId, Natts_pg_type, Anum_pg_type_typname, TypeOidIndexId, "type");
    } else {
        RemoveTypeById(object->objectId);
    }

    return;
}

static void TrDoDropConstraint(TrObjDesc *baseDesc, ObjectAddress *object)
{
    if (TrNeedLogicDrop(object)) {
        TrRenameCommon(baseDesc, object, ConstraintRelationId, Natts_pg_constraint, Anum_pg_constraint_conname,
            ConstraintOidIndexId, "constraint");
    } else {
        RemoveConstraintById(object->objectId);
    }

    return;
}

static void TrDoDropTrigger(TrObjDesc *baseDesc, ObjectAddress *object)
{
    if (TrNeedLogicDrop(object)) {
        TrRenameCommon(baseDesc, object, TriggerRelationId, Natts_pg_trigger, Anum_pg_trigger_tgname, TriggerOidIndexId,
            "trigger");
    } else {
        RemoveTriggerById(object->objectId);
    }

    return;
}

static void TrDoDropRewrite(TrObjDesc *baseDesc, ObjectAddress *object)
{
    if (TrNeedLogicDrop(object)) {
        /* nothing to do as rule-based view requires that origin rule name preserved. */
    } else {
        RemoveRewriteRuleById(object->objectId);
    }

    return;
}

static void TrDoDropAttrdef(TrObjDesc *baseDesc, ObjectAddress *object)
{
    if (TrNeedLogicDrop(object)) {
        /* nothing to do as no name attribute in system catalog */
    } else {
        RemoveAttrDefaultById(object->objectId);
    }

    return;
}

static void TrDoDropProc(TrObjDesc *baseDesc, ObjectAddress *object)
{
    if (TrNeedLogicDrop(object)) {
        TrRenameCommon(baseDesc, object, ProcedureRelationId, Natts_pg_proc, Anum_pg_proc_proname, ProcedureOidIndexId,
            "procedure");
    } else {
        RemoveFunctionById(object->objectId);
    }

    return;
}

static void TrDoDropCast(TrObjDesc *baseDesc, ObjectAddress *object)
{
    if (TrNeedLogicDrop(object)) {
        /* nothing to do as no name attribute in system catalog */
    } else {
        DropCastById(object->objectId);
    }

    return;
}

static void TrDoDropCollation(TrObjDesc *baseDesc, ObjectAddress *object)
{
    if (TrNeedLogicDrop(object)) {
        TrRenameCommon(baseDesc, object, CollationRelationId, Natts_pg_collation, Anum_pg_collation_collname,
            CollationOidIndexId, "collation");
    } else {
        RemoveCollationById(object->objectId);
    }

    return;
}

static void TrDoDropConversion(TrObjDesc *baseDesc, ObjectAddress *object)
{
    if (TrNeedLogicDrop(object)) {
        TrRenameCommon(baseDesc, object, ConversionRelationId, Natts_pg_conversion, Anum_pg_conversion_conname,
            ConversionOidIndexId, "conversion");
    } else {
        RemoveConversionById(object->objectId);
    }

    return;
}


static void TrDoDropProceduralLanguage(TrObjDesc *baseDesc, ObjectAddress *object)
{
    if (TrNeedLogicDrop(object)) {
        TrRenameCommon(baseDesc, object, LanguageRelationId, Natts_pg_language, Anum_pg_language_lanname,
            LanguageOidIndexId, "language");
    } else {
        DropProceduralLanguageById(object->objectId);
    }

    return;
}

static void TrDoDropLargeObject(TrObjDesc *baseDesc, ObjectAddress *object)
{
    if (TrNeedLogicDrop(object)) {
        /* nothing to do as no name attribute in system catalog */
    } else {
        LargeObjectDrop(object->objectId);
    }

    return;
}

static void TrDoDropOperator(TrObjDesc *baseDesc, ObjectAddress *object)
{
    if (TrNeedLogicDrop(object)) {
        TrRenameCommon(baseDesc, object, OperatorRelationId, Natts_pg_operator, Anum_pg_operator_oprname,
            OperatorOidIndexId, "operator");
    } else {
        RemoveOperatorById(object->objectId);
    }

    return;
}

static void TrDoDropOpClass(TrObjDesc *baseDesc, ObjectAddress *object)
{
    if (TrNeedLogicDrop(object)) {
        TrRenameCommon(baseDesc, object, OperatorClassRelationId, Natts_pg_opclass, Anum_pg_opclass_opcname,
            OpclassOidIndexId, "opclass");
    } else {
        RemoveOpClassById(object->objectId);
    }

    return;
}

static void TrDoDropOpFamily(TrObjDesc *baseDesc, ObjectAddress *object)
{
    if (TrNeedLogicDrop(object)) {
        TrRenameCommon(baseDesc, object, OperatorFamilyRelationId, Natts_pg_opfamily, Anum_pg_opfamily_opfname,
            OpfamilyOidIndexId, "opfamily");
    } else {
        RemoveOpFamilyById(object->objectId);
    }

    return;
}


static void TrDoDropAmOp(TrObjDesc *baseDesc, ObjectAddress *object)
{
    if (TrNeedLogicDrop(object)) {
        /* nothing to do as no name attribute in system catalog */
    } else {
        RemoveAmOpEntryById(object->objectId);
    }

    return;
}

static void TrDoDropAmProc(TrObjDesc *baseDesc, ObjectAddress *object)
{
    if (TrNeedLogicDrop(object)) {
        /* nothing to do as no name attribute in system catalog */
    } else {
        RemoveAmProcEntryById(object->objectId);
    }

    return;
}

static void TrDoDropSchema(TrObjDesc *baseDesc, ObjectAddress *object)
{
    if (TrNeedLogicDrop(object)) {
        TrRenameCommon(baseDesc, object, NamespaceRelationId, Natts_pg_namespace, Anum_pg_namespace_nspname,
            NamespaceOidIndexId, "namespace");
    } else {
        RemoveSchemaById(object->objectId);
    }

    return;
}

static void TrDoDropTSParser(TrObjDesc *baseDesc, ObjectAddress *object)
{
    if (TrNeedLogicDrop(object)) {
        TrRenameCommon(baseDesc, object, TSParserRelationId, Natts_pg_ts_parser, Anum_pg_ts_parser_prsname,
            TSParserOidIndexId, "ts parser");
    } else {
        RemoveTSParserById(object->objectId);
    }

    return;
}

static void TrDoDropTSDictionary(TrObjDesc *baseDesc, ObjectAddress *object)
{
    if (TrNeedLogicDrop(object)) {
        TrRenameCommon(baseDesc, object, TSDictionaryRelationId, Natts_pg_ts_dict, Anum_pg_ts_dict_dictname,
            TSDictionaryOidIndexId, "ts dictionary");
    } else {
        RemoveTSDictionaryById(object->objectId);
    }

    return;
}

static void TrDoDropTSTemplate(TrObjDesc *baseDesc, ObjectAddress *object)
{
    if (TrNeedLogicDrop(object)) {
        TrRenameCommon(baseDesc, object, TSTemplateRelationId, Natts_pg_ts_template, Anum_pg_ts_template_tmplname,
            TSTemplateOidIndexId, "ts template");
    } else {
        RemoveTSTemplateById(object->objectId);
    }

    return;
}

static void TrDoDropTSConfiguration(TrObjDesc *baseDesc, ObjectAddress *object)
{
    if (TrNeedLogicDrop(object)) {
        TrRenameCommon(baseDesc, object, TSConfigRelationId, Natts_pg_ts_config, Anum_pg_ts_config_cfgname,
            TSConfigOidIndexId, "ts configuration");
    } else {
        RemoveTSConfigurationById(object->objectId);
    }

    return;
}

static void TrDoDropForeignDataWrapper(TrObjDesc *baseDesc, ObjectAddress *object)
{
    if (TrNeedLogicDrop(object)) {
        TrRenameCommon(baseDesc, object, ForeignDataWrapperRelationId, Natts_pg_foreign_data_wrapper,
            Anum_pg_foreign_data_wrapper_fdwname, ForeignDataWrapperOidIndexId, "foreign data wrapper");
    } else {
        RemoveForeignDataWrapperById(object->objectId);
    }

    return;
}

static void TrDoDropForeignServer(TrObjDesc *baseDesc, ObjectAddress *object)
{
    if (TrNeedLogicDrop(object)) {
        TrRenameCommon(baseDesc, object, ForeignServerRelationId, Natts_pg_foreign_server,
            Anum_pg_foreign_server_srvname, ForeignServerOidIndexId, "foreign server");
    } else {
        RemoveForeignServerById(object->objectId);
    }

    return;
}

static void TrDoDropUserMapping(TrObjDesc *baseDesc, ObjectAddress *object)
{
    if (TrNeedLogicDrop(object)) {
        /* nothing to do as no name attribute in system catalog */
    } else {
        RemoveUserMappingById(object->objectId);
    }

    return;
}


static void TrDoDropDefaultACL(TrObjDesc *baseDesc, ObjectAddress *object)
{
    if (TrNeedLogicDrop(object)) {
        /* nothing to do as no name attribute in system catalog */
    } else {
        RemoveDefaultACLById(object->objectId);
    }

    return;
}

static void TrDoDropPgxcClass(TrObjDesc *baseDesc, ObjectAddress *object)
{
    if (TrNeedLogicDrop(object)) {
        /* nothing to do as no name attribute in system catalog */
    } else {
        RemovePgxcClass(object->objectId);
    }

    return;
}

static void TrDoDropExtension(TrObjDesc *baseDesc, ObjectAddress *object)
{
    if (TrNeedLogicDrop(object)) {
        TrRenameCommon(baseDesc, object, ExtensionRelationId, Natts_pg_extension, Anum_pg_extension_extname,
            ExtensionOidIndexId, "extension");
    } else {
        RemoveExtensionById(object->objectId);
    }

    return;
}

static void TrDoDropDataSource(TrObjDesc *baseDesc, ObjectAddress *object)
{
    if (TrNeedLogicDrop(object)) {
        TrRenameCommon(baseDesc, object, DataSourceRelationId, Natts_pg_extension_data_source,
            Anum_pg_extension_data_source_srcname, DataSourceOidIndexId, "extension data source");
    } else {
        RemoveDataSourceById(object->objectId);
    }

    return;
}

static void TrDoDropDirectory(TrObjDesc *baseDesc, ObjectAddress *object)
{
    if (TrNeedLogicDrop(object)) {
        TrRenameCommon(baseDesc, object, PgDirectoryRelationId, Natts_pg_directory, Anum_pg_directory_directory_name,
            PgDirectoryOidIndexId, "directory");
    } else {
        RemoveDirectoryById(object->objectId);
    }

    return;
}

static void TrDoDropRlsPolicy(TrObjDesc *baseDesc, ObjectAddress *object)
{
    if (TrNeedLogicDrop(object)) {
        TrRenameCommon(baseDesc, object, RlsPolicyRelationId, Natts_pg_rlspolicy, Anum_pg_rlspolicy_polname,
            PgRlspolicyOidIndex, "rlspolicy");
    } else {
        RemoveRlsPolicyById(object->objectId);
    }

    return;
}

static void TrDoDropJob(TrObjDesc *baseDesc, ObjectAddress *object)
{
    if (TrNeedLogicDrop(object)) {
        /* nothing to do as no name attribute in system catalog */
    } else {
        RemoveJobById(object->objectId);
    }

    return;
}

static void TrDoDropSynonym(TrObjDesc *baseDesc, ObjectAddress *object)
{
    if (TrNeedLogicDrop(object)) {
        TrRenameCommon(baseDesc, object, PgSynonymRelationId, Natts_pg_synonym, Anum_pg_synonym_synname,
            SynonymOidIndexId, "synonym");
    } else {
        RemoveSynonymById(object->objectId);
    }

    return;
}

static void TrDoDropPublicationRel(TrObjDesc *baseDesc, ObjectAddress *object)
{
    if (TrNeedLogicDrop(object)) {
        /* nothing to do as no name attribute in system catalog */
    } else {
        RemovePublicationRelById(object->objectId);
    }

    return;
}

/*
 * doDeletion: delete a single object
 * return false if logic deleted,
 * return true if physical deleted,
 */
static void TrDoDrop(TrObjDesc *baseDesc, ObjectAddress *object)
{
    switch (getObjectClass(object)) {
        case OCLASS_CLASS: {
            char relKind = get_rel_relkind(object->objectId);
            if (relKind == RELKIND_INDEX) {
                TrDoDropIndex(baseDesc, object);
            } else {
                /*
                 * We use a unified entry for others:
                 * RELKIND_RELATION, RELKIND_SEQUENCE,
                 * RELKIND_TOASTVALUE, RELKIND_VIEW,
                 * RELKIND_COMPOSITE_TYPE, RELKIND_FOREIGN_TABLE
                 */
                TrDoDropTable(baseDesc, object, relKind);
            }
            break;
        }

        case OCLASS_TYPE:
            TrDoDropType(baseDesc, object);
            break;

        case OCLASS_CONSTRAINT:
            TrDoDropConstraint(baseDesc, object);
            break;

        case OCLASS_TRIGGER:
            TrDoDropTrigger(baseDesc, object);
            break;

        case OCLASS_REWRITE:
            TrDoDropRewrite(baseDesc, object);
            break;

        case OCLASS_DEFAULT:
            TrDoDropAttrdef(baseDesc, object);
            break;

        case OCLASS_PROC:
            TrDoDropProc(baseDesc, object);
            break;

        case OCLASS_CAST:
            TrDoDropCast(baseDesc, object);
            break;

        case OCLASS_COLLATION:
            TrDoDropCollation(baseDesc, object);
            break;

        case OCLASS_CONVERSION:
            TrDoDropConversion(baseDesc, object);
            break;

        case OCLASS_LANGUAGE:
            TrDoDropProceduralLanguage(baseDesc, object);
            break;

        case OCLASS_LARGEOBJECT:
            TrDoDropLargeObject(baseDesc, object);
            break;

        case OCLASS_OPERATOR:
            TrDoDropOperator(baseDesc, object);
            break;

        case OCLASS_OPCLASS:
            TrDoDropOpClass(baseDesc, object);
            break;

        case OCLASS_OPFAMILY:
            TrDoDropOpFamily(baseDesc, object);
            break;

        case OCLASS_AMOP:
            TrDoDropAmOp(baseDesc, object);
            break;

        case OCLASS_AMPROC:
            TrDoDropAmProc(baseDesc, object);
            break;

        case OCLASS_SCHEMA:
            TrDoDropSchema(baseDesc, object);
            break;

        case OCLASS_TSPARSER:
            TrDoDropTSParser(baseDesc, object);
            break;

        case OCLASS_TSDICT:
            TrDoDropTSDictionary(baseDesc, object);
            break;

        case OCLASS_TSTEMPLATE:
            TrDoDropTSTemplate(baseDesc, object);
            break;

        case OCLASS_TSCONFIG:
            TrDoDropTSConfiguration(baseDesc, object);
            break;

            /*
             * OCLASS_ROLE, OCLASS_DATABASE, OCLASS_TBLSPACE intentionally not
             * handled here
             */

        case OCLASS_FDW:
            TrDoDropForeignDataWrapper(baseDesc, object);
            break;

        case OCLASS_FOREIGN_SERVER:
            TrDoDropForeignServer(baseDesc, object);
            break;

        case OCLASS_USER_MAPPING:
            TrDoDropUserMapping(baseDesc, object);
            break;

        case OCLASS_DEFACL:
            TrDoDropDefaultACL(baseDesc, object);
            break;

        case OCLASS_PGXC_CLASS:
            TrDoDropPgxcClass(baseDesc, object);
            break;

        case OCLASS_EXTENSION:
            TrDoDropExtension(baseDesc, object);
            break;

        case OCLASS_DATA_SOURCE:
            TrDoDropDataSource(baseDesc, object);
            break;

        case OCLASS_DIRECTORY:
            TrDoDropDirectory(baseDesc, object);
            break;

        case OCLASS_RLSPOLICY:
            TrDoDropRlsPolicy(baseDesc, object);
            break;

        case OCLASS_PG_JOB:
            if ((IS_PGXC_COORDINATOR && !IsConnFromCoord()) || (g_instance.role == VSINGLENODE))
                TrDoDropJob(baseDesc, object);
            break;

        case OCLASS_SYNONYM:
            TrDoDropSynonym(baseDesc, object);
            break;

        case OCLASS_PUBLICATION_REL:
            TrDoDropPublicationRel(baseDesc, object);
            break;

        default:
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("unrecognized object class: %u", object->classId)));
            break;
    }

    return;
}

/*
 * deleteOneObject: delete a single object for TrDrop.
 *
 * *depRel is the already-open pg_depend relation.
 */
static void TrDropOneObject(TrObjDesc *baseDesc, ObjectAddress *object, Relation *depRel)
{
    ScanKeyData key[3];
    int nkeys;
    SysScanDesc scan;
    HeapTuple tup;

    /* DROP hook of the objects being removed */
    if (object_access_hook) {
        ObjectAccessDrop dropArg;

        dropArg.dropflags = PERFORM_DELETION_INVALID;
        InvokeObjectAccessHook(OAT_DROP, object->classId, object->objectId, object->objectSubId, &dropArg);
    }

    /*
     * Delete the object itself, in an object-type-dependent way.
     *
     * We used to do this after removing the outgoing dependency links, but it
     * seems just as reasonable to do it beforehand.  In the concurrent case
     * we *must *do it in this order, because we can't make any transactional
     * updates before calling doDeletion() --- they'd get committed right
     * away, which is not cool if the deletion then fails.
     */
    TrDoDrop(baseDesc, object);

    /*
     * In logical drop mode, we will keep all related system entries, including
     * linked entries such as pg_depend records. It is done!
     */
    if (TrNeedLogicDrop(object)) {
        /*
         * CommandCounterIncrement here to ensure that preceding changes are all
         * visible to the next deletion step.
         */
        CommandCounterIncrement();

        /*
         * Logic Drop done!
         */
        return;
    }

    /*
     * In physical drop mode, we continue to remove all related system entries.
     */

    /*
     * Now remove any pg_depend records that link from this object to others.
     * (Any records linking to this object should be gone already.)
     *
     * When dropping a whole object (subId = 0), remove all pg_depend records
     * for its sub-objects too.
     */
    ScanKeyInit(&key[0], Anum_pg_depend_classid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(object->classId));
    ScanKeyInit(&key[1], Anum_pg_depend_objid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(object->objectId));
    if (object->objectSubId != 0) {
        ScanKeyInit(&key[2], Anum_pg_depend_objsubid, BTEqualStrategyNumber, F_INT4EQ,
            Int32GetDatum(object->objectSubId));
        nkeys = 3;
    } else
        nkeys = 2;

    scan = systable_beginscan(*depRel, DependDependerIndexId, true, NULL, nkeys, key);

    while (HeapTupleIsValid(tup = systable_getnext(scan))) {
        simple_heap_delete(*depRel, &tup->t_self);
    }

    systable_endscan(scan);

    /*
     * Delete shared dependency references related to this object.	Again, if
     * subId = 0, remove records for sub-objects too.
     */
    deleteSharedDependencyRecordsFor(object->classId, object->objectId, object->objectSubId);

    /*
     * Delete any comments or security labels associated with this object.
     * (This is a convenient place to do these things, rather than having
     * every object type know to do it.)
     */
    DeleteComments(object->objectId, object->classId, object->objectSubId);
    DeleteSecurityLabel(object);

    /*
     * CommandCounterIncrement here to ensure that preceding changes are all
     * visible to the next deletion step.
     */
    CommandCounterIncrement();

    /*
     * Physical Drop done!
     */
}

static bool TrObjIsInList(const ObjectAddresses *targetObjects, const ObjectAddress *thisobj)
{
    ObjectAddress *item = NULL;

    for (int i = 0; i < targetObjects->numrefs; i++) {
        item = targetObjects->refs + i;
        if (TrObjIsEqual(thisobj, item)) {
            return true;
        }
    }
    return false;
}

static ObjectAddress *TrFindIdxInTarget(ObjectAddresses *targetObjects, ObjectAddress *item)
{
    ObjectAddress *thisobj = NULL;

    for (int i = 0; i < targetObjects->numrefs; i++) {
        thisobj = targetObjects->refs + i;
        if (TrObjIsEqual(item, thisobj)) {
            return thisobj;
        }
    }

    return NULL;
}

/*
 * output: refthisobjs
 */
static void TrFindAllSubObjs(Relation depRel, const ObjectAddress *refobj, ObjectAddresses *refthisobjs)
{
    SysScanDesc sd;
    HeapTuple tuple;
    ScanKeyData key[3];
    int nkeys;

    ScanKeyInit(&key[0], Anum_pg_depend_refclassid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(refobj->classId));
    ScanKeyInit(&key[1], Anum_pg_depend_refobjid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(refobj->objectId));
    nkeys = 2;
    if (refobj->objectSubId != 0) {
        ScanKeyInit(&key[2], Anum_pg_depend_refobjsubid, BTEqualStrategyNumber, F_INT4EQ,
            Int32GetDatum(refobj->objectSubId));
        nkeys = 3;
    }

    sd = systable_beginscan(depRel, DependReferenceIndexId, true, NULL, nkeys, key);
    while (HeapTupleIsValid(tuple = systable_getnext(sd))) {
        Form_pg_depend depForm = (Form_pg_depend)GETSTRUCT(tuple);

        /* add the refs to list */
        add_object_address_ext(depForm->classid, depForm->objid, depForm->objsubid, depForm->deptype, refthisobjs);
    }

    systable_endscan(sd);
    return;
}

static void TrTagPhyDeleteSubObjs(Relation depRel, ObjectAddresses *targetObjects, ObjectAddress *thisobj)
{
    ObjectAddress *item = NULL;

    ObjectAddresses *refthisobjs = new_object_addresses();

    /* Tag this obj RB_DROP_MODE_PHYSICAL */
    thisobj->rbDropMode = RB_DROP_MODE_PHYSICAL;

    /* Find all sub objs refered to this obj */
    TrFindAllSubObjs(depRel, thisobj, refthisobjs);

    for (int i = 0; i < refthisobjs->numrefs; i++) {
        item = refthisobjs->refs + i;

        /* the item must exists in targetObjects. */
        item = TrFindIdxInTarget(targetObjects, item);
        if (item == NULL || item->rbDropMode == RB_DROP_MODE_PHYSICAL) {
            continue;
        }
        TrTagPhyDeleteSubObjs(depRel, targetObjects, item);
    }

    free_object_addresses(refthisobjs);
    return;
}

static bool TrNeedPhyDelete(Relation depRel, ObjectAddresses *targetObjects, ObjectAddress *thisobj)
{
    ObjectAddress *item = NULL;
    ObjectAddresses *refobjs = new_object_addresses();
    bool result = false;

    /* Find all objs this obj refered */
    TrFindAllRefObjs(depRel, thisobj, refobjs);

    /* Step 1: tag refobjs of thisobj, return directly if ALL refobjs not need physical drop. */
    for (int i = 0; i < refobjs->numrefs; i++) {
        item = refobjs->refs + i;
        if (!TrObjIsInList(targetObjects, item)) {
            result = true;
            break;
        }
    }
    if (!result) {
        free_object_addresses(refobjs);
        return result;
    }

    /* Step 2: tag refobjs with 'i' deptype to physical drop. */
    for (int i = 0; i < refobjs->numrefs; i++) {
        item = refobjs->refs + i;
        if (item->deptype == 'i') {
            item = TrFindIdxInTarget(targetObjects, item);
            Assert(item != NULL);
            if (item->rbDropMode == RB_DROP_MODE_PHYSICAL) {
                continue;
            }
            TrTagPhyDeleteSubObjs(depRel, targetObjects, item);
        }
    }

    free_object_addresses(refobjs);
    return result;
}

static void TrResetDropMode(const ObjectAddresses *targetObjects, const ObjectAddress *baseObj)
{
    ObjectAddress *thisobj = NULL;

    for (int i = 0; i < targetObjects->numrefs; i++) {
        thisobj = targetObjects->refs + i;
        if (TrObjIsEqual(thisobj, baseObj)) {
            thisobj->rbDropMode = RB_DROP_MODE_LOGIC;
            continue;
        }
        thisobj->rbDropMode = RB_DROP_MODE_INVALID;
    }
    return;
}

static void TrTagDependentObjects(Relation depRel, ObjectAddresses *targetObjects, const ObjectAddress *baseObj)
{
    ObjectAddress *thisobj = NULL;

    TrResetDropMode(targetObjects, baseObj);
    for (int i = 0; i < targetObjects->numrefs; i++) {
        thisobj = targetObjects->refs + i;
        if (TrDropModeIsAlreadySet(thisobj)) {
            continue;
        }

        if (TrNeedPhyDelete(depRel, targetObjects, thisobj)) {
            TrTagPhyDeleteSubObjs(depRel, targetObjects, thisobj);
        } else {
            thisobj->rbDropMode = RB_DROP_MODE_LOGIC;
        }
    }

    return;
}

static bool NeedTrFullEncryptedRel(Oid relid)
{
    Relation rel = relation_open(relid, NoLock);
    if (is_full_encrypted_rel(rel)) {
        relation_close(rel, NoLock);
        return false;
    }
    relation_close(rel, NoLock);
    return true;
}

bool TrCheckRecyclebinDrop(const DropStmt *stmt, ObjectAddresses *objects)
{
    Relation depRel;
    bool rbDrop = false;

    /* No work if no objects... */
    if (objects->numrefs != 1)
        return false;

    if (/*
         * Disable Recyclebin-based-Drop when target object is not OBJECT_TABLE, or
         */
        stmt->removeType != OBJECT_TABLE ||
        /* in concurrent drop mode, or */
        stmt->concurrent ||
        /* with purge option, or */
        stmt->purge ||
        /* multi objects drop. */
        list_length(stmt->objects) != 1) {
        return false;
    }

    if (!NeedTrComm(objects->refs->objectId)) {
        return false;
    }
    if (!NeedTrFullEncryptedRel(objects->refs->objectId)) {
        return false;
    }

    depRel = heap_open(DependRelationId, AccessShareLock);
    rbDrop = !TrNeedPhyDelete(depRel, objects, &objects->refs[0]);
    heap_close(depRel, AccessShareLock);

    return rbDrop;
}

void TrDrop(const DropStmt* drop, const ObjectAddresses *objects, DropBehavior behavior)
{
    Relation depRel;
    Relation baseRel;
    TrObjDesc baseDesc;
    ObjectAddresses *targetObjects = NULL;
    ObjectAddress *baseObj = objects->refs;

    /*
     * We save some cycles by opening pg_depend just once and passing the
     * Relation pointer down to all the recursive deletion steps.
     */
    depRel = heap_open(DependRelationId, RowExclusiveLock);

    /*
     * Construct a list of objects to delete (ie, the given objects plus
     * everything directly or indirectly dependent on them).  Note that
     * because we pass the whole objects list as pendingObjects context, we
     * won't get a failure from trying to delete an object that is internally
     * dependent on another one in the list; we'll just skip that object and
     * delete it when we reach its owner.
     */
    targetObjects = new_object_addresses();

    /*
     * Acquire deletion lock on each target object.  (Ideally the caller
     * has done this already, but many places are sloppy about it.)
     */
    AcquireDeletionLock(baseObj, PERFORM_DELETION_INVALID);

    /*
     * Finds all subobjects that reference the base table recursively.
     */
    findDependentObjects(baseObj, DEPFLAG_ORIGINAL, NULL, /* empty stack */
        targetObjects, objects, &depRel);
    ereport(LOG, (errmsg("Delete object %u/%u/%d", baseObj->classId, baseObj->objectId, baseObj->objectSubId)));

    /*
     * Check if deletion is allowed, and report about cascaded deletes.
     *
     * If there's exactly one object being deleted, report it the same way as
     * in performDeletion(), else we have to be vaguer.
     */
    reportDependentObjects(targetObjects, behavior, NOTICE, baseObj);

    /*
     * Tag all subobjects' drop mode: LOGIC_DROP, PYHSICAL_DROP.
     */
    TrTagDependentObjects(depRel, targetObjects, baseObj);

    /*
     * Initialize the baseDesc structure so that the logic dropped subobjects
     * can be correctly processed when renamed or placed in recycle bin. Notice
     * that base object already locked.
     */
    baseRel = relation_open(baseObj->objectId, NoLock);
    TrDescInit(baseRel, &baseDesc, RB_OPER_DROP, RB_OBJ_TABLE, true, true);
    baseDesc.id = baseDesc.baseid = TrDescWrite(&baseDesc);
    TrUpdateBaseid(&baseDesc);
    relation_close(baseRel, NoLock);

    Oid relid = RelationGetRelid(baseRel);
    UpdatePgObjectChangecsn(relid, baseRel->rd_rel->relkind);

    /*
     * Drop all the objects in the proper order.
     */
    for (int i = 0; i < targetObjects->numrefs; i++) {
        ObjectAddress *thisobj = targetObjects->refs + i;
        TrDropOneObject(&baseDesc, thisobj, &depRel);
    }

    /* And clean up */
    free_object_addresses(targetObjects);
    heap_close(depRel, RowExclusiveLock);
}

void TrDoPurgeObjectDrop(TrObjDesc *desc)
{
    ObjectAddresses *objects;
    ObjectAddress obj;

    objects = new_object_addresses();

    obj.classId = RelationRelationId;
    obj.objectId = desc->relid;
    obj.objectSubId = 0;
    add_exact_object_address(&obj, objects);

    performMultipleDeletions(objects, DROP_CASCADE, PERFORM_DELETION_INVALID);

    if (desc->type == RB_OBJ_TABLE) {
        TrDeleteBaseid(desc->baseid);
    } else { /* RB_OBJ_INDEX */
        TrDeleteId(desc->id);
    }

    free_object_addresses(objects);
    return;
}

/* TIMECAPSULE TABLE { table_name } TO BEFORE DROP [RENAME TO new_tablename] */
void TrRestoreDrop(const TimeCapsuleStmt *stmt)
{
    TrObjDesc desc;
    ObjectAddress obj;
    Relation rel;

    desc.relid = 0;
    TrOperFetch(stmt->relation, RB_OBJ_TABLE, &desc, RB_OPER_RESTORE_DROP);
    if (desc.relid != 0 && (desc.type == RB_OBJ_TABLE)) {
        stmt->relation->relname = desc.name;
        rel = heap_openrv(stmt->relation, AccessExclusiveLock);
        if (rel->rd_tam_ops == TableAmHeap) {
            heap_close(rel, NoLock);
            elog(ERROR, "timecapsule does not support astore yet");
            return;
        }
        heap_close(rel, NoLock);
    }

    desc.authid = GetUserId();
    TrOperPrep(&desc, RB_OPER_RESTORE_DROP);

    obj.classId = RelationRelationId;
    obj.objectId = desc.relid;
    obj.objectSubId = 0;

    TrRenameClass(&desc, &obj, stmt->new_relname ? stmt->new_relname : desc.originname);

    TrDeleteBaseid(desc.baseid);

    return;
}
