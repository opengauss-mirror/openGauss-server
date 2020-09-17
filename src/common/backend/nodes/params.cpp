/* -------------------------------------------------------------------------
 *
 * params.cpp
 *	  Support for finding the values associated with Param nodes.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/common/backend/nodes/params.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "nodes/params.h"
#include "storage/shmem.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"

/*
 * Copy a ParamListInfo structure.
 *
 * The result is allocated in CurrentMemoryContext.
 *
 * Note: the intent of this function is to make a static, self-contained
 * set of parameter values.  If dynamic parameter hooks are present, we
 * intentionally do not copy them into the result.	Rather, we forcibly
 * instantiate all available parameter values and copy the datum values.
 */
ParamListInfo copyParamList(ParamListInfo from)
{
    ParamListInfo retval;
    Size size;
    int i;

    if (from == NULL || from->numParams <= 0) {
        return NULL;
    }

    size = offsetof(ParamListInfoData, params) + from->numParams * sizeof(ParamExternData);

    retval = (ParamListInfo)palloc(size);
    retval->paramFetch = NULL;
    retval->paramFetchArg = NULL;
    retval->parserSetup = NULL;
    retval->parserSetupArg = NULL;
    retval->params_need_process = false;
    retval->numParams = from->numParams;
    retval->paramMask = NULL;
    
    for (i = 0; i < from->numParams; i++) {
        ParamExternData* oprm = &from->params[i];
        ParamExternData* nprm = &retval->params[i];
        int16 typLen;
        bool typByVal = false;

        /* Ignore parameters we don't need, to save cycles and space. */
        if (retval->paramMask != NULL && !bms_is_member(i, retval->paramMask)) {
            nprm->value = (Datum)0;
            nprm->isnull = true;
            nprm->pflags = 0;
            nprm->ptype = InvalidOid;
            continue;
        }

        /* give hook a chance in case parameter is dynamic */
        if (!OidIsValid(oprm->ptype) && from->paramFetch != NULL) {
            (*from->paramFetch)(from, i + 1);
        }

        /* flat-copy the parameter info */
        *nprm = *oprm;

        /* need datumCopy in case it's a pass-by-reference datatype */
        if (nprm->isnull || !OidIsValid(nprm->ptype)) {
            continue;
        }
        get_typlenbyval(nprm->ptype, &typLen, &typByVal);
        nprm->value = datumCopy(nprm->value, typByVal, typLen);
    }

    return retval;
}

/*
 * Estimate the amount of space required to serialize a ParamListInfo.
 */
Size EstimateParamListSpace(ParamListInfo paramLI)
{
    Size sz = sizeof(int);

    if (paramLI == NULL || paramLI->numParams <= 0)
        return sz;

    for (int i = 0; i < paramLI->numParams; i++) {
        ParamExternData *prm = &paramLI->params[i];
        Oid typeOid;
        int16 typLen;
        bool typByVal = false;

        /* Ignore parameters we don't need, to save cycles and space. */
        if (paramLI->paramMask != NULL && !bms_is_member(i, paramLI->paramMask)) {
            typeOid = InvalidOid;
        } else {
            /* give hook a chance in case parameter is dynamic */
            if (!OidIsValid(prm->ptype) && paramLI->paramFetch != NULL)
                (*paramLI->paramFetch)(paramLI, i + 1);
            typeOid = prm->ptype;
        }

        sz = add_size(sz, sizeof(Oid));    /* space for type OID */
        sz = add_size(sz, sizeof(uint16)); /* space for pflags */

        /* space for datum/isnull */
        if (OidIsValid(typeOid)) {
            get_typlenbyval(typeOid, &typLen, &typByVal);
        } else {
            /* If no type OID, assume by-value, like copyParamList does. */
            typLen = sizeof(Datum);
            typByVal = true;
        }
        sz = add_size(sz, datumEstimateSpace(prm->value, prm->isnull, typByVal, typLen));
    }

    return sz;
}

/*
 * Serialize a paramListInfo structure into caller-provided storage.
 *
 * We write the number of parameters first, as a 4-byte integer, and then
 * write details for each parameter in turn.  The details for each parameter
 * consist of a 4-byte type OID, 2 bytes of flags, and then the datum as
 * serialized by datumSerialize().  The caller is responsible for ensuring
 * that there is enough storage to store the number of bytes that will be
 * written; use EstimateParamListSpace to find out how many will be needed.
 * *start_address is updated to point to the byte immediately following those
 * written.
 *
 * RestoreParamList can be used to recreate a ParamListInfo based on the
 * serialized representation; this will be a static, self-contained copy
 * just as copyParamList would create.
 */
void SerializeParamList(ParamListInfo paramLI, char *start_address, Size len)
{
    int nparams;

    /* Write number of parameters. */
    if (paramLI == NULL || paramLI->numParams <= 0) {
        nparams = 0;
    } else {
        nparams = paramLI->numParams;
    }
    int rc = memcpy_s(start_address, len, &nparams, sizeof(int));
    securec_check_c(rc, "", "");
    Size remainLen = len - sizeof(int);
    start_address += sizeof(int);

    /* Write each parameter in turn. */
    for (int i = 0; i < nparams; i++) {
        ParamExternData *prm = &paramLI->params[i];
        Oid typeOid;
        int16 typLen;
        bool typByVal;

        /* Ignore parameters we don't need, to save cycles and space. */
        if (paramLI->paramMask != NULL && !bms_is_member(i, paramLI->paramMask)) {
            typeOid = InvalidOid;
        } else {
            /* give hook a chance in case parameter is dynamic */
            if (!OidIsValid(prm->ptype) && paramLI->paramFetch != NULL)
                (*paramLI->paramFetch)(paramLI, i + 1);
            typeOid = prm->ptype;
        }

        /* Write type OID. */
        rc = memcpy_s(start_address, remainLen, &typeOid, sizeof(Oid));
        securec_check_c(rc, "", "");
        remainLen -= sizeof(Oid);
        start_address += sizeof(Oid);

        /* Write flags. */
        rc = memcpy_s(start_address, remainLen, &prm->pflags, sizeof(uint16));
        securec_check_c(rc, "", "");
        remainLen -= sizeof(uint16);
        start_address += sizeof(uint16);

        /* Write datum/isnull. */
        if (OidIsValid(typeOid)) {
            get_typlenbyval(typeOid, &typLen, &typByVal);
        } else {
            /* If no type OID, assume by-value, like copyParamList does. */
            typLen = sizeof(Datum);
            typByVal = true;
        }
        datumSerialize(prm->value, prm->isnull, typByVal, typLen, &start_address, &remainLen);
    }
}

/*
 * Copy a ParamListInfo structure.
 *
 * The result is allocated in CurrentMemoryContext.
 *
 * Note: the intent of this function is to make a static, self-contained
 * set of parameter values.  If dynamic parameter hooks are present, we
 * intentionally do not copy them into the result.  Rather, we forcibly
 * instantiate all available parameter values and copy the datum values.
 */
ParamListInfo RestoreParamList(char *start_address, Size len)
{
    int nparams;

    int rc = memcpy_s(&nparams, len, start_address, sizeof(int));
    securec_check_c(rc, "", "");
    Size remainLen = len - sizeof(int);
    start_address += sizeof(int);

    Size size = offsetof(ParamListInfoData, params) + nparams * sizeof(ParamExternData);

    ParamListInfo paramLI = (ParamListInfo)palloc(size);
    paramLI->paramFetch = NULL;
    paramLI->paramFetchArg = NULL;
    paramLI->parserSetup = NULL;
    paramLI->parserSetupArg = NULL;
    paramLI->numParams = nparams;
    paramLI->paramMask = NULL;

    for (int i = 0; i < nparams; i++) {
        ParamExternData *prm = &paramLI->params[i];

        /* Read type OID. */
        rc = memcpy_s(&prm->ptype, remainLen, start_address, sizeof(Oid));
        securec_check_c(rc, "", "");
        remainLen -= sizeof(Oid);
        start_address += sizeof(Oid);

        /* Read flags. */
        rc = memcpy_s(&prm->pflags, remainLen, start_address, sizeof(uint16));
        securec_check_c(rc, "", "");
        remainLen -= sizeof(uint16);
        start_address += sizeof(uint16);

        /* Read datum/isnull. */
        prm->value = datumRestore(&start_address, &remainLen, &prm->isnull);
    }

    return paramLI;
}
