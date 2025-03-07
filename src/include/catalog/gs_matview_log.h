#ifndef GS_MATVIEW_LOG_H
#define GS_MATVIEW_LOG_H

#include "catalog/genbki.h"
#include "nodes/parsenodes.h"

#define MatviewLogRelationId   9753
#define MatviewLogRelationId_Rowtype_Id 9756

CATALOG(gs_matview_log,9753) BKI_WITHOUT_OIDS BKI_ROWTYPE_OID(9756) BKI_SCHEMA_MACRO
{
    Oid         mlogid;
    Oid         relid;
} FormData_gs_matview_log;

typedef FormData_gs_matview_log *Form_gs_matview_log;

#define Natts_gs_matview_log               2

#define Anum_gs_matview_log_mlogid         1
#define Anum_gs_matview_relid              2

#endif   /* GS_MATVIEW_LOG_H */
