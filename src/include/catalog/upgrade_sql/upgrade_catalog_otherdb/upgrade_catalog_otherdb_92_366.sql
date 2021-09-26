DROP SCHEMA IF EXISTS db4ai cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_NAMESPACE, 4991;
CREATE SCHEMA db4ai;
COMMENT ON schema db4ai IS 'db4ai schema';

/* Create table gs_model_warehouse */
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 3991, 3994, 3995, 3996;

CREATE TABLE IF NOT EXISTS pg_catalog.gs_model_warehouse
(
    modelname name NOCOMPRESS NOT NULL,
    modelowner Oid NOCOMPRESS NOT NULL,
    createtime timestamp with time zone NOCOMPRESS NOT NULL,
    processedtuples int4 NOCOMPRESS NOT NULL,
    discardedtuples int4 NOCOMPRESS NOT NULL,
    pre_process_time float4 NOCOMPRESS NOT NULL,
    exec_time float4 NOCOMPRESS NOT NULL,
    iterations int4 NOCOMPRESS NOT NULL,
    outputtype Oid NOCOMPRESS NOT NULL,
    modeltype text,
    query text,
    modeldata bytea,
    weight float4[1],
    hyperparametersnames text[1],
    hyperparametersvalues text[1],
    hyperparametersoids Oid[1],
    coefnames text[1],
    coefvalues text[1],
    coefoids Oid[1],
    trainingscoresname text[1],
    trainingscoresvalue float4[1],
    modeldescribe text[1]
)WITH OIDS TABLESPACE pg_default;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 3992;
CREATE UNIQUE INDEX gs_model_oid_index ON pg_catalog.gs_model_warehouse USING BTREE(oid OID_OPS);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 3993;
CREATE UNIQUE INDEX gs_model_name_index ON pg_catalog.gs_model_warehouse USING BTREE(modelname name_ops);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;
GRANT SELECT ON TABLE pg_catalog.gs_model_warehouse TO PUBLIC;
