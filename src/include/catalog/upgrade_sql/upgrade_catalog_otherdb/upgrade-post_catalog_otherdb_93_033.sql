--upgrade TABLE
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 4885, 4886, 4895, 4896;
CREATE TABLE pg_catalog.pg_statistic_history (                                                      
    namespaceid oid,                                                           
    starelid oid,                                                     
    partid oid,   
    statype "char",   
    last_analyzetime timestamp with time zone,           
    current_analyzetime timestamp with time zone not null,                                                   
    starelkind "char",                                                        
    staattnum smallint,                                                      
    stainherit boolean, 
    stanullfrac real,                                                     
    stawidth int,                                                              
    stadistinct real,
    reltuples double precision,
    relpages double precision,
    stalocktype "char",
    stakind1 smallint,
    stakind2 smallint,
    stakind3 smallint,
    stakind4 smallint,
    stakind5 smallint,
    staop1 oid,
    staop2 oid,
    staop3 oid,
    staop4 oid,
    staop5 oid,
    stanumbers1 real[],
    stanumbers2 real[],
    stanumbers3 real[],
    stanumbers4 real[],
    stanumbers5 real[],
    stavalues1 anyarray,
    stavalues2 anyarray,
    stavalues3 anyarray,
    stavalues4 anyarray,
    stavalues5 anyarray,
    stadndistinct real,
    statextinfo text                                                   
) WITH(oids=false) TABLESPACE pg_default;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 4887;
CREATE INDEX pg_catalog.pg_statistic_history_tab_statype_attnum_index ON pg_statistic_history USING btree (starelid oid_ops, statype char_ops, staattnum int2_ops);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 4888;
CREATE INDEX pg_catalog.pg_statistic_history_current_analyzetime_relid_index ON pg_statistic_history USING btree (current_analyzetime timestamptz_ops, starelid oid_ops);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;
GRANT SELECT ON pg_catalog.pg_statistic_history TO PUBLIC;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 4897, 4898, 0, 0;
CREATE TABLE pg_catalog.pg_statistic_lock (                                                      
    namespaceid oid not null,
    stalocktype "char" not null,                                                
    relid oid,
    partid oid,
    lock bool
) WITH(oids=false) TABLESPACE pg_default;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 4899;
CREATE INDEX pg_catalog.pg_statistic_lock_index ON pg_statistic_lock USING btree (namespaceid oid_ops, stalocktype char_ops, relid oid_ops, partid oid_ops);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;
GRANT SELECT ON pg_catalog.pg_statistic_lock TO PUBLIC;
