/*
 * PostgreSQL System Views
 *
 * Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 * src/backend/catalog/system_views.sql
 */
CREATE VIEW pg_roles AS
    SELECT
        rolname,
        rolsuper,
        rolinherit,
        rolcreaterole,
        rolcreatedb,
        rolcatupdate,
        rolcanlogin,
        rolreplication,
        rolauditadmin,
        rolsystemadmin,
        rolconnlimit,
        '********'::text as rolpassword,
        rolvalidbegin,
        rolvaliduntil,
        rolrespool,
        rolparentid,
        roltabspace,
        setconfig as rolconfig,
        pg_authid.oid,
        roluseft,
        rolkind,
        pgxc_group.group_name as nodegroup,
        roltempspace,
        rolspillspace,
        rolmonitoradmin,
        roloperatoradmin,
        rolpolicyadmin
    FROM pg_authid LEFT JOIN pg_db_role_setting s
    ON (pg_authid.oid = setrole AND setdatabase = 0) 
    LEFT JOIN pgxc_group
    ON (pg_authid.rolnodegroup = pgxc_group.oid)
    WHERE pg_authid.rolname = current_user 
    OR (SELECT rolcreaterole FROM pg_authid WHERE pg_authid.rolname = current_user)
    OR (SELECT rolsystemadmin FROM pg_authid WHERE pg_authid.rolname = current_user);

CREATE VIEW pg_shadow AS
    SELECT
        rolname AS usename,
        pg_authid.oid AS usesysid,
        rolcreatedb AS usecreatedb,
        rolsuper AS usesuper,
        rolcatupdate AS usecatupd,
        rolreplication AS userepl,
        rolpassword AS passwd,
        rolvalidbegin AS valbegin,
        rolvaliduntil AS valuntil,
        rolrespool AS respool,
        rolparentid AS parent,
        roltabspace AS spacelimit,
        setconfig AS useconfig,
        roltempspace AS tempspacelimit,
        rolspillspace AS spillspacelimit,
        rolmonitoradmin AS usemonitoradmin,
        roloperatoradmin AS useoperatoradmin,
        rolpolicyadmin AS usepolicyadmin
    FROM pg_authid LEFT JOIN pg_db_role_setting s
    ON (pg_authid.oid = setrole AND setdatabase = 0)
    WHERE rolcanlogin;

REVOKE ALL on pg_shadow FROM public;

CREATE VIEW pg_group AS
    SELECT
        rolname AS groname,
        oid AS grosysid,
        ARRAY(SELECT member FROM pg_auth_members WHERE roleid = oid) AS grolist
    FROM pg_authid
    WHERE NOT rolcanlogin;

CREATE VIEW pg_user AS
    SELECT
        rolname AS usename,
        pg_authid.oid AS usesysid,
        rolcreatedb AS usecreatedb,
        rolsuper AS usesuper,
        rolcatupdate AS usecatupd,
        rolreplication AS userepl,
        '********'::text AS passwd,
        rolvalidbegin AS valbegin,
        rolvaliduntil AS valuntil,
        rolrespool AS respool,
        rolparentid AS parent,
        roltabspace AS spacelimit,
        setconfig AS useconfig,
        pgxc_group.group_name AS nodegroup,
        roltempspace AS tempspacelimit,
        rolspillspace AS spillspacelimit,
        rolmonitoradmin AS usemonitoradmin,
        roloperatoradmin AS useoperatoradmin,
        rolpolicyadmin AS usepolicyadmin
    FROM pg_authid LEFT JOIN pg_db_role_setting s
    ON (pg_authid.oid = setrole AND setdatabase = 0)
    LEFT JOIN pgxc_group
    ON (pg_authid.rolnodegroup = pgxc_group.oid)
    WHERE rolcanlogin;

REVOKE ALL on pg_user FROM public;
	
CREATE VIEW pg_rules AS
    SELECT
        N.nspname AS schemaname,
        C.relname AS tablename,
        R.rulename AS rulename,
        pg_catalog.pg_get_ruledef(R.oid) AS definition
    FROM (pg_rewrite R JOIN pg_class C ON (C.oid = R.ev_class))
        LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
    WHERE R.rulename != '_RETURN';

create view pg_catalog.gs_labels as
SELECT labelname
    ,labeltype
    ,fqdntype
    ,CASE fqdntype
        WHEN 'column' THEN (select nspname from pg_namespace where oid = fqdnnamespace)
        WHEN 'table' THEN (select nspname from pg_namespace where oid = fqdnnamespace)
        WHEN 'view' THEN (select nspname from pg_namespace where oid = fqdnnamespace)
        WHEN 'schema' THEN (select nspname from pg_namespace where oid = fqdnnamespace)
        WHEN 'function' THEN (select nspname from pg_namespace where oid = fqdnnamespace)
        ELSE ''
    END AS schemaname
    ,CASE fqdntype
        WHEN 'column' THEN (select relname from pg_class where oid = fqdnid)
        WHEN 'table' THEN (select relname from pg_class where oid = fqdnid)
        WHEN 'view' THEN (select relname from pg_class where oid = fqdnid)
        WHEN 'function' THEN (select proname from pg_proc where oid=fqdnid)
        WHEN 'label' THEN relcolumn
        ELSE ''
    END AS fqdnname
    ,CASE fqdntype
        WHEN 'column' THEN relcolumn
        ELSE ''
    END AS columnname
FROM gs_policy_label WHERE pg_catalog.length(fqdntype)>0 ORDER BY labelname, labeltype ,fqdntype;

REVOKE ALL on pg_catalog.gs_labels FROM public;
--for audit
create view pg_catalog.gs_auditing_access as
    select distinct
        p.polname,
        'access' as pol_type,
        p.polenabled,
        a.accesstype as access_type,
        a.labelname as label_name,
        --CONCAT(l.fqdntype, ':', l.columnname) as access_object,
        CASE l.fqdntype
            WHEN 'column'   THEN l.fqdntype || ':' || l.schemaname || '.' || l.fqdnname || '.' || l.columnname
            WHEN 'table'    THEN l.fqdntype || ':' || l.schemaname || '.' || l.fqdnname
            WHEN 'view'     THEN l.fqdntype || ':' || l.schemaname || '.' || l.fqdnname
            WHEN 'schema'   THEN l.fqdntype || ':' || l.schemaname
            WHEN 'function' THEN l.fqdntype || ':' || l.schemaname || '.' || l.fqdnname
            WHEN 'label'    THEN l.fqdntype || ':' || l.columnname
            ELSE l.fqdntype || ':' || ''
        END AS access_object,
        (select
            logicaloperator
            from gs_auditing_policy_filters
            where p.Oid=policyoid) as filter_name
    from gs_auditing_policy p
        left join gs_auditing_policy_access a ON (a.policyoid=p.Oid)
        left join gs_labels l ON (a.labelname=l.labelname)
    where pg_catalog.length(a.accesstype) > 0 order by 1,3;

REVOKE ALL on pg_catalog.gs_auditing_access FROM public;

create view pg_catalog.gs_auditing_privilege as
    select distinct
        p.polname,
        'privilege' as pol_type,
        p.polenabled,
        priv.privilegetype as access_type,
        priv.labelname as label_name,
        --CONCAT(l.fqdntype, ':', l.columnname) as priv_object,
        CASE l.fqdntype
            WHEN 'column'   THEN l.fqdntype || ':' || l.schemaname || '.' || l.fqdnname || '.' || l.columnname
            WHEN 'table'    THEN l.fqdntype || ':' || l.schemaname || '.' || l.fqdnname
            WHEN 'view'     THEN l.fqdntype || ':' || l.schemaname || '.' || l.fqdnname
            WHEN 'schema'   THEN l.fqdntype || ':' || l.schemaname
            WHEN 'function' THEN l.fqdntype || ':' || l.schemaname || '.' || l.fqdnname
            WHEN 'label'    THEN l.fqdntype || ':' || l.columnname
            ELSE l.fqdntype || ':' || ''
        END AS priv_object,
        (select
            logicaloperator
            from gs_auditing_policy_filters
            where p.Oid=policyoid) as filter_name
        from gs_auditing_policy p
            left join gs_auditing_policy_privileges priv ON (priv.policyoid=p.Oid)
            left join gs_labels l ON (priv.labelname=l.labelname)
        where pg_catalog.length(priv.privilegetype) > 0 order by 1,3;

REVOKE ALL on pg_catalog.gs_auditing_privilege FROM public;

create view pg_catalog.gs_auditing as
    select * from gs_auditing_privilege
    union all
    select * from gs_auditing_access order by polname;

REVOKE ALL on pg_catalog.gs_auditing FROM public;
--for audit end

--for masking
create view pg_catalog.gs_masking as
select distinct p.polname,
p.polenabled,
a.actiontype as maskaction,
a.actlabelname as labelname,
CASE l.fqdntype
            WHEN 'column'   THEN l.fqdntype || ':' || l.schemaname || '.' || l.fqdnname || '.' || l.columnname
            WHEN 'table'    THEN l.fqdntype || ':' || l.schemaname || '.' || l.fqdnname
            WHEN 'view'     THEN l.fqdntype || ':' || l.schemaname || '.' || l.fqdnname
            WHEN 'schema'   THEN l.fqdntype || ':' || l.schemaname
            WHEN 'function' THEN l.fqdntype || ':' || l.schemaname || '.' || l.fqdnname
            WHEN 'label'    THEN l.fqdntype || ':' || l.columnname
            ELSE l.fqdntype || ':' || ''
        END AS masking_object,
(select
    logicaloperator
    from gs_masking_policy_filters
    where p.Oid=policyoid) as filter_name
from gs_masking_policy p join gs_masking_policy_actions a ON (p.Oid=a.policyoid ) join gs_labels l ON (a.actlabelname=l.labelname) WHERE l.fqdntype='column' or l.fqdntype='table' order by polname;

REVOKE ALL on pg_catalog.gs_masking FROM public;

-- CREATE VIEW for pg_rlspolicy
CREATE VIEW pg_rlspolicies AS
    SELECT
        N.nspname AS schemaname,
        C.relname AS tablename,
        pol.polname AS policyname,
        CASE
            WHEN pol.polpermissive THEN
                'PERMISSIVE'
            ELSE
                'RESTRICTIVE'
        END AS policypermissive,
        CASE
            WHEN pol.polroles = '{0}' THEN
                pg_catalog.string_to_array('public', ' ')
            ELSE
                ARRAY
                (
                    SELECT rolname
                    FROM pg_catalog.pg_authid
                    WHERE oid = ANY (pol.polroles) ORDER BY 1
                )
        END AS policyroles,
        CASE pol.polcmd
            WHEN 'r' THEN 'SELECT'
            WHEN 'a' THEN 'INSERT'
            WHEN 'w' THEN 'UPDATE'
            WHEN 'd' THEN 'DELETE'
            WHEN '*' THEN 'ALL'
        END AS policycmd,
        pg_catalog.pg_get_expr(pol.polqual, pol.polrelid) AS policyqual
    FROM pg_catalog.pg_rlspolicy pol
    JOIN pg_catalog.pg_class C ON (C.oid = pol.polrelid)
    LEFT JOIN pg_catalog.pg_namespace N ON (N.oid = C.relnamespace)
    WHERE C.relowner = (SELECT oid FROM pg_authid WHERE rolname=current_user)
    OR (SELECT rolsystemadmin FROM pg_authid WHERE rolname=current_user);

CREATE VIEW pg_views AS
    SELECT
        N.nspname AS schemaname,
        C.relname AS viewname,
        pg_catalog.pg_get_userbyid(C.relowner) AS viewowner,
        pg_catalog.pg_get_viewdef(C.oid) AS definition
    FROM pg_class C LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
    WHERE C.relkind = 'v';

CREATE VIEW pg_tables AS
    SELECT
        N.nspname AS schemaname,
        C.relname AS tablename,
        pg_catalog.pg_get_userbyid(C.relowner) AS tableowner,
        T.spcname AS tablespace,
        C.relhasindex AS hasindexes,
        C.relhasrules AS hasrules,
        C.relhastriggers AS hastriggers,
        case
            when pg_catalog.pg_check_authid(po.creator) then pg_catalog.pg_get_userbyid(po.creator)
            else CAST(NULL AS name)
        end as tablecreator,
        po.ctime AS created,
        po.mtime AS last_ddl_time
    FROM pg_class C LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
         LEFT JOIN pg_tablespace T ON (T.oid = C.reltablespace)
         LEFT JOIN pg_object po ON (po.object_oid = C.oid and po.object_type = 'r')
    WHERE C.relkind = 'r';

CREATE VIEW pg_catalog.gs_matviews AS
    SELECT
        N.nspname AS schemaname,
        C.relname AS matviewname,
        pg_catalog.pg_get_userbyid(C.relowner) AS matviewowner,
        T.spcname AS tablespace,
        C.relhasindex AS hasindexes,
        pg_catalog.pg_get_viewdef(C.oid) AS definition
    FROM pg_class C LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
         LEFT JOIN pg_tablespace T ON (T.oid = C.reltablespace)
    WHERE C.relkind = 'm';

CREATE VIEW pg_indexes AS
    SELECT
        N.nspname AS schemaname,
        C.relname AS tablename,
        I.relname AS indexname,
        T.spcname AS tablespace,
        pg_catalog.pg_get_indexdef(I.oid) AS indexdef
    FROM pg_index X JOIN pg_class C ON (C.oid = X.indrelid)
         JOIN pg_class I ON (I.oid = X.indexrelid)
         LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
         LEFT JOIN pg_tablespace T ON (T.oid = I.reltablespace)
    WHERE C.relkind IN ('r','m') AND I.relkind IN ('i','I');

-- For global temporary table
CREATE VIEW pg_catalog.pg_gtt_relstats WITH (security_barrier) AS
 SELECT n.nspname AS schemaname,
    c.relname AS tablename,
    (select relfilenode from pg_catalog.pg_get_gtt_relstats(c.oid)),
    (select relpages from pg_catalog.pg_get_gtt_relstats(c.oid)),
    (select reltuples from pg_catalog.pg_get_gtt_relstats(c.oid)),
    (select relallvisible from pg_catalog.pg_get_gtt_relstats(c.oid)),
    (select relfrozenxid from pg_catalog.pg_get_gtt_relstats(c.oid)),
    (select relminmxid from pg_catalog.pg_get_gtt_relstats(c.oid))
 FROM
     pg_class c
     LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE c.relpersistence='g' AND c.relkind in('r','p','i','t');

CREATE VIEW pg_catalog.pg_gtt_attached_pids WITH (security_barrier) AS
 SELECT n.nspname AS schemaname,
    c.relname AS tablename,
    c.oid AS relid,
    array(select pid from pg_catalog.pg_gtt_attached_pid(c.oid)) AS pids
 FROM
     pg_class c
     LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE c.relpersistence='g' AND c.relkind in('r', 'S', 'L');

CREATE VIEW pg_catalog.pg_gtt_stats WITH (security_barrier) AS
SELECT s.nspname AS schemaname,
    s.relname AS tablename,
    s.attname,
    s.stainherit AS inherited,
    s.stanullfrac AS null_frac,
    s.stawidth AS avg_width,
    s.stadistinct AS n_distinct,
        CASE
            WHEN s.stakind1 = 1 THEN s.stavalues1
            WHEN s.stakind2 = 1 THEN s.stavalues2
            WHEN s.stakind3 = 1 THEN s.stavalues3
            WHEN s.stakind4 = 1 THEN s.stavalues4
            WHEN s.stakind5 = 1 THEN s.stavalues5
        END AS most_common_vals,
        CASE
            WHEN s.stakind1 = 1 THEN s.stanumbers1
            WHEN s.stakind2 = 1 THEN s.stanumbers2
            WHEN s.stakind3 = 1 THEN s.stanumbers3
            WHEN s.stakind4 = 1 THEN s.stanumbers4
            WHEN s.stakind5 = 1 THEN s.stanumbers5
        END AS most_common_freqs,
        CASE
            WHEN s.stakind1 = 2 THEN s.stavalues1
            WHEN s.stakind2 = 2 THEN s.stavalues2
            WHEN s.stakind3 = 2 THEN s.stavalues3
            WHEN s.stakind4 = 2 THEN s.stavalues4
            WHEN s.stakind5 = 2 THEN s.stavalues5
        END AS histogram_bounds,
        CASE
            WHEN s.stakind1 = 3 THEN s.stanumbers1[1]
            WHEN s.stakind2 = 3 THEN s.stanumbers2[1]
            WHEN s.stakind3 = 3 THEN s.stanumbers3[1]
            WHEN s.stakind4 = 3 THEN s.stanumbers4[1]
            WHEN s.stakind5 = 3 THEN s.stanumbers5[1]
        END AS correlation,
        CASE
            WHEN s.stakind1 = 4 THEN s.stavalues1
            WHEN s.stakind2 = 4 THEN s.stavalues2
            WHEN s.stakind3 = 4 THEN s.stavalues3
            WHEN s.stakind4 = 4 THEN s.stavalues4
            WHEN s.stakind5 = 4 THEN s.stavalues5
        END AS most_common_elems,
        CASE
            WHEN s.stakind1 = 4 THEN s.stanumbers1
            WHEN s.stakind2 = 4 THEN s.stanumbers2
            WHEN s.stakind3 = 4 THEN s.stanumbers3
            WHEN s.stakind4 = 4 THEN s.stanumbers4
            WHEN s.stakind5 = 4 THEN s.stanumbers5
        END AS most_common_elem_freqs,
        CASE
            WHEN s.stakind1 = 5 THEN s.stanumbers1
            WHEN s.stakind2 = 5 THEN s.stanumbers2
            WHEN s.stakind3 = 5 THEN s.stanumbers3
            WHEN s.stakind4 = 5 THEN s.stanumbers4
            WHEN s.stakind5 = 5 THEN s.stanumbers5
        END AS elem_count_histogram
   FROM 
    (SELECT n.nspname,
        c.relname,
        a.attname,
        (select stainherit from pg_catalog.pg_get_gtt_statistics(c.oid, a.attnum, ''::text)) as stainherit,
        (select stanullfrac from pg_catalog.pg_get_gtt_statistics(c.oid, a.attnum, ''::text)) as stanullfrac,
        (select stawidth from pg_catalog.pg_get_gtt_statistics(c.oid, a.attnum, ''::text)) as stawidth,
        (select stadistinct from pg_catalog.pg_get_gtt_statistics(c.oid, a.attnum, ''::text)) as stadistinct,
        (select stakind1 from pg_catalog.pg_get_gtt_statistics(c.oid, a.attnum, ''::text)) as stakind1,
        (select stakind2 from pg_catalog.pg_get_gtt_statistics(c.oid, a.attnum, ''::text)) as stakind2,
        (select stakind3 from pg_catalog.pg_get_gtt_statistics(c.oid, a.attnum, ''::text)) as stakind3,
        (select stakind4 from pg_catalog.pg_get_gtt_statistics(c.oid, a.attnum, ''::text)) as stakind4,
        (select stakind5 from pg_catalog.pg_get_gtt_statistics(c.oid, a.attnum, ''::text)) as stakind5,
        (select stanumbers1 from pg_catalog.pg_get_gtt_statistics(c.oid, a.attnum, ''::text)) as stanumbers1,
        (select stanumbers2 from pg_catalog.pg_get_gtt_statistics(c.oid, a.attnum, ''::text)) as stanumbers2,
        (select stanumbers3 from pg_catalog.pg_get_gtt_statistics(c.oid, a.attnum, ''::text)) as stanumbers3,
        (select stanumbers4 from pg_catalog.pg_get_gtt_statistics(c.oid, a.attnum, ''::text)) as stanumbers4,
        (select stanumbers5 from pg_catalog.pg_get_gtt_statistics(c.oid, a.attnum, ''::text)) as stanumbers5,
        (select stavalues1 from pg_catalog.pg_get_gtt_statistics(c.oid, a.attnum, ''::text)) as stavalues1,
        (select stavalues2 from pg_catalog.pg_get_gtt_statistics(c.oid, a.attnum, ''::text)) as stavalues2,
        (select stavalues3 from pg_catalog.pg_get_gtt_statistics(c.oid, a.attnum, ''::text)) as stavalues3,
        (select stavalues4 from pg_catalog.pg_get_gtt_statistics(c.oid, a.attnum, ''::text)) as stavalues4,
        (select stavalues5 from pg_catalog.pg_get_gtt_statistics(c.oid, a.attnum, ''::text)) as stavalues5
       FROM 
         pg_class c
         JOIN pg_attribute a ON c.oid = a.attrelid
         LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE c.relpersistence='g' AND c.relkind in('r','p','i','t') and a.attnum > 0 and NOT a.attisdropped AND pg_catalog.has_column_privilege(c.oid, a.attnum, 'select'::text)) s;

CREATE VIEW pg_stats AS
    SELECT
        nspname AS schemaname,
        relname AS tablename,
        attname AS attname,
        stainherit AS inherited,
        stanullfrac AS null_frac,
        stawidth AS avg_width,
        stadistinct AS n_distinct,
        stadndistinct As n_dndistinct,
        CASE
            WHEN stakind1 = 1 THEN stavalues1
            WHEN stakind2 = 1 THEN stavalues2
            WHEN stakind3 = 1 THEN stavalues3
            WHEN stakind4 = 1 THEN stavalues4
            WHEN stakind5 = 1 THEN stavalues5
        END AS most_common_vals,
        CASE
            WHEN stakind1 = 1 THEN stanumbers1
            WHEN stakind2 = 1 THEN stanumbers2
            WHEN stakind3 = 1 THEN stanumbers3
            WHEN stakind4 = 1 THEN stanumbers4
            WHEN stakind5 = 1 THEN stanumbers5
        END AS most_common_freqs,
        CASE
            WHEN stakind1 = 2 THEN stavalues1
            WHEN stakind2 = 2 THEN stavalues2
            WHEN stakind3 = 2 THEN stavalues3
            WHEN stakind4 = 2 THEN stavalues4
            WHEN stakind5 = 2 THEN stavalues5
        END AS histogram_bounds,
        CASE
            WHEN stakind1 = 3 THEN stanumbers1[1]
            WHEN stakind2 = 3 THEN stanumbers2[1]
            WHEN stakind3 = 3 THEN stanumbers3[1]
            WHEN stakind4 = 3 THEN stanumbers4[1]
            WHEN stakind5 = 3 THEN stanumbers5[1]
        END AS correlation,
        CASE
            WHEN stakind1 = 4 THEN stavalues1
            WHEN stakind2 = 4 THEN stavalues2
            WHEN stakind3 = 4 THEN stavalues3
            WHEN stakind4 = 4 THEN stavalues4
            WHEN stakind5 = 4 THEN stavalues5
        END AS most_common_elems,
        CASE
            WHEN stakind1 = 4 THEN stanumbers1
            WHEN stakind2 = 4 THEN stanumbers2
            WHEN stakind3 = 4 THEN stanumbers3
            WHEN stakind4 = 4 THEN stanumbers4
            WHEN stakind5 = 4 THEN stanumbers5
        END AS most_common_elem_freqs,
        CASE
            WHEN stakind1 = 5 THEN stanumbers1
            WHEN stakind2 = 5 THEN stanumbers2
            WHEN stakind3 = 5 THEN stanumbers3
            WHEN stakind4 = 5 THEN stanumbers4
            WHEN stakind5 = 5 THEN stanumbers5
        END AS elem_count_histogram
    FROM pg_statistic s JOIN pg_class c ON (c.oid = s.starelid AND s.starelkind='c')
         JOIN pg_attribute a ON (c.oid = attrelid AND attnum = s.staattnum)
         LEFT JOIN pg_namespace n ON (n.oid = c.relnamespace)
    WHERE NOT attisdropped AND pg_catalog.has_column_privilege(c.oid, a.attnum, 'select');

CREATE VIEW pg_catalog.pg_ext_stats AS
    SELECT
        nspname AS schemaname,
        relname AS tablename,
        stakey AS attname,
        stainherit AS inherited,
        stanullfrac AS null_frac,
        stawidth AS avg_width,
        stadistinct AS n_distinct,
        stadndistinct As n_dndistinct,
        CASE
            WHEN stakind1 = 1 THEN stavalues1
            WHEN stakind2 = 1 THEN stavalues2
            WHEN stakind3 = 1 THEN stavalues3
            WHEN stakind4 = 1 THEN stavalues4
            WHEN stakind5 = 1 THEN stavalues5
        END AS most_common_vals,
        CASE
            WHEN stakind1 = 1 THEN stanumbers1
            WHEN stakind2 = 1 THEN stanumbers2
            WHEN stakind3 = 1 THEN stanumbers3
            WHEN stakind4 = 1 THEN stanumbers4
            WHEN stakind5 = 1 THEN stanumbers5
        END AS most_common_freqs,
        CASE
            WHEN stakind1 = 6 THEN stavalues1
            WHEN stakind2 = 6 THEN stavalues2
            WHEN stakind3 = 6 THEN stavalues3
            WHEN stakind4 = 6 THEN stavalues4
            WHEN stakind5 = 6 THEN stavalues5
        END AS most_common_vals_null,
        CASE
            WHEN stakind1 = 6 THEN stanumbers1
            WHEN stakind2 = 6 THEN stanumbers2
            WHEN stakind3 = 6 THEN stanumbers3
            WHEN stakind4 = 6 THEN stanumbers4
            WHEN stakind5 = 6 THEN stanumbers5
        END AS most_common_freqs_null,
        CASE
            WHEN stakind1 = 2 THEN stavalues1
            WHEN stakind2 = 2 THEN stavalues2
            WHEN stakind3 = 2 THEN stavalues3
            WHEN stakind4 = 2 THEN stavalues4
            WHEN stakind5 = 2 THEN stavalues5
        END AS histogram_bounds
    FROM pg_statistic_ext s JOIN pg_class c ON (c.oid = s.starelid AND s.starelkind='c')
         LEFT JOIN pg_namespace n ON (n.oid = c.relnamespace);

REVOKE ALL on pg_statistic FROM public;
REVOKE ALL on pg_statistic_ext FROM public;

CREATE VIEW pg_locks AS
    SELECT * FROM pg_catalog.pg_lock_status() AS L;

CREATE VIEW pg_cursors AS
    SELECT * FROM pg_catalog.pg_cursor() AS C;

CREATE VIEW pg_available_extensions AS
    SELECT E.name, E.default_version, X.extversion AS installed_version,
           E.comment
      FROM pg_catalog.pg_available_extensions() AS E
           LEFT JOIN pg_extension AS X ON E.name = X.extname;

CREATE VIEW pg_available_extension_versions AS
    SELECT E.name, E.version, (X.extname IS NOT NULL) AS installed,
           E.superuser, E.relocatable, E.schema, E.requires, E.comment
      FROM pg_catalog.pg_available_extension_versions() AS E
           LEFT JOIN pg_extension AS X
             ON E.name = X.extname AND E.version = X.extversion;

CREATE VIEW pg_prepared_xacts AS
    SELECT P.transaction, P.gid, P.prepared,
           U.rolname AS owner, D.datname AS database
    FROM pg_catalog.pg_prepared_xact() AS P
         LEFT JOIN pg_authid U ON P.ownerid = U.oid
         LEFT JOIN pg_database D ON P.dbid = D.oid;

CREATE VIEW pg_prepared_statements AS
    SELECT * FROM pg_catalog.pg_prepared_statement() AS P;

CREATE VIEW pg_seclabels AS
SELECT
	l.objoid, l.classoid, l.objsubid,
	CASE WHEN rel.relkind = 'r' THEN 'table'::text
		 WHEN rel.relkind = 'v' THEN 'view'::text
		 WHEN rel.relkind = 'm' THEN 'materialized view'::text
		 WHEN rel.relkind = 'S' THEN 'sequence'::text
         WHEN rel.relkind = 'L' THEN 'large sequence'::text
		 WHEN rel.relkind = 'f' THEN 'foreign table'::text END AS objtype,
	rel.relnamespace AS objnamespace,
	CASE WHEN pg_catalog.pg_table_is_visible(rel.oid)
	     THEN pg_catalog.quote_ident(rel.relname)
	     ELSE pg_catalog.quote_ident(nsp.nspname) || '.' || pg_catalog.quote_ident(rel.relname)
	     END AS objname,
	l.provider, l.label
FROM
	pg_seclabel l
	JOIN pg_class rel ON l.classoid = rel.tableoid AND l.objoid = rel.oid
	JOIN pg_namespace nsp ON rel.relnamespace = nsp.oid
WHERE
	l.objsubid = 0
UNION ALL
SELECT
	l.objoid, l.classoid, l.objsubid,
	'column'::text AS objtype,
	rel.relnamespace AS objnamespace,
	CASE WHEN pg_catalog.pg_table_is_visible(rel.oid)
	     THEN pg_catalog.quote_ident(rel.relname)
	     ELSE pg_catalog.quote_ident(nsp.nspname) || '.' || pg_catalog.quote_ident(rel.relname)
	     END || '.' || att.attname AS objname,
	l.provider, l.label
FROM
	pg_seclabel l
	JOIN pg_class rel ON l.classoid = rel.tableoid AND l.objoid = rel.oid
	JOIN pg_attribute att
	     ON rel.oid = att.attrelid AND l.objsubid = att.attnum
	JOIN pg_namespace nsp ON rel.relnamespace = nsp.oid
WHERE
	l.objsubid != 0
UNION ALL
SELECT
	l.objoid, l.classoid, l.objsubid,
	CASE WHEN pro.proisagg = true THEN 'aggregate'::text
	     WHEN pro.proisagg = false THEN 'function'::text
	END AS objtype,
	pro.pronamespace AS objnamespace,
	CASE WHEN pg_catalog.pg_function_is_visible(pro.oid)
	     THEN pg_catalog.quote_ident(pro.proname)
	     ELSE pg_catalog.quote_ident(nsp.nspname) || '.' || pg_catalog.quote_ident(pro.proname)
	END || '(' || pg_catalog.pg_get_function_arguments(pro.oid) || ')' AS objname,
	l.provider, l.label
FROM
	pg_seclabel l
	JOIN pg_proc pro ON l.classoid = pro.tableoid AND l.objoid = pro.oid
	JOIN pg_namespace nsp ON pro.pronamespace = nsp.oid
WHERE
	l.objsubid = 0
UNION ALL
SELECT
	l.objoid, l.classoid, l.objsubid,
	CASE WHEN typ.typtype = 'd' THEN 'domain'::text
	ELSE 'type'::text END AS objtype,
	typ.typnamespace AS objnamespace,
	CASE WHEN pg_catalog.pg_type_is_visible(typ.oid)
	THEN pg_catalog.quote_ident(typ.typname)
	ELSE pg_catalog.quote_ident(nsp.nspname) || '.' || pg_catalog.quote_ident(typ.typname)
	END AS objname,
	l.provider, l.label
FROM
	pg_seclabel l
	JOIN pg_type typ ON l.classoid = typ.tableoid AND l.objoid = typ.oid
	JOIN pg_namespace nsp ON typ.typnamespace = nsp.oid
WHERE
	l.objsubid = 0
UNION ALL
SELECT
	l.objoid, l.classoid, l.objsubid,
	'large object'::text AS objtype,
	NULL::oid AS objnamespace,
	l.objoid::text AS objname,
	l.provider, l.label
FROM
	pg_seclabel l
	JOIN pg_largeobject_metadata lom ON l.objoid = lom.oid
WHERE
	l.classoid = 'pg_catalog.pg_largeobject'::regclass AND l.objsubid = 0
UNION ALL
SELECT
	l.objoid, l.classoid, l.objsubid,
	'language'::text AS objtype,
	NULL::oid AS objnamespace,
    pg_catalog.quote_ident(lan.lanname) AS objname,
	l.provider, l.label
FROM
	pg_seclabel l
	JOIN pg_language lan ON l.classoid = lan.tableoid AND l.objoid = lan.oid
WHERE
	l.objsubid = 0
UNION ALL
SELECT
	l.objoid, l.classoid, l.objsubid,
	'schema'::text AS objtype,
	nsp.oid AS objnamespace,
    pg_catalog.quote_ident(nsp.nspname) AS objname,
	l.provider, l.label
FROM
	pg_seclabel l
	JOIN pg_namespace nsp ON l.classoid = nsp.tableoid AND l.objoid = nsp.oid
WHERE
	l.objsubid = 0
UNION ALL
SELECT
	l.objoid, l.classoid, 0::int4 AS objsubid,
	'database'::text AS objtype,
	NULL::oid AS objnamespace,
    pg_catalog.quote_ident(dat.datname) AS objname,
	l.provider, l.label
FROM
	pg_shseclabel l
	JOIN pg_database dat ON l.classoid = dat.tableoid AND l.objoid = dat.oid
UNION ALL
SELECT
	l.objoid, l.classoid, 0::int4 AS objsubid,
	'tablespace'::text AS objtype,
	NULL::oid AS objnamespace,
    pg_catalog.quote_ident(spc.spcname) AS objname,
	l.provider, l.label
FROM
	pg_shseclabel l
	JOIN pg_tablespace spc ON l.classoid = spc.tableoid AND l.objoid = spc.oid
UNION ALL
SELECT
	l.objoid, l.classoid, 0::int4 AS objsubid,
	'role'::text AS objtype,
	NULL::oid AS objnamespace,
    pg_catalog.quote_ident(rol.rolname) AS objname,
	l.provider, l.label
FROM
	pg_shseclabel l
	JOIN pg_authid rol ON l.classoid = rol.tableoid AND l.objoid = rol.oid;

CREATE VIEW pg_settings AS
    SELECT * FROM pg_catalog.pg_show_all_settings() AS A;

CREATE RULE pg_settings_u AS
    ON UPDATE TO pg_settings
    WHERE new.name = old.name DO
    SELECT pg_catalog.set_config(old.name, new.setting, 'f');

CREATE RULE pg_settings_n AS
    ON UPDATE TO pg_settings
    DO INSTEAD NOTHING;

GRANT SELECT, UPDATE ON pg_settings TO PUBLIC;

CREATE VIEW pg_timezone_abbrevs AS
    SELECT * FROM pg_catalog.pg_timezone_abbrevs();

CREATE VIEW pg_timezone_names AS
    SELECT * FROM pg_catalog.pg_timezone_names();
    
CREATE VIEW pg_control_group_config AS
    SELECT * FROM pg_catalog.pg_control_group_config();

-- Statistics views

CREATE VIEW pg_stat_all_tables AS
    SELECT
            C.oid AS relid,
            N.nspname AS schemaname,
            C.relname AS relname,
            pg_catalog.pg_stat_get_numscans(C.oid) AS seq_scan,
            pg_catalog.pg_stat_get_tuples_returned(C.oid) AS seq_tup_read,
            pg_catalog.sum(pg_catalog.pg_stat_get_numscans(I.indexrelid))::bigint AS idx_scan,
            pg_catalog.sum(pg_catalog.pg_stat_get_tuples_fetched(I.indexrelid))::bigint +
            pg_catalog.pg_stat_get_tuples_fetched(C.oid) AS idx_tup_fetch,
            pg_catalog.pg_stat_get_tuples_inserted(C.oid) AS n_tup_ins,
            pg_catalog.pg_stat_get_tuples_updated(C.oid) AS n_tup_upd,
            pg_catalog.pg_stat_get_tuples_deleted(C.oid) AS n_tup_del,
            pg_catalog.pg_stat_get_tuples_hot_updated(C.oid) AS n_tup_hot_upd,
            pg_catalog.pg_stat_get_live_tuples(C.oid) AS n_live_tup,
            pg_catalog.pg_stat_get_dead_tuples(C.oid) AS n_dead_tup,
            pg_catalog.pg_stat_get_last_vacuum_time(C.oid) as last_vacuum,
            pg_catalog.pg_stat_get_last_autovacuum_time(C.oid) as last_autovacuum,
            pg_catalog.pg_stat_get_last_analyze_time(C.oid) as last_analyze,
            pg_catalog.pg_stat_get_last_autoanalyze_time(C.oid) as last_autoanalyze,
            pg_catalog.pg_stat_get_vacuum_count(C.oid) AS vacuum_count,
            pg_catalog.pg_stat_get_autovacuum_count(C.oid) AS autovacuum_count,
            pg_catalog.pg_stat_get_analyze_count(C.oid) AS analyze_count,
            pg_catalog.pg_stat_get_autoanalyze_count(C.oid) AS autoanalyze_count,
            pg_catalog.pg_stat_get_last_data_changed_time(C.oid) AS last_data_changed
    FROM pg_class C LEFT JOIN
         pg_index I ON C.oid = I.indrelid
         LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
    WHERE C.relkind IN ('r', 't', 'm')
    GROUP BY C.oid, N.nspname, C.relname;
	
CREATE VIEW pg_stat_xact_all_tables AS
    SELECT
            C.oid AS relid,
            N.nspname AS schemaname,
            C.relname AS relname,
            pg_catalog.pg_stat_get_xact_numscans(C.oid) AS seq_scan,
            pg_catalog.pg_stat_get_xact_tuples_returned(C.oid) AS seq_tup_read,
            pg_catalog.sum(pg_catalog.pg_stat_get_xact_numscans(I.indexrelid))::bigint AS idx_scan,
            pg_catalog.sum(pg_catalog.pg_stat_get_xact_tuples_fetched(I.indexrelid))::bigint +
            pg_catalog.pg_stat_get_xact_tuples_fetched(C.oid) AS idx_tup_fetch,
            pg_catalog.pg_stat_get_xact_tuples_inserted(C.oid) AS n_tup_ins,
            pg_catalog.pg_stat_get_xact_tuples_updated(C.oid) AS n_tup_upd,
            pg_catalog.pg_stat_get_xact_tuples_deleted(C.oid) AS n_tup_del,
            pg_catalog.pg_stat_get_xact_tuples_hot_updated(C.oid) AS n_tup_hot_upd
    FROM pg_class C LEFT JOIN
         pg_index I ON C.oid = I.indrelid
         LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
    WHERE C.relkind IN ('r', 't', 'm')
    GROUP BY C.oid, N.nspname, C.relname;

CREATE VIEW pg_stat_sys_tables AS
    SELECT * FROM pg_stat_all_tables
    WHERE schemaname IN ('pg_catalog', 'information_schema') OR
          schemaname ~ '^pg_toast';

CREATE VIEW pg_stat_xact_sys_tables AS
    SELECT * FROM pg_stat_xact_all_tables
    WHERE schemaname IN ('pg_catalog', 'information_schema') OR
          schemaname ~ '^pg_toast';

CREATE VIEW pg_stat_user_tables AS
    SELECT * FROM pg_stat_all_tables
    WHERE schemaname NOT IN ('pg_catalog', 'information_schema') AND
          schemaname !~ '^pg_toast';

CREATE VIEW pg_stat_xact_user_tables AS
    SELECT * FROM pg_stat_xact_all_tables
    WHERE schemaname NOT IN ('pg_catalog', 'information_schema') AND
          schemaname !~ '^pg_toast';

CREATE VIEW pg_statio_all_tables AS
    SELECT
            C.oid AS relid,
            N.nspname AS schemaname,
            C.relname AS relname,
            pg_catalog.pg_stat_get_blocks_fetched(C.oid) -
                    pg_catalog.pg_stat_get_blocks_hit(C.oid) AS heap_blks_read,
            pg_catalog.pg_stat_get_blocks_hit(C.oid) AS heap_blks_hit,
            pg_catalog.sum(pg_catalog.pg_stat_get_blocks_fetched(I.indexrelid) -
                pg_catalog.pg_stat_get_blocks_hit(I.indexrelid))::bigint AS idx_blks_read,
            pg_catalog.sum(pg_catalog.pg_stat_get_blocks_hit(I.indexrelid))::bigint AS idx_blks_hit,
            pg_catalog.pg_stat_get_blocks_fetched(T.oid) -
                    pg_catalog.pg_stat_get_blocks_hit(T.oid) AS toast_blks_read,
            pg_catalog.pg_stat_get_blocks_hit(T.oid) AS toast_blks_hit,
            pg_catalog.pg_stat_get_blocks_fetched(X.oid) -
                    pg_catalog.pg_stat_get_blocks_hit(X.oid) AS tidx_blks_read,
            pg_catalog.pg_stat_get_blocks_hit(X.oid) AS tidx_blks_hit
    FROM pg_class C LEFT JOIN
            pg_index I ON C.oid = I.indrelid LEFT JOIN
            pg_class T ON C.reltoastrelid = T.oid LEFT JOIN
            pg_class X ON T.reltoastidxid = X.oid
            LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
    WHERE C.relkind IN ('r', 't', 'm')
    GROUP BY C.oid, N.nspname, C.relname, T.oid, X.oid;

CREATE VIEW pg_statio_sys_tables AS
    SELECT * FROM pg_statio_all_tables
    WHERE schemaname IN ('pg_catalog', 'information_schema') OR
          schemaname ~ '^pg_toast';

CREATE VIEW pg_statio_user_tables AS
    SELECT * FROM pg_statio_all_tables
    WHERE schemaname NOT IN ('pg_catalog', 'information_schema') AND
          schemaname !~ '^pg_toast';

CREATE VIEW pg_stat_all_indexes AS
    SELECT
            C.oid AS relid,
            I.oid AS indexrelid,
            N.nspname AS schemaname,
            C.relname AS relname,
            I.relname AS indexrelname,
            pg_catalog.pg_stat_get_numscans(I.oid) AS idx_scan,
            pg_catalog.pg_stat_get_tuples_returned(I.oid) AS idx_tup_read,
            pg_catalog.pg_stat_get_tuples_fetched(I.oid) AS idx_tup_fetch
    FROM pg_class C JOIN
            pg_index X ON C.oid = X.indrelid JOIN
            pg_class I ON I.oid = X.indexrelid
            LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
    WHERE C.relkind IN ('r', 't', 'm');

CREATE VIEW pg_stat_sys_indexes AS
    SELECT * FROM pg_stat_all_indexes
    WHERE schemaname IN ('pg_catalog', 'information_schema') OR
          schemaname ~ '^pg_toast';

CREATE VIEW pg_stat_user_indexes AS
    SELECT * FROM pg_stat_all_indexes
    WHERE schemaname NOT IN ('pg_catalog', 'information_schema') AND
          schemaname !~ '^pg_toast';

CREATE VIEW pg_statio_all_indexes AS
    SELECT
            C.oid AS relid,
            I.oid AS indexrelid,
            N.nspname AS schemaname,
            C.relname AS relname,
            I.relname AS indexrelname,
            pg_catalog.pg_stat_get_blocks_fetched(I.oid) -
                    pg_catalog.pg_stat_get_blocks_hit(I.oid) AS idx_blks_read,
            pg_catalog.pg_stat_get_blocks_hit(I.oid) AS idx_blks_hit
    FROM pg_class C JOIN
            pg_index X ON C.oid = X.indrelid JOIN
            pg_class I ON I.oid = X.indexrelid
            LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
    WHERE C.relkind IN ('r', 't', 'm');

CREATE VIEW pg_statio_sys_indexes AS
    SELECT * FROM pg_statio_all_indexes
    WHERE schemaname IN ('pg_catalog', 'information_schema') OR
          schemaname ~ '^pg_toast';

CREATE VIEW pg_statio_user_indexes AS
    SELECT * FROM pg_statio_all_indexes
    WHERE schemaname NOT IN ('pg_catalog', 'information_schema') AND
          schemaname !~ '^pg_toast';

CREATE VIEW pg_statio_all_sequences AS
    SELECT
            C.oid AS relid,
            N.nspname AS schemaname,
            C.relname AS relname,
            pg_catalog.pg_stat_get_blocks_fetched(C.oid) -
                    pg_catalog.pg_stat_get_blocks_hit(C.oid) AS blks_read,
            pg_catalog.pg_stat_get_blocks_hit(C.oid) AS blks_hit
    FROM pg_class C
            LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
    WHERE C.relkind = 'S' or C.relkind = 'L';

CREATE VIEW pg_statio_sys_sequences AS
    SELECT * FROM pg_statio_all_sequences
    WHERE schemaname IN ('pg_catalog', 'information_schema') OR
          schemaname ~ '^pg_toast';

CREATE VIEW pg_statio_user_sequences AS
    SELECT * FROM pg_statio_all_sequences
    WHERE schemaname NOT IN ('pg_catalog', 'information_schema') AND
          schemaname !~ '^pg_toast';

CREATE OR REPLACE VIEW pg_catalog.pg_stat_activity AS
    SELECT
            S.datid AS datid,
            D.datname AS datname,
            S.pid,
            S.sessionid,
            S.usesysid,
            U.rolname AS usename,
            S.application_name,
            S.client_addr,
            S.client_hostname,
            S.client_port,
            S.backend_start,
            S.xact_start,
            S.query_start,
            S.state_change,
            S.waiting,
            S.enqueue,
            S.state,
            CASE
			WHEN S.srespool = 'unknown' THEN (U.rolrespool) :: name
			ELSE S.srespool
			END AS resource_pool,
            S.query_id,
            S.query,
            S.connection_info,
            S.unique_sql_id,
            S.trace_id
    FROM pg_database D, pg_catalog.pg_stat_get_activity_with_conninfo(NULL) AS S, pg_authid U
    WHERE S.datid = D.oid AND
            S.usesysid = U.oid;

CREATE OR REPLACE VIEW pg_catalog.pg_stat_activity_ng AS
    SELECT
            S.datid AS datid,
            D.datname AS datname,
            S.pid,
            S.sessionid,
            S.usesysid,
            U.rolname AS usename,
            S.application_name,
            S.client_addr,
            S.client_hostname,
            S.client_port,
            S.backend_start,
            S.xact_start,
            S.query_start,
            S.state_change,
            S.waiting,
            S.enqueue,
            S.state,
            CASE
                        WHEN S.srespool = 'unknown' THEN (U.rolrespool) :: name
                        ELSE S.srespool
                        END AS resource_pool,
            S.query_id,
            S.query,
            N.node_group
    FROM pg_database D, pg_catalog.pg_stat_get_activity(NULL) AS S, pg_catalog.pg_stat_get_activity_ng(NULL) AS N, pg_authid U
    WHERE S.datid = D.oid AND
            S.usesysid = U.oid AND
            S.sessionid = N.sessionid;

ALTER TEXT SEARCH CONFIGURATION pound ADD MAPPING
        FOR zh_words, en_word, numeric, alnum, grapsymbol, multisymbol
        WITH simple;

CREATE OR REPLACE VIEW pg_catalog.pg_session_wlmstat AS
    SELECT
            S.datid AS datid,
            D.datname AS datname,
            S.threadid,
            S.sessionid,
            S.threadpid AS processid,
            S.usesysid,
			S.appname,
            U.rolname AS usename,
            S.priority,
            S.attribute,
            S.block_time,
            S.elapsed_time,
            S.total_cpu_time,
			S.skew_percent AS cpu_skew_percent,
			S.statement_mem,
			S.active_points,
			S.dop_value,
            S.current_cgroup AS control_group,
            S.current_status AS status,
            S.enqueue_state AS enqueue,
            CASE
				WHEN T.session_respool = 'unknown' THEN (U.rolrespool) :: name
				ELSE T.session_respool
				END AS resource_pool,
            S.query,
            S.is_plana,
            S.node_group
    FROM pg_database D, pg_catalog.pg_stat_get_session_wlmstat(NULL) AS S, pg_authid AS U, pg_catalog.gs_wlm_session_respool(0) AS T
    WHERE S.datid = D.oid AND
            S.usesysid = U.oid AND
            T.sessionid = S.sessionid;

CREATE VIEW pg_wlm_statistics AS 
    SELECT 
            statement,
            block_time,
            elapsed_time,
            total_cpu_time,
            qualification_time,
            skew_percent AS cpu_skew_percent,
            control_group,
            status,
            action 
    FROM pg_catalog.pg_stat_get_wlm_statistics(NULL);
    
CREATE VIEW gs_session_memory_statistics AS
SELECT
        S.datid AS datid,
        S.usename,
        S.pid,
        S.query_start AS start_time,
        T.min_peak_memory,
        T.max_peak_memory,
        T.spill_info,
        S.query,
        S.node_group,
        T.top_mem_dn
FROM pg_stat_activity_ng AS S, pg_catalog.pg_stat_get_wlm_realtime_session_info(NULL) AS T
WHERE S.pid = T.threadid;
 
CREATE VIEW pg_session_iostat AS
    SELECT
        S.query_id,
        T.mincurr_iops as mincurriops,
        T.maxcurr_iops as maxcurriops,
        T.minpeak_iops as minpeakiops,
        T.maxpeak_iops as maxpeakiops,
        T.iops_limits as io_limits,
        CASE WHEN T.io_priority = 0 THEN 'None'::text
             WHEN T.io_priority = 10 THEN 'Low'::text
             WHEN T.io_priority = 20 THEN 'Medium'::text
             WHEN T.io_priority = 50 THEN 'High'::text END AS io_priority,
        S.query,
        S.node_group,
        T.curr_io_limits as curr_io_limits
FROM pg_stat_activity_ng AS S,  pg_catalog.pg_stat_get_wlm_session_iostat_info(0) AS T
WHERE S.pid = T.threadid;

CREATE VIEW gs_cluster_resource_info AS SELECT * FROM pg_catalog.pg_stat_get_wlm_node_resource_info(0);

CREATE VIEW gs_session_cpu_statistics AS
SELECT
        S.datid AS datid,
        S.usename,
        S.pid,
        S.query_start AS start_time,
        T.min_cpu_time,
        T.max_cpu_time,
        T.total_cpu_time,
        S.query,
        S.node_group,
        T.top_cpu_dn
FROM pg_stat_activity_ng AS S, pg_catalog.pg_stat_get_wlm_realtime_session_info(NULL) AS T
WHERE S.sessionid = T.threadid;

CREATE VIEW gs_wlm_session_statistics AS
SELECT
        S.datid AS datid,
        S.datname AS dbname,
        T.schemaname,
        T.nodename,
        S.usename AS username,
        S.application_name,
        S.client_addr,
        S.client_hostname,
        S.client_port,
        T.query_band,
        S.pid,
        S.sessionid,
        T.block_time,
        S.query_start AS start_time,
        T.duration,
        T.estimate_total_time,
        T.estimate_left_time,
        S.enqueue,
        S.resource_pool,
        T.control_group,
        T.estimate_memory,
        T.min_peak_memory,
        T.max_peak_memory,
        T.average_peak_memory,
        T.memory_skew_percent,
        T.spill_info,
        T.min_spill_size,
        T.max_spill_size,
        T.average_spill_size,
        T.spill_skew_percent,
        T.min_dn_time,
        T.max_dn_time,
        T.average_dn_time,
        T.dntime_skew_percent,
        T.min_cpu_time,
        T.max_cpu_time,
        T.total_cpu_time,
        T.cpu_skew_percent,
        T.min_peak_iops,
        T.max_peak_iops,
        T.average_peak_iops,
        T.iops_skew_percent,
        T.warning,
        S.query_id AS queryid,
        T.query,
        T.query_plan,
        S.node_group,
        T.top_cpu_dn,
        T.top_mem_dn
FROM pg_stat_activity_ng AS S, pg_catalog.pg_stat_get_wlm_realtime_session_info(NULL) AS T
WHERE S.pid = T.threadid;

CREATE OR REPLACE FUNCTION pg_catalog.gs_wlm_get_all_user_resource_info()
RETURNS setof record                    
AS $$                                                                      
DECLARE                                                                    
	row_data record;                                 
	row_name record;                                                    
	query_str text;                                                    
	query_str2 text;                                              
	BEGIN                                                                   
		query_str := 'SELECT rolname FROM pg_authid';
		FOR row_name IN EXECUTE(query_str) LOOP              
			query_str2 := 'SELECT * FROM pg_catalog.gs_wlm_user_resource_info(''' || row_name.rolname || ''')';
			FOR row_data IN EXECUTE(query_str2) LOOP          
				return next row_data;
			END LOOP;                                          
		END LOOP;                                                  
		return;                                                    
	END; $$                                                            
LANGUAGE 'plpgsql' NOT FENCED;

CREATE VIEW pg_total_user_resource_info_oid AS
SELECT * FROM pg_catalog.gs_wlm_get_all_user_resource_info() AS
(userid Oid, 
 used_memory int, 
 total_memory int, 
 used_cpu float,
 total_cpu int,
 used_space bigint,
 total_space bigint,
 used_temp_space bigint,
 total_temp_space bigint,
 used_spill_space bigint,
 total_spill_space bigint,
 read_kbytes bigint,
 write_kbytes bigint,
 read_counts bigint,
 write_counts bigint,
 read_speed float,
 write_speed float
);

create view pg_total_user_resource_info AS 
SELECT
    S.usename AS username,
    T.used_memory,
    T.total_memory,
    T.used_cpu,
    T.total_cpu,
    T.used_space,
    T.total_space,
    T.used_temp_space,
    T.total_temp_space,
    T.used_spill_space,
    T.total_spill_space,
    T.read_kbytes,
    T.write_kbytes,
    T.read_counts,
    T.write_counts,
    T.read_speed,
    T.write_speed
FROM pg_user AS S, pg_total_user_resource_info_oid AS T
WHERE S.usesysid = T.userid;

create table gs_wlm_user_resource_history
(
    username text,
    timestamp timestamp with time zone DEFAULT now(),
    used_memory int,
    total_memory int,
    used_cpu float(2),
    total_cpu int,
    used_space bigint,
    total_space bigint,
    used_temp_space bigint,
    total_temp_space bigint,
    used_spill_space bigint,
    total_spill_space bigint,
    read_kbytes bigint,
    write_kbytes bigint,
    read_counts bigint,
    write_counts bigint,
    read_speed float(2),
    write_speed float(2)
);

REVOKE all on gs_wlm_user_resource_history FROM public;

CREATE OR REPLACE FUNCTION pg_catalog.gs_wlm_persistent_user_resource_info()
RETURNS setof record
AS $$
DECLARE
        query_str text;
        insert_str text;
        row_data record;
        BEGIN
            query_str := 'SELECT * FROM pg_total_user_resource_info';
        FOR row_data IN EXECUTE(query_str) LOOP
            insert_str := 'INSERT INTO gs_wlm_user_resource_history values (''' || row_data.username || ''', CURRENT_TIMESTAMP, ' || row_data.used_memory || ', ' || row_data.total_memory || ', ' ||
             row_data.used_cpu || ', ' || row_data.total_cpu || ',' ||  row_data.used_space || ',' || row_data.total_space || ',' || row_data.used_temp_space || ',' || row_data.total_temp_space || ',' ||
             row_data.used_spill_space || ',' || row_data.total_spill_space || ',' || row_data.read_kbytes || ',' || row_data.write_kbytes || ',' || row_data.read_counts || ',' || row_data.write_counts || ',' ||
             row_data.read_speed || ',' || row_data.write_speed || ')';

            EXECUTE(insert_str);
        END LOOP;
        return;
        END; $$
LANGUAGE plpgsql NOT FENCED;

create table gs_wlm_instance_history
(
	instancename	text,
	timestamp    	timestamp with time zone,
	used_cpu    	int,
	free_mem    	int,
	used_mem        int,
	io_await        float(2),
	io_util         float(2),
	disk_read   	float(2),
	disk_write      float(2),
	process_read    bigint,
	process_write   bigint,
	logical_read    bigint,
	logical_write   bigint,
	read_counts  bigint,
	write_counts bigint
);

REVOKE ALL on gs_wlm_instance_history FROM public;

CREATE OR REPLACE FUNCTION pg_catalog.create_wlm_instance_statistics_info()
RETURNS int
AS $$
DECLARE
	query_str text;
	record_cnt int;
	BEGIN
	    record_cnt := 0;
		query_str := 'SELECT * FROM pg_catalog.pg_stat_get_wlm_instance_info_with_cleanup()';
        EXECUTE 'INSERT INTO gs_wlm_instance_history ' || query_str;
        return record_cnt;
	END; $$
LANGUAGE plpgsql NOT FENCED;

create table gs_wlm_session_query_info_all
(
	datid	     		Oid,
	dbname	     		text,
	schemaname    		text,
	nodename    		text,
	username    		text,
	application_name	text,
	client_addr    		inet,
	client_hostname 	text,
	client_port 		int,
	query_band    		text,
	block_time    		bigint,
	start_time   		timestamp with time zone,
	finish_time   		timestamp with time zone,
	duration      		bigint,
	estimate_total_time	bigint,
	status      		text,
	abort_info  		text,
	resource_pool 		text,
	control_group 		text,
	estimate_memory		int,
	min_peak_memory		int,
	max_peak_memory		int,
	average_peak_memory	int,
	memory_skew_percent	int,
	spill_info  		text,
	min_spill_size		int,
	max_spill_size		int,
	average_spill_size	int,
	spill_skew_percent	int,
	min_dn_time	    	bigint,
	max_dn_time	    	bigint,
	average_dn_time		bigint,
	dntime_skew_percent	int,
	min_cpu_time		bigint,
	max_cpu_time		bigint,
	total_cpu_time  	bigint,
	cpu_skew_percent	int,
	min_peak_iops		int,
	max_peak_iops		int,
	average_peak_iops	int,
	iops_skew_percent	int,
	warning	    		text,
	queryid      		bigint NOT NULL,
	query       		text,
	query_plan	    	text,
	node_group		text,
	cpu_top1_node_name text,
	cpu_top2_node_name text,
	cpu_top3_node_name text,
	cpu_top4_node_name text,
	cpu_top5_node_name text,
	mem_top1_node_name text,
	mem_top2_node_name text,
	mem_top3_node_name text,
	mem_top4_node_name text,
	mem_top5_node_name text,
	cpu_top1_value bigint,
	cpu_top2_value bigint,
	cpu_top3_value bigint,
	cpu_top4_value bigint,
	cpu_top5_value bigint,
	mem_top1_value bigint,
	mem_top2_value bigint,
	mem_top3_value bigint,
	mem_top4_value bigint,
	mem_top5_value bigint,
	top_mem_dn text,
    top_cpu_dn text,
    n_returned_rows      bigint,
    n_tuples_fetched     bigint,
    n_tuples_returned    bigint,
    n_tuples_inserted    bigint,
    n_tuples_updated     bigint,
    n_tuples_deleted     bigint,
    n_blocks_fetched     bigint,
    n_blocks_hit         bigint,
    db_time              bigint,
    cpu_time             bigint,
    execution_time       bigint,
    parse_time           bigint,
    plan_time            bigint,
    rewrite_time         bigint,
    pl_execution_time    bigint,
    pl_compilation_time  bigint,
    net_send_time        bigint,
    data_io_time         bigint,
    is_slow_query        bigint
);

CREATE VIEW gs_wlm_session_info_all AS
SELECT * FROM pg_catalog.pg_stat_get_wlm_session_info(0);

CREATE VIEW gs_wlm_session_info AS
SELECT
        S.datid,
        S.dbname,
        S.schemaname,
        S.nodename,
        S.username,
        S.application_name,
        S.client_addr,
        S.client_hostname,
        S.client_port,
        S.query_band,
        S.block_time,
        S.start_time,
        S.finish_time,
        S.duration,
        S.estimate_total_time,
        S.status,
        S.abort_info,
        S.resource_pool,
        S.control_group,
        S.estimate_memory,
        S.min_peak_memory,
        S.max_peak_memory,
        S.average_peak_memory,
        S.memory_skew_percent,
        S.spill_info,
        S.min_spill_size,
        S.max_spill_size,
        S.average_spill_size,
        S.spill_skew_percent,
        S.min_dn_time,
        S.max_dn_time,
        S.average_dn_time,
        S.dntime_skew_percent,
        S.min_cpu_time,
        S.max_cpu_time,
        S.total_cpu_time,
        S.cpu_skew_percent,
        S.min_peak_iops,
        S.max_peak_iops,
        S.average_peak_iops,
        S.iops_skew_percent,
        S.warning,
        S.queryid,
        S.query,
        S.query_plan,
        S.node_group,
        S.cpu_top1_node_name,
        S.cpu_top2_node_name,
        S.cpu_top3_node_name,
        S.cpu_top4_node_name,
        S.cpu_top5_node_name,
        S.mem_top1_node_name,
        S.mem_top2_node_name,
        S.mem_top3_node_name,
        S.mem_top4_node_name,
        S.mem_top5_node_name,
        S.cpu_top1_value,
        S.cpu_top2_value,
        S.cpu_top3_value,
        S.cpu_top4_value,
        S.cpu_top5_value,
        S.mem_top1_value,
        S.mem_top2_value,
        S.mem_top3_value,
        S.mem_top4_value,
        S.mem_top5_value,
        S.top_mem_dn,
        S.top_cpu_dn
FROM gs_wlm_session_query_info_all S;

CREATE VIEW gs_wlm_session_history AS
SELECT
        S.datid,
        S.dbname,
        S.schemaname,
        S.nodename,
        S.username,
        S.application_name,
        S.client_addr,
        S.client_hostname,
        S.client_port,
        S.query_band,
        S.block_time,
        S.start_time,
        S.finish_time,
        S.duration,
        S.estimate_total_time,
        S.status,
        S.abort_info,
        S.resource_pool,
        S.control_group,
        S.estimate_memory,
        S.min_peak_memory,
        S.max_peak_memory,
        S.average_peak_memory,
        S.memory_skew_percent,
        S.spill_info,
        S.min_spill_size,
        S.max_spill_size,
        S.average_spill_size,
        S.spill_skew_percent,
        S.min_dn_time,
        S.max_dn_time,
        S.average_dn_time,
        S.dntime_skew_percent,
        S.min_cpu_time,
        S.max_cpu_time,
        S.total_cpu_time,
        S.cpu_skew_percent,
        S.min_peak_iops,
        S.max_peak_iops,
        S.average_peak_iops,
        S.iops_skew_percent,
        S.warning,
        S.queryid,
        S.query,
        S.query_plan,
        S.node_group,
        S.cpu_top1_node_name,
        S.cpu_top2_node_name,
        S.cpu_top3_node_name,
        S.cpu_top4_node_name,
        S.cpu_top5_node_name,
        S.mem_top1_node_name,
        S.mem_top2_node_name,
        S.mem_top3_node_name,
        S.mem_top4_node_name,
        S.mem_top5_node_name,
        S.cpu_top1_value,
        S.cpu_top2_value,
        S.cpu_top3_value,
        S.cpu_top4_value,
        S.cpu_top5_value,
        S.mem_top1_value,
        S.mem_top2_value,
        S.mem_top3_value,
        S.mem_top4_value,
        S.mem_top5_value,
        S.top_mem_dn,
        S.top_cpu_dn
FROM gs_wlm_session_info_all S;



CREATE OR REPLACE FUNCTION pg_catalog.create_wlm_session_info(IN flag int)
RETURNS int
AS $$
DECLARE
	query_str text;
	record_cnt int;
	BEGIN
		record_cnt := 0;
		
		query_str := 'SELECT * FROM pg_catalog.pg_stat_get_wlm_session_info(1)';
		
		IF flag > 0 THEN
			EXECUTE 'INSERT INTO gs_wlm_session_query_info_all ' || query_str;
		ELSE
			EXECUTE query_str;
		END IF;
		
		RETURN record_cnt;
	END; $$
LANGUAGE plpgsql NOT FENCED;

CREATE VIEW gs_wlm_cgroup_info AS
    SELECT
            cgroup_name,
            percent AS priority,
            usage_percent AS usage_percent,
            shares,
            usage AS cpuacct,
            cpuset,
            relpath,
            valid,
            node_group
    FROM pg_catalog.pg_stat_get_cgroup_info(NULL);
	
CREATE VIEW gs_wlm_user_info AS
SELECT
		T.userid,
		S.rolname AS username,
		T.sysadmin,
		T.rpoid,
		R.respool_name AS respool,
		T.parentid,
		T.totalspace,
		T.spacelimit,
		T.childcount,
		T.childlist
FROM pg_roles AS S, pg_catalog.gs_wlm_get_user_info(NULL) AS T, pg_resource_pool AS R
WHERE S.oid = T.userid AND T.rpoid = R.oid;

CREATE VIEW gs_wlm_resource_pool AS
SELECT
		T.respool_oid AS rpoid,
		R.respool_name AS respool,
		R.control_group AS control_group,
		R.parentid AS parentid,
		T.ref_count,
		T.active_points,
		T.running_count,
		T.waiting_count,
		T.iops_limits as io_limits,
		T.io_priority
FROM pg_catalog.gs_wlm_get_resource_pool_info(0) AS T, pg_resource_pool AS R
WHERE T.respool_oid = R.oid;

CREATE VIEW gs_wlm_rebuild_user_resource_pool AS
	SELECT * FROM pg_catalog.gs_wlm_rebuild_user_resource_pool(0);
	
CREATE VIEW gs_wlm_workload_records AS
    SELECT
			P.node_name,
            S.threadid AS thread_id,
            S.threadpid AS processid,
			P.start_time AS time_stamp,
            U.rolname AS username,
			P.memory,
			P.actpts AS active_points,
			P.maxpts AS max_points,
			P.priority,
			P.resource_pool,
			S.current_status AS status,
			S.current_cgroup AS control_group,
			P.queue_type AS enqueue,
            S.query,
            P.node_group
    FROM pg_catalog.pg_stat_get_session_wlmstat(NULL) AS S, pg_authid U, pg_catalog.gs_wlm_get_workload_records(0) P
    WHERE P.query_pid = S.threadpid AND
            S.usesysid = U.oid;	

CREATE VIEW gs_os_run_info AS SELECT * FROM pg_catalog.pv_os_run_info();
CREATE VIEW gs_session_memory_context AS SELECT * FROM pg_catalog.pv_session_memory_detail();
CREATE VIEW gs_thread_memory_context AS SELECT * FROM pg_catalog.pv_thread_memory_detail();
CREATE VIEW gs_shared_memory_detail AS SELECT * FROM pg_catalog.pg_shared_memory_detail();
CREATE VIEW gs_instance_time AS SELECT * FROM pg_catalog.pv_instance_time();
CREATE VIEW gs_session_time AS SELECT * FROM pg_catalog.pv_session_time();
CREATE VIEW gs_session_memory AS SELECT * FROM pg_catalog.pv_session_memory();
CREATE VIEW gs_total_memory_detail AS SELECT * FROM pg_catalog.pv_total_memory_detail();
CREATE VIEW pg_total_memory_detail AS SELECT * FROM pg_catalog.pv_total_memory_detail();
CREATE VIEW gs_redo_stat AS SELECT * FROM pg_catalog.pg_stat_get_redo_stat();
CREATE VIEW gs_session_stat AS SELECT * FROM pg_catalog.pv_session_stat();
CREATE VIEW gs_file_stat AS SELECT * FROM pg_catalog.pg_stat_get_file_stat();

CREATE OR REPLACE FUNCTION pg_catalog.gs_session_memory_detail_tp(OUT sessid TEXT, OUT sesstype TEXT, OUT contextname TEXT, OUT level INT2, OUT parent TEXT, OUT totalsize INT8, OUT freesize INT8, OUT usedsize INT8)
RETURNS setof record
AS $$
DECLARE
  enable_threadpool bool;
  row_data record;
  query_str text;
BEGIN
  show enable_thread_pool into enable_threadpool;

  IF enable_threadpool THEN
    query_str := 'with SM AS
                   (SELECT
                      S.sessid AS sessid,
                      T.thrdtype AS sesstype,
                      S.contextname AS contextname,
                      S.level AS level,
                      S.parent AS parent,
                      S.totalsize AS totalsize,
                      S.freesize AS freesize,
                      S.usedsize AS usedsize
                    FROM
                      gs_session_memory_context S
                      LEFT JOIN
                     (SELECT DISTINCT thrdtype, tid
                      FROM gs_thread_memory_context) T
                      on S.threadid = T.tid
                   ),
                   TM AS
                   (SELECT
                      S.sessid AS Ssessid,
                      T.thrdtype AS sesstype,
                      T.threadid AS Tsessid,
                      T.contextname AS contextname,
                      T.level AS level,
                      T.parent AS parent,
                      T.totalsize AS totalsize,
                      T.freesize AS freesize,
                      T.usedsize AS usedsize
                    FROM
                      gs_thread_memory_context T
                      LEFT JOIN
                      (SELECT DISTINCT sessid, threadid
                       FROM gs_session_memory_context) S
                      ON T.tid = S.threadid
                   )
                   SELECT * from SM
                   UNION
                   SELECT 
                     Ssessid AS sessid, sesstype, contextname, level, parent, totalsize, freesize, usedsize
                   FROM TM WHERE Ssessid IS NOT NULL
                   UNION
                   SELECT
                     Tsessid AS sessid, sesstype, contextname, level, parent, totalsize, freesize, usedsize
                   FROM TM WHERE Ssessid IS NULL;';
    FOR row_data IN EXECUTE(query_str) LOOP
      sessid = row_data.sessid;
      sesstype = row_data.sesstype;
      contextname = row_data.contextname;
      level = row_data.level;
      parent = row_data.parent;
      totalsize = row_data.totalsize;
      freesize = row_data.freesize;
      usedsize = row_data.usedsize;
      return next;
    END LOOP;
  ELSE
    query_str := 'SELECT
                    T.threadid AS sessid,
                    T.thrdtype AS sesstype,
                    T.contextname AS contextname,
                    T.level AS level,
                    T.parent AS parent,
                    T.totalsize AS totalsize,
                    T.freesize AS freesize,
                    T.usedsize AS usedsize
                  FROM pg_catalog.pv_thread_memory_detail() T;';
    FOR row_data IN EXECUTE(query_str) LOOP
      sessid = row_data.sessid;
      sesstype = row_data.sesstype;
      contextname = row_data.contextname;
      level = row_data.level;
      parent = row_data.parent;
      totalsize = row_data.totalsize;
      freesize = row_data.freesize;
      usedsize = row_data.usedsize;
      return next;
    END LOOP;
  END IF;
  RETURN;
END; $$
LANGUAGE plpgsql NOT FENCED;

CREATE VIEW gs_session_memory_detail AS SELECT * FROM pg_catalog.gs_session_memory_detail_tp() ORDER BY sessid;

CREATE VIEW pg_stat_replication AS
    SELECT
            S.pid,
            S.usesysid,
            U.rolname AS usename,
            S.application_name,
            S.client_addr,
            S.client_hostname,
            S.client_port,
            S.backend_start,
            W.state,
            W.sender_sent_location,
            W.receiver_write_location,
            W.receiver_flush_location,
            W.receiver_replay_location,
            W.sync_priority,
            W.sync_state
    FROM pg_catalog.pg_stat_get_activity(NULL) AS S, pg_authid U,
         pg_catalog.pg_stat_get_wal_senders() AS W
    WHERE S.usesysid = U.oid AND
            S.pid = W.pid;
            
CREATE VIEW pg_replication_slots AS
    SELECT
            L.slot_name,
            L.plugin,
            L.slot_type,
            L.datoid,
            D.datname AS database,
            L.active,
            L.xmin,
            L.catalog_xmin,
            L.restart_lsn,
            L.dummy_standby
    FROM pg_catalog.pg_get_replication_slots() AS L
            LEFT JOIN pg_database D ON (L.datoid = D.oid);


CREATE VIEW pg_stat_database AS
    SELECT
            D.oid AS datid,
            D.datname AS datname,
            pg_catalog.pg_stat_get_db_numbackends(D.oid) AS numbackends,
            pg_catalog.pg_stat_get_db_xact_commit(D.oid) AS xact_commit,
            pg_catalog.pg_stat_get_db_xact_rollback(D.oid) AS xact_rollback,
            pg_catalog.pg_stat_get_db_blocks_fetched(D.oid) -
                    pg_catalog.pg_stat_get_db_blocks_hit(D.oid) AS blks_read,
            pg_catalog.pg_stat_get_db_blocks_hit(D.oid) AS blks_hit,
            pg_catalog.pg_stat_get_db_tuples_returned(D.oid) AS tup_returned,
            pg_catalog.pg_stat_get_db_tuples_fetched(D.oid) AS tup_fetched,
            pg_catalog.pg_stat_get_db_tuples_inserted(D.oid) AS tup_inserted,
            pg_catalog.pg_stat_get_db_tuples_updated(D.oid) AS tup_updated,
            pg_catalog.pg_stat_get_db_tuples_deleted(D.oid) AS tup_deleted,
            pg_catalog.pg_stat_get_db_conflict_all(D.oid) AS conflicts,
            pg_catalog.pg_stat_get_db_temp_files(D.oid) AS temp_files,
            pg_catalog.pg_stat_get_db_temp_bytes(D.oid) AS temp_bytes,
            pg_catalog.pg_stat_get_db_deadlocks(D.oid) AS deadlocks,
            pg_catalog.pg_stat_get_db_blk_read_time(D.oid) AS blk_read_time,
            pg_catalog.pg_stat_get_db_blk_write_time(D.oid) AS blk_write_time,
            pg_catalog.pg_stat_get_db_stat_reset_time(D.oid) AS stats_reset
    FROM pg_database D;

CREATE VIEW pg_stat_database_conflicts AS
    SELECT
            D.oid AS datid,
            D.datname AS datname,
            pg_catalog.pg_stat_get_db_conflict_tablespace(D.oid) AS confl_tablespace,
            pg_catalog.pg_stat_get_db_conflict_lock(D.oid) AS confl_lock,
            pg_catalog.pg_stat_get_db_conflict_snapshot(D.oid) AS confl_snapshot,
            pg_catalog.pg_stat_get_db_conflict_bufferpin(D.oid) AS confl_bufferpin,
            pg_catalog.pg_stat_get_db_conflict_startup_deadlock(D.oid) AS confl_deadlock
    FROM pg_database D;

CREATE VIEW pg_stat_user_functions AS
    SELECT
            P.oid AS funcid,
            N.nspname AS schemaname,
            P.proname AS funcname,
            pg_catalog.pg_stat_get_function_calls(P.oid) AS calls,
            pg_catalog.pg_stat_get_function_total_time(P.oid) AS total_time,
            pg_catalog.pg_stat_get_function_self_time(P.oid) AS self_time
    FROM pg_proc P LEFT JOIN pg_namespace N ON (N.oid = P.pronamespace)
    WHERE P.prolang != 12  -- fast check to eliminate built-in functions
          AND pg_catalog.pg_stat_get_function_calls(P.oid) IS NOT NULL;

CREATE VIEW pg_stat_xact_user_functions AS
    SELECT
            P.oid AS funcid,
            N.nspname AS schemaname,
            P.proname AS funcname,
            pg_catalog.pg_stat_get_xact_function_calls(P.oid) AS calls,
            pg_catalog.pg_stat_get_xact_function_total_time(P.oid) AS total_time,
            pg_catalog.pg_stat_get_xact_function_self_time(P.oid) AS self_time
    FROM pg_proc P LEFT JOIN pg_namespace N ON (N.oid = P.pronamespace)
    WHERE P.prolang != 12  -- fast check to eliminate built-in functions
          AND pg_catalog.pg_stat_get_xact_function_calls(P.oid) IS NOT NULL;

CREATE VIEW pg_stat_bgwriter AS
    SELECT
        pg_catalog.pg_stat_get_bgwriter_timed_checkpoints() AS checkpoints_timed,
        pg_catalog.pg_stat_get_bgwriter_requested_checkpoints() AS checkpoints_req,
        pg_catalog.pg_stat_get_checkpoint_write_time() AS checkpoint_write_time,
        pg_catalog.pg_stat_get_checkpoint_sync_time() AS checkpoint_sync_time,
        pg_catalog.pg_stat_get_bgwriter_buf_written_checkpoints() AS buffers_checkpoint,
        pg_catalog.pg_stat_get_bgwriter_buf_written_clean() AS buffers_clean,
        pg_catalog.pg_stat_get_bgwriter_maxwritten_clean() AS maxwritten_clean,
        pg_catalog.pg_stat_get_buf_written_backend() AS buffers_backend,
        pg_catalog.pg_stat_get_buf_fsync_backend() AS buffers_backend_fsync,
        pg_catalog.pg_stat_get_buf_alloc() AS buffers_alloc,
        pg_catalog.pg_stat_get_bgwriter_stat_reset_time() AS stats_reset;

CREATE VIEW pg_user_mappings AS
    SELECT
        U.oid       AS umid,
        S.oid       AS srvid,
        S.srvname   AS srvname,
        U.umuser    AS umuser,
        CASE WHEN U.umuser = 0 THEN
            'public'
        ELSE
            A.rolname
        END AS usename,
        CASE WHEN pg_catalog.pg_has_role(S.srvowner, 'USAGE') OR pg_catalog.has_server_privilege(S.oid, 'USAGE') THEN
            U.umoptions
        ELSE
            NULL
        END AS umoptions
    FROM pg_user_mapping U
         LEFT JOIN pg_authid A ON (A.oid = U.umuser) JOIN
        pg_foreign_server S ON (U.umserver = S.oid);

REVOKE ALL on pg_user_mapping FROM public;

-- these functions are added for supporting default format transformation
CREATE OR REPLACE FUNCTION pg_catalog.to_char(NUMERIC)
RETURNS VARCHAR2
AS $$ SELECT CAST(pg_catalog.numeric_out($1) AS VARCHAR2) $$
LANGUAGE SQL  STRICT IMMUTABLE NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.to_char(INT2)
RETURNS VARCHAR2
AS $$ SELECT CAST(pg_catalog.int2out($1) AS VARCHAR2) $$
LANGUAGE SQL  STRICT IMMUTABLE NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.to_char(INT4)
RETURNS VARCHAR2
AS $$  SELECT CAST(pg_catalog.int4out($1) AS VARCHAR2) $$
LANGUAGE SQL  STRICT IMMUTABLE NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.to_char(INT8)
RETURNS VARCHAR2
AS $$ SELECT CAST(pg_catalog.int8out($1) AS VARCHAR2) $$
LANGUAGE SQL  STRICT IMMUTABLE NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.to_char(FLOAT4)
RETURNS VARCHAR2
AS $$ SELECT CAST(pg_catalog.float4out($1) AS VARCHAR2) $$
LANGUAGE SQL  STRICT IMMUTABLE NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.to_char(FLOAT8)
RETURNS VARCHAR2
AS $$ SELECT CAST(pg_catalog.float8out($1) AS VARCHAR2) $$
LANGUAGE SQL  STRICT IMMUTABLE NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.to_char(TEXT)
RETURNS TEXT
AS $$ SELECT $1 $$
LANGUAGE SQL  STRICT IMMUTABLE NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.to_number(TEXT)
RETURNS NUMERIC
AS $$ SELECT pg_catalog.numeric_in(pg_catalog.textout($1), 0::Oid, -1) $$
LANGUAGE SQL  STRICT IMMUTABLE NOT FENCED;

CREATE CAST (VARCHAR2 AS RAW) WITH FUNCTION pg_catalog.hextoraw(text) AS IMPLICIT;

--
-- We have a few function definitions in here, too.
-- At some point there might be enough to justify breaking them out into
-- a separate "system_functions.sql" file.
--

-- Tsearch debug function.  Defined here because it'd be pretty unwieldy
-- to put it into pg_proc.h

CREATE FUNCTION pg_catalog.ts_debug(IN config regconfig, IN document text,
    OUT alias text,
    OUT description text,
    OUT token text,
    OUT dictionaries regdictionary[],
    OUT dictionary regdictionary,
    OUT lexemes text[])
RETURNS SETOF record AS
$$
SELECT
    tt.alias AS alias,
    tt.description AS description,
    parse.token AS token,
    ARRAY ( SELECT m.mapdict::pg_catalog.regdictionary
            FROM pg_catalog.pg_ts_config_map AS m
            WHERE m.mapcfg = $1 AND m.maptokentype = parse.tokid
            ORDER BY m.mapseqno )
    AS dictionaries,
    ( SELECT mapdict::pg_catalog.regdictionary
      FROM pg_catalog.pg_ts_config_map AS m
      WHERE m.mapcfg = $1 AND m.maptokentype = parse.tokid
      ORDER BY pg_catalog.ts_lexize(mapdict, parse.token) IS NULL, m.mapseqno
      LIMIT 1
    ) AS dictionary,
    ( SELECT pg_catalog.ts_lexize(mapdict, parse.token)
      FROM pg_catalog.pg_ts_config_map AS m
      WHERE m.mapcfg = $1 AND m.maptokentype = parse.tokid
      ORDER BY pg_catalog.ts_lexize(mapdict, parse.token) IS NULL, m.mapseqno
      LIMIT 1
    ) AS lexemes
FROM pg_catalog.ts_parse(
        (SELECT cfgparser FROM pg_catalog.pg_ts_config WHERE oid = $1 ), $2
    ) AS parse,
     pg_catalog.ts_token_type(
        (SELECT cfgparser FROM pg_catalog.pg_ts_config WHERE oid = $1 )
    ) AS tt
WHERE tt.tokid = parse.tokid
$$
LANGUAGE SQL STRICT STABLE NOT FENCED;

COMMENT ON FUNCTION pg_catalog.ts_debug(regconfig,text) IS
    'debug function for text search configuration';

CREATE FUNCTION pg_catalog.ts_debug(IN document text,
    OUT alias text,
    OUT description text,
    OUT token text,
    OUT dictionaries regdictionary[],
    OUT dictionary regdictionary,
    OUT lexemes text[])
RETURNS SETOF record AS
$$
    SELECT * FROM pg_catalog.ts_debug( pg_catalog.get_current_ts_config(), $1);
$$
LANGUAGE SQL STRICT STABLE NOT FENCED;

COMMENT ON FUNCTION pg_catalog.ts_debug(text) IS
    'debug function for current text search configuration';

--
-- Redeclare built-in functions that need default values attached to their
-- arguments.  It's impractical to set those up directly in pg_proc.h because
-- of the complexity and platform-dependency of the expression tree
-- representation.  (Note that internal functions still have to have entries
-- in pg_proc.h; we are merely causing their proargnames and proargdefaults
-- to get filled in.)
--

CREATE OR REPLACE FUNCTION pg_catalog.TO_TEXT(INT2)
RETURNS TEXT
AS $$ select CAST(pg_catalog.int2out($1) AS VARCHAR)  $$
LANGUAGE SQL  STRICT IMMUTABLE NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.TO_TEXT(INT4)
RETURNS TEXT
AS $$  select CAST(pg_catalog.int4out($1) AS VARCHAR)  $$
LANGUAGE SQL  STRICT IMMUTABLE NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.TO_TEXT(INT8)
RETURNS TEXT
AS $$ select CAST(pg_catalog.int8out($1) AS VARCHAR) $$
LANGUAGE SQL  STRICT IMMUTABLE NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.TO_TEXT(FLOAT4)
RETURNS TEXT
AS $$ select CAST(pg_catalog.float4out($1) AS VARCHAR) $$
LANGUAGE SQL  STRICT IMMUTABLE NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.TO_TEXT(FLOAT8)
RETURNS TEXT
AS $$ select CAST(pg_catalog.float8out($1) AS VARCHAR) $$
LANGUAGE SQL  STRICT IMMUTABLE NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.TO_TEXT(NUMERIC)
RETURNS TEXT
AS $$ SELECT CAST(pg_catalog.numeric_out($1) AS VARCHAR) $$
LANGUAGE SQL  STRICT IMMUTABLE NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.TO_TEXT(INTERVAL)
RETURNS TEXT
AS $$  select CAST(pg_catalog.interval_out($1) AS TEXT) $$
LANGUAGE SQL IMMUTABLE STRICT NOT FENCED;
--logical decoding

CREATE CAST (INTERVAL AS TEXT) WITH FUNCTION
pg_catalog.TO_TEXT(INTERVAL) AS IMPLICIT;

create or replace function pg_catalog.to_number(text)
returns numeric
AS $$ select pg_catalog.numeric_in(pg_catalog.textout($1), 0::Oid, -1) $$
LANGUAGE SQL  STRICT IMMUTABLE NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.time_text(time)
RETURNS text 
AS $$ SELECT CAST(pg_catalog.time_out($1) AS text) $$
LANGUAGE SQL STRICT IMMUTABLE NOT FENCED;

CREATE CAST (time AS text) WITH FUNCTION pg_catalog.time_text(time) AS IMPLICIT;

CREATE OR REPLACE FUNCTION pg_catalog.timetz_text(timetz)
RETURNS text 
AS $$ SELECT CAST(pg_catalog.timetz_out($1) AS text) $$
LANGUAGE SQL STRICT IMMUTABLE NOT FENCED;

CREATE CAST (timetz AS text) WITH FUNCTION pg_catalog.timetz_text(timetz) AS IMPLICIT;

CREATE OR REPLACE FUNCTION pg_catalog.reltime_text(reltime)
RETURNS text 
AS $$ SELECT CAST(pg_catalog.reltimeout($1) AS text) $$
LANGUAGE SQL STRICT IMMUTABLE NOT FENCED;

CREATE CAST (reltime AS text) WITH FUNCTION pg_catalog.reltime_text(reltime) AS IMPLICIT;

CREATE OR REPLACE FUNCTION pg_catalog.abstime_text(abstime)
RETURNS text 
AS $$ SELECT CAST(pg_catalog.abstimeout($1) AS text) $$
LANGUAGE SQL STRICT IMMUTABLE NOT FENCED;

CREATE CAST (abstime AS text) WITH FUNCTION pg_catalog.abstime_text(abstime) AS IMPLICIT;

/*text to num*/
create or replace function pg_catalog.int1(text)
returns int1
as $$ select cast(pg_catalog.to_number($1) as int1)$$
language sql IMMUTABLE strict NOT FENCED;
create or replace function pg_catalog.int2(text)
returns int2
as $$ select cast(pg_catalog.to_number($1) as int2)$$
language sql IMMUTABLE strict NOT FENCED;

create or replace function pg_catalog.int4(text)
returns int4
as $$ select cast(pg_catalog.to_number($1) as int4) $$
language sql IMMUTABLE strict NOT FENCED;

create or replace function pg_catalog.int8(text)
returns int8
as $$ select cast(pg_catalog.to_number($1) as int8) $$
language sql IMMUTABLE strict NOT FENCED;

create or replace function pg_catalog.float4(text)
returns float4
as $$ select cast(pg_catalog.to_number($1) as float4) $$
language sql IMMUTABLE strict NOT FENCED;

create or replace function pg_catalog.float8(text)
returns float8
as $$ select cast(pg_catalog.to_number($1) as float8) $$
language sql IMMUTABLE strict NOT FENCED;

/*character to numeric*/
CREATE OR REPLACE FUNCTION pg_catalog.TO_NUMERIC(CHAR)
RETURNS NUMERIC
AS $$ SELECT pg_catalog.TO_NUMBER($1::TEXT)$$
LANGUAGE SQL IMMUTABLE STRICT NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.TO_NUMERIC(VARCHAR)
RETURNS NUMERIC
AS $$ SELECT pg_catalog.TO_NUMBER($1::TEXT)$$
LANGUAGE SQL IMMUTABLE STRICT NOT FENCED;

/*character to int*/
CREATE OR REPLACE FUNCTION pg_catalog.TO_INTEGER(VARCHAR)
RETURNS INTEGER
AS $$ SELECT pg_catalog.int4in(pg_catalog.varcharout($1)) $$
LANGUAGE SQL IMMUTABLE STRICT NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.TO_INTEGER(CHAR)
RETURNS INTEGER
AS $$ SELECT pg_catalog.int4in(pg_catalog.bpcharout($1)) $$
LANGUAGE SQL IMMUTABLE STRICT NOT FENCED;


CREATE CAST (TEXT AS RAW) WITH FUNCTION pg_catalog.hextoraw(TEXT);
CREATE CAST (RAW AS TEXT) WITH FUNCTION pg_catalog.rawtohex(raw) AS IMPLICIT;

CREATE CAST (BLOB AS RAW) WITHOUT FUNCTION AS IMPLICIT;
CREATE CAST (RAW AS BLOB) WITHOUT FUNCTION AS IMPLICIT;

CREATE CAST (TEXT AS CLOB) WITHOUT FUNCTION AS IMPLICIT;
CREATE CAST (CLOB AS TEXT) WITHOUT FUNCTION AS IMPLICIT;

/* text to clob */
CREATE OR REPLACE FUNCTION pg_catalog.to_clob(TEXT)
RETURNS CLOB
AS $$ select $1 $$
LANGUAGE SQL IMMUTABLE STRICT NOT FENCED;

/* char to clob */
CREATE OR REPLACE FUNCTION pg_catalog.to_clob(CHAR)
RETURNS CLOB
AS $$ select CAST($1 AS TEXT) $$
LANGUAGE SQL IMMUTABLE STRICT NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.to_clob(VARCHAR)
RETURNS CLOB
AS $$ select CAST($1 AS TEXT) $$
LANGUAGE SQL IMMUTABLE STRICT NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.to_clob(NVARCHAR2)
RETURNS CLOB
AS $$ select CAST($1 AS TEXT) $$
LANGUAGE SQL IMMUTABLE STRICT NOT FENCED;

/*character to int8*/
CREATE OR REPLACE FUNCTION pg_catalog.TO_BIGINT(VARCHAR)
RETURNS BIGINT
AS $$ SELECT pg_catalog.int8in(pg_catalog.varcharout($1))$$
LANGUAGE SQL IMMUTABLE STRICT NOT FENCED;

/*float8 to numeric*/
CREATE OR REPLACE FUNCTION pg_catalog.TO_NUMERIC(double precision)
RETURNS NUMERIC
AS $$ SELECT pg_catalog.TO_NUMBER($1::TEXT)$$
LANGUAGE SQL IMMUTABLE STRICT NOT FENCED;

/*date to char(n)*/
CREATE OR REPLACE FUNCTION pg_catalog.TO_TEXT(TIMESTAMP WITHOUT TIME ZONE)
RETURNS TEXT
AS $$  select CAST(pg_catalog.timestamp_out($1) AS VARCHAR2)  $$
LANGUAGE SQL IMMUTABLE STRICT NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.TO_TEXT(TIMESTAMP WITH TIME ZONE)
RETURNS TEXT
AS $$  select CAST(pg_catalog.timestamptz_out($1) AS VARCHAR2)  $$
LANGUAGE SQL IMMUTABLE STRICT NOT FENCED;


CREATE OR REPLACE FUNCTION pg_catalog.TRUNC(TIMESTAMP WITH TIME ZONE)
RETURNS TIMESTAMP WITHOUT TIME ZONE AS $$
        SELECT CAST(DATE_TRUNC('day',$1) AS TIMESTAMP WITHOUT TIME ZONE);
$$
LANGUAGE SQL IMMUTABLE STRICT NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.SUBSTR(TEXT, INT8, INT8) RETURNS TEXT AS $$
	select pg_catalog.SUBSTR($1, $2::INT4, $3::INT4);
$$
LANGUAGE SQL  STRICT IMMUTABLE NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.SUBSTR(TEXT, INT8) RETURNS TEXT AS $$
	select pg_catalog.SUBSTR($1, $2::INT4);
$$
LANGUAGE SQL  STRICT IMMUTABLE NOT FENCED;

/* timestamp to varchar2 */
CREATE OR REPLACE FUNCTION pg_catalog.TO_VARCHAR2(TIMESTAMP WITHOUT TIME ZONE)
RETURNS VARCHAR2
AS $$  select CAST(pg_catalog.timestamp_out($1) AS VARCHAR2)  $$
LANGUAGE SQL IMMUTABLE STRICT NOT FENCED;

/* interval to varchar2 */
CREATE OR REPLACE FUNCTION pg_catalog.TO_VARCHAR2(INTERVAL)
RETURNS VARCHAR2
AS $$  select CAST(pg_catalog.interval_out($1) AS VARCHAR2)  $$
LANGUAGE SQL IMMUTABLE STRICT NOT FENCED;

CREATE CAST (INTERVAL AS VARCHAR2) WITH FUNCTION pg_catalog.TO_VARCHAR2(INTERVAL) AS IMPLICIT;

/* char,varchar2 to interval */
CREATE OR REPLACE FUNCTION pg_catalog.TO_INTERVAL(BPCHAR)
RETURNS INTERVAL
AS $$  select pg_catalog.interval_in(pg_catalog.bpcharout($1), 0::Oid, -1) $$
LANGUAGE SQL IMMUTABLE STRICT NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.TO_INTERVAL(VARCHAR2)
RETURNS INTERVAL
AS $$  select pg_catalog.interval_in(pg_catalog.varcharout($1), 0::Oid, -1) $$
LANGUAGE SQL IMMUTABLE STRICT NOT FENCED;

CREATE CAST (BPCHAR AS INTERVAL) WITH FUNCTION pg_catalog.TO_INTERVAL(BPCHAR) AS IMPLICIT;
CREATE CAST (VARCHAR2 AS INTERVAL) WITH FUNCTION pg_catalog.TO_INTERVAL(VARCHAR2) AS IMPLICIT;

/* raw to varchar2 */
CREATE CAST (RAW AS VARCHAR2) WITH FUNCTION pg_catalog.rawtohex(RAW) AS IMPLICIT;


/* varchar2,char to timestamp */
CREATE OR REPLACE FUNCTION pg_catalog.TO_TS(VARCHAR2)
RETURNS TIMESTAMP WITHOUT TIME ZONE
AS $$  select pg_catalog.timestamp_in(pg_catalog.varcharout($1), 0::Oid, -1)  $$
LANGUAGE SQL IMMUTABLE STRICT NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.TO_TS(BPCHAR)
RETURNS TIMESTAMP WITHOUT TIME ZONE
AS $$  select pg_catalog.timestamp_in(pg_catalog.bpcharout($1), 0::Oid, -1)  $$
LANGUAGE SQL IMMUTABLE STRICT NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.timestamp_to_smalldatetime(TIMESTAMP WITHOUT TIME ZONE)
RETURNS SMALLDATETIME
AS $$  select pg_catalog.smalldatetime_in(pg_catalog.timestamp_out($1), 0::Oid, -1)  $$
LANGUAGE SQL IMMUTABLE STRICT NOT FENCED;
CREATE CAST (TIMESTAMP WITHOUT TIME ZONE AS SMALLDATETIME) WITH FUNCTION pg_catalog.timestamp_to_smalldatetime(TIMESTAMP WITHOUT TIME ZONE) AS IMPLICIT;

CREATE OR REPLACE FUNCTION pg_catalog.smalldatetime_to_timestamp(smalldatetime)
RETURNS TIMESTAMP WITHOUT TIME ZONE
AS $$  select pg_catalog.timestamp_in(pg_catalog.smalldatetime_out($1), 0::Oid, -1)  $$
LANGUAGE SQL IMMUTABLE STRICT NOT FENCED;

CREATE CAST (smalldatetime AS TIMESTAMP WITHOUT TIME ZONE) WITH FUNCTION pg_catalog.smalldatetime_to_timestamp(smalldatetime) AS IMPLICIT;

/* smalldatetime to text */
CREATE OR REPLACE FUNCTION pg_catalog.TO_TEXT(smalldatetime)
RETURNS TEXT
AS $$  select CAST(pg_catalog.smalldatetime_out($1) AS VARCHAR2)  $$
LANGUAGE SQL IMMUTABLE STRICT NOT FENCED;

CREATE CAST (smalldatetime AS TEXT) WITH FUNCTION pg_catalog.TO_TEXT(smalldatetime) AS IMPLICIT;

/* smalldatetime to varchar2 */
CREATE OR REPLACE FUNCTION pg_catalog.SMALLDATETIME_TO_VARCHAR2(smalldatetime)
RETURNS VARCHAR2
AS $$  select CAST(pg_catalog.smalldatetime_out($1) AS VARCHAR2)  $$
LANGUAGE SQL IMMUTABLE STRICT NOT FENCED;

CREATE CAST (smalldatetime AS VARCHAR2) WITH FUNCTION pg_catalog.SMALLDATETIME_TO_VARCHAR2(smalldatetime) AS IMPLICIT;

/* varchar2, bpchar to smalldatetime */
CREATE OR REPLACE FUNCTION pg_catalog.VARCHAR2_TO_SMLLDATETIME(VARCHAR2)
RETURNS SMALLDATETIME
AS $$  select pg_catalog.smalldatetime_in(pg_catalog.varcharout($1), 0::Oid, -1)  $$
LANGUAGE SQL IMMUTABLE STRICT NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.BPCHAR_TO_SMALLDATETIME(BPCHAR)
RETURNS SMALLDATETIME
AS $$  select pg_catalog.smalldatetime_in(pg_catalog.bpcharout($1), 0::Oid, -1)  $$
LANGUAGE SQL IMMUTABLE STRICT NOT FENCED;

CREATE CAST (VARCHAR2 AS SMALLDATETIME) WITH FUNCTION pg_catalog.VARCHAR2_TO_SMLLDATETIME(VARCHAR2) AS IMPLICIT;

CREATE CAST (BPCHAR AS SMALLDATETIME) WITH FUNCTION pg_catalog.BPCHAR_TO_SMALLDATETIME(BPCHAR) AS IMPLICIT;
/*abstime TO smalldatetime*/
CREATE OR REPLACE FUNCTION pg_catalog.abstime_to_smalldatetime(ABSTIME)
RETURNS SMALLDATETIME
AS $$  select pg_catalog.smalldatetime_in(pg_catalog.timestamp_out($1), 0::Oid, -1)  $$
LANGUAGE SQL IMMUTABLE STRICT NOT FENCED;
CREATE CAST (ABSTIME AS SMALLDATETIME) WITH FUNCTION pg_catalog.abstime_to_smalldatetime(ABSTIME) AS IMPLICIT;

/*smalldatetime_to_abstime*/
CREATE OR REPLACE FUNCTION pg_catalog.smalldatetime_to_abstime(smalldatetime)
RETURNS abstime
AS $$  select pg_catalog.abstimein(pg_catalog.smalldatetime_out($1))  $$
LANGUAGE SQL IMMUTABLE STRICT NOT FENCED;

CREATE CAST (smalldatetime AS abstime) WITH FUNCTION pg_catalog.smalldatetime_to_abstime(smalldatetime) AS IMPLICIT;

/*smalldatetime to time*/
CREATE OR REPLACE FUNCTION pg_catalog.smalldatetime_to_time(smalldatetime)
RETURNS time
AS $$  select pg_catalog.time_in(pg_catalog.smalldatetime_out($1), 0::Oid, -1)  $$
LANGUAGE SQL IMMUTABLE STRICT NOT FENCED;
CREATE CAST (smalldatetime AS time) WITH FUNCTION pg_catalog.smalldatetime_to_time(smalldatetime) AS IMPLICIT;

/*smalldatetime_to_timestamptz*/
CREATE OR REPLACE FUNCTION pg_catalog.smalldatetime_to_timestamptz(smalldatetime)
RETURNS TIMESTAMP WITH TIME ZONE
AS $$  select pg_catalog.timestamptz_in(pg_catalog.smalldatetime_out($1), 0::Oid, -1)  $$
LANGUAGE SQL IMMUTABLE STRICT NOT FENCED;

CREATE CAST (smalldatetime AS TIMESTAMP WITH TIME ZONE) WITH FUNCTION pg_catalog.smalldatetime_to_timestamptz(smalldatetime) AS IMPLICIT;

/*timestamptz_to_smalldatetime*/
CREATE OR REPLACE FUNCTION pg_catalog.timestamptz_to_smalldatetime(TIMESTAMP WITH TIME ZONE)
RETURNS smalldatetime
AS $$  select pg_catalog.smalldatetime_in(pg_catalog.TIMESTAMPTZ_OUT($1), 0::Oid, -1)  $$
LANGUAGE SQL IMMUTABLE STRICT NOT FENCED;

CREATE CAST (TIMESTAMP WITH TIME ZONE AS smalldatetime) WITH FUNCTION pg_catalog.timestamptz_to_smalldatetime(TIMESTAMP WITH TIME ZONE) AS IMPLICIT;
create type exception as (code integer, message varchar2);
create or replace function pg_catalog.regexp_substr(text,text)
returns text
AS '$libdir/plpgsql','regexp_substr'
LANGUAGE C STRICT IMMUTABLE NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.report_application_error(
    IN log text,
    IN code integer default null 
)RETURNS void 
AS '$libdir/plpgsql','report_application_error'
LANGUAGE C VOLATILE NOT FENCED;

create or replace function pg_catalog.bitand(bigint,bigint)
returns bigint 
as $$ select $1 & $2 $$
LANGUAGE SQL STRICT IMMUTABLE NOT FENCED;

create or replace function pg_catalog.regexp_like(text,text)
returns boolean as $$ select $1 ~ $2 $$
LANGUAGE SQL STRICT IMMUTABLE NOT FENCED;

create or replace function pg_catalog.regexp_like(text,text,text)
returns boolean as $$ 
select case $3 when 'i' then $1 ~* $2 else $1 ~ $2 end;$$
LANGUAGE SQL STRICT IMMUTABLE NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.INTERVALTONUM(INTERVAL)
RETURNS NUMERIC
AS '$libdir/plpgsql','intervaltonum'
LANGUAGE C STRICT IMMUTABLE NOT FENCED;

CREATE CAST (INTERVAL AS NUMERIC) WITH FUNCTION pg_catalog.INTERVALTONUM(INTERVAL) AS IMPLICIT;

/* add for nvarcahr2 data type */
CREATE OR REPLACE FUNCTION pg_catalog.TO_NUMERIC(NVARCHAR2)
RETURNS NUMERIC
AS $$ SELECT pg_catalog.TO_NUMBER($1::TEXT)$$
LANGUAGE SQL IMMUTABLE STRICT NOT FENCED;
CREATE CAST (NVARCHAR2 AS NUMERIC) WITH FUNCTION pg_catalog.TO_NUMERIC(NVARCHAR2) AS IMPLICIT;

CREATE OR REPLACE FUNCTION pg_catalog.TO_INTEGER(NVARCHAR2)
RETURNS INTEGER
AS $$ SELECT pg_catalog.int4in(pg_catalog.nvarchar2out($1))$$
LANGUAGE SQL IMMUTABLE STRICT NOT FENCED;
CREATE CAST (NVARCHAR2 AS INTEGER) WITH FUNCTION pg_catalog.TO_INTEGER(NVARCHAR2) AS IMPLICIT;

CREATE OR REPLACE FUNCTION pg_catalog.TO_NVARCHAR2(TIMESTAMP WITHOUT TIME ZONE)
RETURNS NVARCHAR2
AS $$  select CAST(pg_catalog.timestamp_out($1) AS NVARCHAR2)  $$
LANGUAGE SQL IMMUTABLE STRICT NOT FENCED;
CREATE CAST (TIMESTAMP WITHOUT TIME ZONE AS NVARCHAR2) WITH FUNCTION pg_catalog.TO_NVARCHAR2(TIMESTAMP WITHOUT TIME ZONE) AS IMPLICIT;

CREATE OR REPLACE FUNCTION pg_catalog.TO_NVARCHAR2(INTERVAL)
RETURNS NVARCHAR2
AS $$  select CAST(pg_catalog.interval_out($1) AS NVARCHAR2)  $$
LANGUAGE SQL IMMUTABLE STRICT NOT FENCED;
CREATE CAST (INTERVAL AS NVARCHAR2) WITH FUNCTION pg_catalog.TO_NVARCHAR2(INTERVAL) AS IMPLICIT;

CREATE OR REPLACE FUNCTION pg_catalog.TO_NVARCHAR2(NUMERIC)
RETURNS NVARCHAR2
AS $$ SELECT CAST(pg_catalog.numeric_out($1) AS NVARCHAR2)    $$
LANGUAGE SQL  STRICT IMMUTABLE NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.TO_NVARCHAR2(INT2)
RETURNS NVARCHAR2
AS $$ select CAST(pg_catalog.int2out($1) AS NVARCHAR2)  $$
LANGUAGE SQL  STRICT IMMUTABLE NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.TO_NVARCHAR2(INT4)
RETURNS NVARCHAR2
AS $$  select CAST(pg_catalog.int4out($1) AS NVARCHAR2)  $$
LANGUAGE SQL  STRICT IMMUTABLE NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.TO_NVARCHAR2(INT8)
RETURNS NVARCHAR2
AS $$ select CAST(pg_catalog.int8out($1) AS NVARCHAR2) $$
LANGUAGE SQL  STRICT IMMUTABLE NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.TO_NVARCHAR2(FLOAT4)
RETURNS NVARCHAR2
AS $$ select CAST(pg_catalog.float4out($1) AS NVARCHAR2) $$
LANGUAGE SQL  STRICT IMMUTABLE NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.TO_NVARCHAR2(FLOAT8)
RETURNS NVARCHAR2
AS $$ select CAST(pg_catalog.float8out($1) AS NVARCHAR2) $$
LANGUAGE SQL  STRICT IMMUTABLE NOT FENCED;

CREATE CAST (INT2 AS NVARCHAR2) WITH FUNCTION pg_catalog.TO_NVARCHAR2(INT2) AS IMPLICIT;
CREATE CAST (INT4 AS NVARCHAR2) WITH FUNCTION pg_catalog.TO_NVARCHAR2(INT4) AS IMPLICIT;
CREATE CAST (INT8 AS NVARCHAR2) WITH FUNCTION pg_catalog.TO_NVARCHAR2(INT8) AS IMPLICIT;
CREATE CAST (NUMERIC AS NVARCHAR2) WITH FUNCTION pg_catalog.TO_NVARCHAR2(NUMERIC) AS IMPLICIT;
CREATE CAST (FLOAT4 AS NVARCHAR2) WITH FUNCTION pg_catalog.TO_NVARCHAR2(FLOAT4) AS IMPLICIT;
CREATE CAST (FLOAT8 AS NVARCHAR2) WITH FUNCTION pg_catalog.TO_NVARCHAR2(FLOAT8) AS IMPLICIT;

CREATE OR REPLACE FUNCTION pg_catalog.TO_TS(NVARCHAR2)
RETURNS TIMESTAMP WITHOUT TIME ZONE
AS $$  select pg_catalog.timestamp_in(pg_catalog.nvarchar2out($1), 0::Oid, -1)  $$
LANGUAGE SQL IMMUTABLE STRICT NOT FENCED;
CREATE CAST (NVARCHAR2 AS TIMESTAMP WITHOUT TIME ZONE) WITH FUNCTION pg_catalog.TO_TS(NVARCHAR2) AS IMPLICIT;

create or replace function pg_catalog.regex_like_m(text,text) returns boolean
as $$
declare
        source_line integer := 1;
        regex_line integer := 1;
        position integer := 1;
        i integer := 1;
        j integer := 1;
        regex_temp text := '';
        flag boolean := false;
        TYPE array_text is varray(1024) of text;
        source_array array_text := array_text();
        regex_array array_text := array_text();
begin
	if pg_catalog.left($2,1) <> '^' and pg_catalog.right($2,1) <> '$' then
		return $1 ~ $2;
	end if;
	--source string to source_array
	for i in 1..pg_catalog.length($1) loop
		if pg_catalog.substr($1,i,1) ~ '\n' then
			if position = i then
				source_array(source_line) := '\n';
			else
				source_array(source_line) := pg_catalog.substr($1,position,i - position);
			end if;
			position := i + 1;
			source_line := source_line + 1;
		end if;
	end loop;
	if position <= pg_catalog.length($1) or position = 1 then
		source_array(source_line) := pg_catalog.substr($1,position);
	else 
		if position > pg_catalog.length($1) then
			source_line := source_line - 1;
		end if;
	end if;
		
	--regexp string to regex_array
	position := 1;
	for i in 1..pg_catalog.length($2) loop
		if pg_catalog.substr($2,i,1) ~ '\n' then
			if position = i then
				regex_array(regex_line) := '\n';
			else
				regex_array(regex_line) := pg_catalog.substr($2,position,i - position);
			end if;
			position := i + 1;
			regex_line := regex_line + 1;
		end if;
	end loop;
		if position <= pg_catalog.length($2) or position = 1 then
			regex_array(regex_line) := pg_catalog.substr($2,position);
		else
			if position > pg_catalog.length($2) then
				regex_line := regex_line - 1;
			end if;
		end if;
	
	--start
	for i in 1..source_line loop
		if source_array[i] ~ regex_array[j] then
			flag := true;
			j := j + 1;
			while j <= regex_line loop
				i := i + 1;
				if source_array[i] ~ regex_array[j] then
					j := j + 1;
				else
					flag := false;
					exit;
				end if;
			end loop;
			exit;
		end if;
	end loop;
	if pg_catalog.left($2,1) = '^' then
		regex_temp := pg_catalog.substr($2,2);
	else
		regex_temp := $2;
	end if;
	if pg_catalog.right($2,1) = '$' then
		regex_temp := pg_catalog.substr(regex_temp,1,pg_catalog.length(regex_temp)-1);
	end if;
	if flag then
 		flag := $1 ~ regex_temp;
 	end if;
	return flag;
end;
$$ LANGUAGE plpgsql shippable NOT FENCED;

create or replace function pg_catalog.regexp_like(text,text,text) 
returns boolean
as $$
declare
	regex_char varchar(1);
begin
	for i in 1..pg_catalog.length($3) loop
		regex_char := pg_catalog.substr($3,i,1);
		if regex_char <> 'i' and  regex_char <> 'm' and  regex_char <> 'c' then
			raise info 'illegal argument for function';
			return false;
		end if;
	end loop;
	case pg_catalog.right($3, 1)
		when 'i' then return $1 ~* $2;
		when 'c' then return $1 ~ $2;
		when 'm' then return pg_catalog.regex_like_m($1,$2);
	end case;
end;
$$ LANGUAGE plpgsql shippable NOT FENCED;


create or replace function pg_catalog.rawtohex(text)
returns text
AS '$libdir/plpgsql','rawtohex'
LANGUAGE C STRICT IMMUTABLE NOT FENCED;
/*
 * login_audit_messages
 */
CREATE OR REPLACE FUNCTION pg_catalog.login_audit_messages(in flag boolean) returns table (username text, database text, logintime timestamp with time zone, mytype text, result text, client_conninfo text) AUTHID DEFINER
AS $$
DECLARE
user_id text;
user_name text;
db_name text;
SQL_STMT VARCHAR2(500);
fail_cursor REFCURSOR;
success_cursor REFCURSOR;
BEGIN
	SELECT pg_catalog.text(oid) FROM pg_catalog.pg_authid WHERE rolname=SESSION_USER INTO user_id;
	SELECT SESSION_USER INTO user_name;
	SELECT pg_catalog.CURRENT_DATABASE() INTO db_name;
	IF flag = true THEN 
		SQL_STMT := 'SELECT username,database,time,type,result,client_conninfo FROM pg_catalog.pg_query_audit(''1970-1-1'',''9999-12-31'') WHERE 
					type IN (''login_success'') AND username =' || pg_catalog.quote_literal(user_name) ||
					' AND database =' || pg_catalog.quote_literal(db_name) || ' AND userid =' || pg_catalog.quote_literal(user_id) || ';';
		OPEN success_cursor FOR EXECUTE SQL_STMT;		
		--search bottom up for all the success login info
		FETCH LAST FROM success_cursor into username, database, logintime, mytype, result, client_conninfo;
		FETCH BACKWARD FROM success_cursor into username, database, logintime, mytype, result, client_conninfo;
		IF FOUND THEN
			return next;
		END IF;
		CLOSE success_cursor;
	ELSE 
		SQL_STMT := 'SELECT username,database,time,type,result,client_conninfo FROM pg_catalog.pg_query_audit(''1970-1-1'',''9999-12-31'') WHERE 
					type IN (''login_success'', ''login_failed'') AND username =' || pg_catalog.quote_literal(user_name) ||
					' AND database =' || pg_catalog.quote_literal(db_name) || ' AND userid =' || pg_catalog.quote_literal(user_id) || ';';
		OPEN fail_cursor FOR EXECUTE SQL_STMT;
		--search bottom up 
		FETCH LAST FROM fail_cursor into username, database, logintime, mytype, result, client_conninfo;
		LOOP
			FETCH BACKWARD FROM fail_cursor into username, database, logintime, mytype, result, client_conninfo;
			EXIT WHEN NOT FOUND;
			IF mytype = 'login_failed' THEN 
				return next;
			ELSE 
			-- must be login_success
				EXIT;
			END IF;
		END LOOP;
		CLOSE fail_cursor;
	END IF;
END; $$
LANGUAGE plpgsql NOT FENCED;
/*
 * login_audit_messages_pid
 * Different from login_audit_messages is that: it will find the last login record based on the current pid.
 * So after the current user login, whenever he calls this method, it returns the same record.
 * He can not see the following login information for the current user.
 * This is a special API for DataStudio, not the common behavrior. 
 * Highly suggest to use the login_audit_messages instead of this.
 */
CREATE OR REPLACE FUNCTION pg_catalog.login_audit_messages_pid(flag boolean)
 RETURNS TABLE(username text, database text, logintime timestamp with time zone, mytype text, result text, client_conninfo text, backendid bigint) AUTHID DEFINER
AS $$
DECLARE
user_id text;
user_name text;
db_name text;
SQL_STMT VARCHAR2(500);
fail_cursor REFCURSOR;
success_cursor REFCURSOR;
mybackendid bigint;
curSessionFound boolean;
BEGIN
	SELECT pg_catalog.text(oid) FROM pg_catalog.pg_authid WHERE rolname=SESSION_USER INTO user_id;
	SELECT SESSION_USER INTO user_name;
	SELECT pg_catalog.CURRENT_DATABASE() INTO db_name;
	SELECT pg_catalog.pg_backend_pid() INTO mybackendid;
	curSessionFound = false;	
	IF flag = true THEN 
		SQL_STMT := 'SELECT username,database,time,type,result,client_conninfo, pg_catalog.split_part(thread_id,''@'',1) backendid FROM pg_catalog.pg_query_audit(''1970-1-1'',''9999-12-31'') WHERE 
					type IN (''login_success'') AND username =' || pg_catalog.quote_literal(user_name) ||
					' AND database =' || pg_catalog.quote_literal(db_name) || ' AND userid =' || pg_catalog.quote_literal(user_id) || ';';
		OPEN success_cursor FOR EXECUTE SQL_STMT;		
		--search bottom up for all the success login info
		FETCH LAST FROM success_cursor into username, database, logintime, mytype, result, client_conninfo, backendid;
		LOOP  
			IF backendid = mybackendid THEN 
				--found the login info for the current session			
				curSessionFound = true;
				EXIT;
			END IF; 	
			FETCH BACKWARD FROM success_cursor into username, database, logintime, mytype, result, client_conninfo, backendid;
			EXIT WHEN NOT FOUND;
		END LOOP;
		IF curSessionFound THEN 
			FETCH BACKWARD FROM success_cursor into username, database, logintime, mytype, result, client_conninfo, backendid;
			IF FOUND THEN
				return next;
			END IF;
		END IF;
	ELSE 
		SQL_STMT := 'SELECT username,database,time,type,result,client_conninfo, pg_catalog.split_part(thread_id,''@'',1) backendid FROM pg_catalog.pg_query_audit(''1970-1-1'',''9999-12-31'') WHERE 
					type IN (''login_success'', ''login_failed'') AND username =' || pg_catalog.quote_literal(user_name) || 
					' AND database =' || pg_catalog.quote_literal(db_name) || ' AND userid =' || pg_catalog.quote_literal(user_id) || ';';
		OPEN fail_cursor FOR EXECUTE SQL_STMT;
		--search bottom up 
		FETCH LAST FROM fail_cursor into username, database, logintime, mytype, result, client_conninfo, backendid;
		LOOP  
			IF backendid = mybackendid AND mytype = 'login_success' THEN 
				--found the login info for the current session			
				curSessionFound = true;
				EXIT;
			END IF; 	
			FETCH BACKWARD FROM fail_cursor into username, database, logintime, mytype, result, client_conninfo, backendid;
			EXIT WHEN NOT FOUND;
		END LOOP;
		IF curSessionFound THEN 
			LOOP
				FETCH BACKWARD FROM fail_cursor into username, database, logintime, mytype, result, client_conninfo, backendid ;
				EXIT WHEN NOT FOUND;
				IF mytype = 'login_failed' THEN 
					return next;
				ELSE 
				-- must be login_success
					EXIT;
				END IF;
			END LOOP;
		END IF; --curSessionFound
		CLOSE fail_cursor;
	END IF;
END; $$
LANGUAGE plpgsql NOT FENCED;

/*
 * pg_thread_wait_status
 *
 * local way to fetch all thread wait status in local node.
 */
CREATE VIEW pg_thread_wait_status AS
	SELECT * FROM pg_catalog.pg_stat_get_status(NULL);

/*
 * pgxc_thread_wait_status
 *
 * parallel way to fetch global thread wait status.
 */
CREATE VIEW pgxc_thread_wait_status AS
	SELECT * FROM pg_catalog.pgxc_get_thread_wait_status();

/*
 *gs_sql_count
 */
CREATE VIEW gs_sql_count AS
	SELECT 
			node_name::name,
			user_name::name, 	
			select_count,	
			update_count, 
			insert_count, 
			delete_count,
			mergeinto_count,
			ddl_count,
			dml_count,
			dcl_count,
			total_select_elapse,
			avg_select_elapse,
			max_select_elapse,
			min_select_elapse,
			total_update_elapse,
			avg_update_elapse,
			max_update_elapse,
			min_update_elapse,
			total_insert_elapse,
			avg_insert_elapse,
			max_insert_elapse,
			min_insert_elapse,
			total_delete_elapse,
			avg_delete_elapse,
			max_delete_elapse,
			min_delete_elapse
	FROM pg_catalog.pg_stat_get_sql_count();

CREATE VIEW pg_os_threads AS
    SELECT
			S.node_name,
			S.pid,
			S.lwpid,
			S.thread_name,
			S.creation_time
    FROM pg_catalog.pg_stat_get_thread() AS S;
	
CREATE VIEW pg_node_env AS
    SELECT
			S.node_name,
			S.host,
			S.process,
			S.port,
			S.installpath,
			S.datapath,
			S.log_directory
    FROM pg_catalog.pg_stat_get_env() AS S;
	
/*
 * PGXC system view to look for libcomm stat
 */
CREATE VIEW pg_comm_status AS
    SELECT * FROM pg_catalog.pg_comm_status();
/*
 * PGXC system view to look for libcomm recv stream status
 */
CREATE VIEW pg_comm_recv_stream AS
	SELECT
			S.node_name,
			S.local_tid,
			S.remote_name,
			S.remote_tid,
			S.idx,
			S.sid,
			S.tcp_sock,
			S.state,
			S.query_id,
			S.pn_id,
			S.send_smp,
			S.recv_smp,
			S.recv_bytes,
			S.time,
			S.speed,
			S.quota,
			S.buff_usize
	FROM pg_catalog.pg_comm_recv_stream() AS S;

/*
 * PGXC system view to look for libcomm send stream status
 */
CREATE VIEW pg_comm_send_stream AS
	SELECT
			S.node_name,
			S.local_tid,
			S.remote_name,
			S.remote_tid,
			S.idx,
			S.sid,
			S.tcp_sock,
			S.state,
			S.query_id,
			S.pn_id,
			S.send_smp,
			S.recv_smp,
			S.send_bytes,
			S.time,
			S.speed,
			S.quota,
			S.wait_quota
	FROM pg_catalog.pg_comm_send_stream() AS S;
/*
 * PGXC sytem view to show running transctions on node
 */
CREATE VIEW pg_running_xacts AS                                            
SELECT                                                                     
		*                                                          
FROM pg_catalog.pg_get_running_xacts(); 
	                                                                   
/*
 * PGXC sytem view to show variable cache on node
 */
CREATE VIEW pg_variable_info AS
SELECT * FROM pg_catalog.pg_get_variable_info();
--Test distribute situation 
create or replace function pg_catalog.table_skewness(table_name text, column_name text,
                        OUT seqNum text, OUT Num text, OUT Ratio text, row_num text default '0')
RETURNS setof record
AS $$
DECLARE
    tolal_num text;
    row_data record;
    execute_query text;
    BEGIN
        if row_num = 0 then
            EXECUTE 'select pg_catalog.count(1) from ' || table_name into tolal_num;
            execute_query = 'select seqNum, pg_catalog.count(1) as num
                            from (select pg_catalog.table_data_skewness(row(' || column_name ||'), ''H'') as seqNum from ' || table_name ||
                            ') group by seqNum order by num DESC';
        else
            tolal_num = row_num;
            execute_query = 'select seqNum, pg_catalog.count(1) as num
                            from (select pg_catalog.table_data_skewness(row(' || column_name ||'), ''H'') as seqNum from ' || table_name ||
                            ' limit ' || row_num ||') group by seqNum order by num DESC';
        end if;

        if tolal_num = 0 then
            seqNum = 0;
            Num = 0;
            Ratio = pg_catalog.ROUND(0, 3) || '%';
            return;
        end if;

        for row_data in EXECUTE execute_query loop
            seqNum = row_data.seqNum;
            Num = row_data.num;
            Ratio = pg_catalog.ROUND(row_data.num / tolal_num * 100, 3) || '%';
            RETURN next;
        end loop;
    END;
$$LANGUAGE plpgsql NOT FENCED;
/*
 * view for backends holding invalid pooler connection on coordinator
 */
CREATE VIEW pg_get_invalid_backends AS
	SELECT
			C.pid,
			C.node_name,
			S.datname AS dbname,
			S.backend_start,
			S.query
	FROM pg_catalog.pg_pool_validate(false, ' ') AS C LEFT JOIN pg_stat_activity AS S
		ON (C.pid = S.sessionid);

/*
 * view for data senders and wal senders catchup time
 */
CREATE VIEW pg_get_senders_catchup_time AS
	SELECT
			W.pid,
			W.sender_pid AS lwpid,
			W.local_role,
			W.peer_role,
			W.state,
			'Wal' AS type,
			W.catchup_start,
			W.catchup_end
	FROM pg_catalog.pg_stat_get_wal_senders() AS W
	UNION ALL
	SELECT
			D.pid,
			D.sender_pid AS lwpid,
			D.local_role,
			D.peer_role,
			D.state,
			'Data' AS type,
			D.catchup_start,
			D.catchup_end
	FROM pg_catalog.pg_stat_get_data_senders() AS D;

CREATE OR REPLACE FUNCTION pg_catalog.pg_stat_session_cu(OUT mem_hit int, OUT hdd_sync_read int, OUT hdd_asyn_read int)
RETURNS setof record
AS $$
DECLARE
	stat_result record;
	query_str text;
	statname text;
	BEGIN
		query_str := 'select statname, pg_catalog.sum(value) as value from gs_session_stat group by statname;';
		FOR stat_result IN EXECUTE(query_str) LOOP
			statname := stat_result.statname;
			IF statname = 'n_cu_mem_hit' THEN
				mem_hit := stat_result.value;
			ELSIF statname = 'n_cu_hdd_sync_read' THEN
				hdd_sync_read := stat_result.value;
			ELSIF statname = 'n_cu_hdd_asyn_read' THEN
				hdd_asyn_read := stat_result.value;
			END IF;
		END LOOP;
		return next;
	END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE VIEW gs_stat_session_cu AS
    SELECT DISTINCT * from pg_catalog.pg_stat_session_cu();

/*
 * PGXC system view to look for libcomm delay information
 */
CREATE VIEW pg_comm_delay AS
    SELECT DISTINCT * from pg_catalog.pg_comm_delay();

CREATE VIEW gs_comm_proxy_thread_status AS
    SELECT DISTINCT * from pg_catalog.gs_comm_proxy_thread_status();

ALTER TEXT SEARCH CONFIGURATION ngram ADD MAPPING
        FOR zh_words, en_word, numeric, alnum, grapsymbol, multisymbol
        WITH simple;

CREATE VIEW gs_all_control_group_info AS
    SELECT DISTINCT * from pg_catalog.gs_all_control_group_info();

CREATE VIEW mpp_tables AS
    SELECT n.nspname AS schemaname, c.relname AS tablename, 
        pg_catalog.pg_get_userbyid(c.relowner) AS tableowner, t.spcname AS tablespace, x.pgroup,x.nodeoids
    FROM pg_class c
    LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
    LEFT JOIN pg_tablespace t ON t.oid = c.reltablespace
    JOIN pgxc_class x ON x.pcrelid = c.oid
    WHERE n.nspname <> 'pmk';

--table definition for history info 
create table gs_wlm_operator_info
(
	queryid  bigint not null,
	pid  bigint,
	plan_node_id int,
	plan_node_name text,
	start_time	timestamp with time zone,
	duration  bigint,
	query_dop int,
	estimated_rows bigint,
	tuple_processed bigint,
	min_peak_memory int,
	max_peak_memory int,
	average_peak_memory  int,
	memory_skew_percent   int,
	min_spill_size int,
	max_spill_size int,
	average_spill_size int,
	spill_skew_percent int,
	min_cpu_time bigint,
	max_cpu_time bigint,
	total_cpu_time bigint,
	cpu_skew_percent int,
	warning text
);

--real time operator-level view in single CN
CREATE VIEW gs_wlm_operator_statistics AS
SELECT t.*
FROM pg_stat_activity AS s, pg_catalog.pg_stat_get_wlm_realtime_operator_info(NULL) as t
where s.query_id = t.queryid;

--history operator-level view for DM in single CN
CREATE VIEW gs_wlm_operator_history AS
SELECT * FROM pg_catalog.pg_stat_get_wlm_operator_info(0);

--function used to get history table from hash table
CREATE OR REPLACE FUNCTION pg_catalog.create_wlm_operator_info(IN flag int)
RETURNS int
AS $$
DECLARE
	query_ec_str text;
    query_plan_str text;
	query_str text;
	record_cnt int;
	BEGIN
		record_cnt := 0;

		query_ec_str := 'SELECT 
							queryid,
							plan_node_id,
							start_time,
							duration,
							tuple_processed,
							min_peak_memory,
							max_peak_memory,
							average_peak_memory,
							ec_status,
							ec_execute_datanode,
							ec_dsn,
							ec_username,
							ec_query,
							ec_libodbc_type
						FROM pg_catalog.pg_stat_get_wlm_ec_operator_info(0) where ec_operator > 0';
		
        query_plan_str := 'SELECT * FROM pg_catalog.gs_stat_get_wlm_plan_operator_info(0)';

		query_str := 'SELECT * FROM pg_catalog.pg_stat_get_wlm_operator_info(1)';
		
		IF flag > 0 THEN
			EXECUTE 'INSERT INTO gs_wlm_ec_operator_info ' || query_ec_str;
            EXECUTE 'INSERT INTO gs_wlm_plan_operator_info ' || query_plan_str;
			EXECUTE 'INSERT INTO gs_wlm_operator_info ' || query_str;
		ELSE
			EXECUTE query_ec_str;
            EXECUTE query_plan_str;
			EXECUTE query_str;
		END IF;
		
		RETURN record_cnt;
	END; $$
LANGUAGE plpgsql NOT FENCED;

--table definition for operator history info with specific plan information
create table gs_wlm_plan_operator_info
(
	datname name,
	queryid bigint not null,
	plan_node_id int,
	startup_time bigint,
	total_time  bigint,
	actual_rows bigint,
	max_peak_memory int,
	query_dop int,
	parent_node_id int,
	left_child_id int,
	right_child_id int,
	operation  text,
	orientation   text,
	strategy text,
	options text,
	condition text,
	projection text
);

CREATE VIEW gs_wlm_plan_operator_history AS
SELECT * FROM pg_catalog.gs_stat_get_wlm_plan_operator_info(0);

--perf hist encoder
CREATE OR REPLACE FUNCTION pg_catalog.encode_feature_perf_hist
(
IN datname text,
OUT queryid bigint,
OUT plan_node_id int,
OUT parent_node_id int,
OUT left_child_id int,
OUT right_child_id int,
OUT encode text,
OUT startup_time bigint,
OUT total_time bigint,
OUT rows bigint,
OUT peak_memory int
)
RETURNS setof record
AS $$
DECLARE
    query_str_delete text;
    query_str_select text;
    query_str_encode text;
    encoded_record integer;
    row_data record;
    encoded_data record;
    dop integer;
    operation text;
    orientation text;
    strategy text;
    options text;
    condition text;
    projection text;
    BEGIN
        query_str_select := 'SELECT * FROM gs_wlm_plan_operator_info where datname ='''|| datname || ''';';
        FOR row_data IN EXECUTE(query_str_select) LOOP
            queryid = row_data.queryid;
            plan_node_id = row_data.plan_node_id;
            parent_node_id = row_data.parent_node_id;
            left_child_id = row_data.left_child_id;
            right_child_id = row_data.right_child_id;
            startup_time = row_data.startup_time;
            total_time = row_data.total_time;
            rows = row_data.actual_rows;
            peak_memory = row_data.max_peak_memory;
            operation = row_data.operation;
            orientation = row_data.orientation;
            strategy = row_data.strategy;
            options = row_data.options;
            dop = row_data.query_dop;
            condition = row_data.condition;
            projection = row_data.projection;
            query_str_encode := 'SELECT pg_catalog.encode_plan_node($tag$'|| operation ||'$tag$,$tag$'|| orientation ||'$tag$,$tag$'|| strategy ||'$tag$,$tag$ '|| options || '$tag$,$tag$'|| dop ||'$tag$,$tag$' || condition || '$tag$,$tag$' || projection || '$tag$) as result;';
            EXECUTE query_str_encode INTO encoded_data;
            encode = encoded_data.result;
        return next;
        END LOOP;
    END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE TABLE gs_wlm_plan_encoding_table
(
    queryid bigint,
    plan_node_id int,
    parent_node_id int,
    startup_time bigint,
    total_time bigint,
    rows bigint,
    peak_memory int,
    encode text
);

CREATE OR REPLACE FUNCTION pg_catalog.gather_encoding_info(IN datname text)
RETURNS int
AS $$
DECLARE
    BEGIN
        EXECUTE 'INSERT INTO gs_wlm_plan_encoding_table
                    (queryid, plan_node_id, parent_node_id, encode, startup_time, total_time, rows, peak_memory)
                 SELECT queryid, plan_node_id, parent_node_id, encode, startup_time, total_time, rows, peak_memory
                 FROM pg_catalog.encode_feature_perf_hist('''|| datname ||''') order by queryid, plan_node_id;';
        RETURN 0;
    END;$$
LANGUAGE plpgsql NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.pg_catalog.copy_error_log_create()
RETURNS bool
AS $$
DECLARE
	query_str_create_table text;
	query_str_create_index text;
	query_str_do_revoke text;
	BEGIN
		query_str_create_table := 'CREATE TABLE public.pgxc_copy_error_log 
							(relname varchar, begintime timestamptz, filename varchar, lineno int8, rawrecord text, detail text)';
		EXECUTE query_str_create_table;
		
		query_str_create_index := 'CREATE INDEX copy_error_log_relname_idx ON public.pgxc_copy_error_log(relname)';
		EXECUTE query_str_create_index;
		
		query_str_do_revoke := 'REVOKE ALL on public.pgxc_copy_error_log FROM public';
		EXECUTE query_str_do_revoke;
		
		return true;
	END; $$
LANGUAGE 'plpgsql' NOT FENCED;

REVOKE ALL on FUNCTION pg_catalog.copy_error_log_create() FROM public;

CREATE OR REPLACE FUNCTION pg_catalog.copy_summary_create()
RETURNS bool
AS $$
DECLARE
	BEGIN
        EXECUTE 'CREATE TABLE public.gs_copy_summary
                (relname varchar, begintime timestamptz, endtime timestamptz, 
                id bigint, pid bigint, readrows bigint, skiprows bigint, loadrows bigint, errorrows bigint, whenrows bigint, allnullrows bigint, detail text);';

        EXECUTE 'CREATE INDEX gs_copy_summary_idx on public.gs_copy_summary (id);';

		return true;
	END; $$
LANGUAGE 'plpgsql' NOT FENCED;

-- Get all control group information including installation group and logic cluster group.
CREATE OR REPLACE FUNCTION pg_catalog.gs_get_control_group_info()
RETURNS setof record
AS $$
DECLARE
    row_data record;
    row_name record;
    query_str text;
    query_str_nodes text;
    BEGIN
        query_str_nodes := 'SELECT group_name,group_kind FROM pgxc_group WHERE group_kind = ''v'' OR group_kind = ''i'' ';
        FOR row_name IN EXECUTE(query_str_nodes) LOOP
            IF row_name.group_kind = 'i' THEN
                query_str := 'SELECT *,CAST(''' || row_name.group_name || ''' AS TEXT) AS nodegroup,CAST(''' || row_name.group_kind || ''' AS TEXT) AS group_kind FROM pg_catalog.gs_all_nodegroup_control_group_info(''installation'')';
            ELSE
                query_str := 'SELECT *,CAST(''' || row_name.group_name || ''' AS TEXT) AS nodegroup,CAST(''' || row_name.group_kind || ''' AS TEXT) AS group_kind FROM pg_catalog.gs_all_nodegroup_control_group_info(''' ||row_name.group_name||''')';
            END IF;
            FOR row_data IN EXECUTE(query_str) LOOP
                return next row_data;
            END LOOP;
        END LOOP;
        return;
    END; $$
LANGUAGE 'plpgsql' NOT FENCED;

-- the view for function gs_total_nodegroup_memory_detail.
CREATE VIEW pg_catalog.gs_total_nodegroup_memory_detail AS SELECT * FROM pg_catalog.gs_total_nodegroup_memory_detail();

-- the view for function gs_get_control_group_info.
CREATE VIEW pg_catalog.gs_get_control_group_info AS
    SELECT * from pg_catalog.gs_get_control_group_info() AS
    (
         name         text,
         type         text,
         gid          bigint,
         classgid     bigint,
         class        text,
         workload     text,
         shares       bigint,
         limits       bigint,
         wdlevel      bigint,
         cpucores     text,
         nodegroup    text,
         group_kind   text
    );

--real time ec operator-level view in single CN
CREATE VIEW gs_wlm_ec_operator_statistics AS
SELECT 
	t.queryid,
	t.plan_node_id,
	t.start_time,
	t.ec_status,
	t.ec_execute_datanode,
	t.ec_dsn,
	t.ec_username,
	t.ec_query,
	t.ec_libodbc_type,
	t.ec_fetch_count
FROM pg_stat_activity AS s, pg_catalog.pg_stat_get_wlm_realtime_ec_operator_info(NULL) as t
where s.query_id = t.queryid and t.ec_operator > 0;

--ec history operator-level view for DM in single CN
CREATE VIEW gs_wlm_ec_operator_history AS
SELECT
	queryid,
	plan_node_id,
	start_time,
	duration,
	tuple_processed,
	min_peak_memory,
	max_peak_memory,	
	average_peak_memory,
	ec_status,
	ec_execute_datanode,
	ec_dsn,
	ec_username,
	ec_query,
	ec_libodbc_type
FROM pg_catalog.pg_stat_get_wlm_ec_operator_info(0) where ec_operator > 0;

--table definition for ec history info 
create table gs_wlm_ec_operator_info
(
	queryid  bigint not null,
	plan_node_id int,
	start_time	timestamp with time zone,
	duration  bigint,
	tuple_processed bigint,
	min_peak_memory int,
	max_peak_memory int,	
	average_peak_memory int,
	ec_status text,
	ec_execute_datanode text,
	ec_dsn text,
	ec_username text,
	ec_query text,
	ec_libodbc_type text
);

-- create view pg_tde_info
CREATE VIEW pg_catalog.pg_tde_info AS 
SELECT * from pg_catalog.pg_tde_info();

--get delta infomation in single DN
CREATE OR REPLACE FUNCTION pg_catalog.pg_get_delta_info(IN rel TEXT, IN schema_name TEXT, OUT part_name TEXT, OUT live_tuple INT8, OUT data_size INT8, OUT blockNum INT8)
RETURNS setof record
AS $$
DECLARE
	query_info_str text;
	query_str text;
	query_part_str text;
	query_select_str text;
	query_size_str text;
	row_info_data record;
	row_data record;
	row_part_info record;
	BEGIN
		query_info_str := 'SELECT C.oid,C.reldeltarelid,C.parttype FROM pg_class C LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)  WHERE C.relname = '''|| rel ||''' and N.nspname = '''|| schema_name ||'''';
		FOR row_info_data IN EXECUTE(query_info_str) LOOP
		IF row_info_data.parttype = 'n' THEN
			query_str := 'SELECT relname,oid from pg_class where oid= '||row_info_data.reldeltarelid||'';
			EXECUTE(query_str) INTO row_data;
			query_select_str := 'select pg_catalog.count(*) from cstore.' || row_data.relname || '';
			EXECUTE (query_select_str) INTO live_tuple;
			query_size_str := 'select * from pg_catalog.pg_relation_size(' || row_data.oid || ')';
			EXECUTE (query_size_str) INTO data_size;
			blockNum := data_size/8192;
			part_name := 'non partition table';
			return next;
		ELSE
			query_part_str := 'SELECT relname,reldeltarelid from pg_partition where parentid = '||row_info_data.oid|| 'and relname <> '''||rel||'''';
			FOR row_part_info IN EXECUTE(query_part_str) LOOP
				query_str := 'SELECT relname,oid from pg_class where  oid = '||row_part_info.reldeltarelid||'';
				part_name := row_part_info.relname;
				FOR row_data IN EXECUTE(query_str) LOOP
					query_select_str := 'select pg_catalog.count(*) from cstore.' || row_data.relname || '';
					EXECUTE (query_select_str) INTO live_tuple;
					query_size_str := 'select * from pg_catalog.pg_relation_size(' || row_data.oid || ')';
					EXECUTE (query_size_str) INTO data_size;
				END LOOP;
				blockNum := data_size/8192;
				return next;
			END LOOP;
		END IF;
		END LOOP;
	END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE VIEW pg_catalog.pg_stat_bad_block AS
	SELECT DISTINCT * from pg_catalog.pg_stat_bad_block();

CREATE OR REPLACE FUNCTION pg_catalog.lock_cluster_ddl()
RETURNS boolean                    
AS $$                                                                      
DECLARE                                                                                                   
	databse_name record; 
	lock_str text;
	query_database_oid text;
	lock_result  boolean = false;
	return_result  bool = true;
	BEGIN                                                                   
		query_database_oid := 'SELECT datname FROM pg_database WHERE datallowconn = true order by datname';
		for databse_name in EXECUTE(query_database_oid) LOOP
			lock_str = pg_catalog.format('SELECT * FROM pg_catalog.pgxc_lock_for_sp_database(''%s'')', databse_name.datname);
			begin
				EXECUTE(lock_str) into lock_result;
				if lock_result = 'f' then
					return_result = false;
					return return_result; 
				end if;
			end;
		end loop;
		return return_result;                                                   
	END; $$                                                            
LANGUAGE 'plpgsql' NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.unlock_cluster_ddl()
RETURNS bool                    
AS $$                                                                      
DECLARE                                                                                                   
	databse_name record;  
	unlock_str text;
	query_database_oid text;
	unlock_result  boolean = false;
	return_result  bool = true;
	BEGIN                                                                   	
		query_database_oid := 'SELECT datname FROM pg_database WHERE datallowconn = true order by datname';
		for databse_name in EXECUTE(query_database_oid) LOOP
			unlock_str = pg_catalog.format('SELECT * FROM pg_catalog.pgxc_unlock_for_sp_database(''%s'')', databse_name.datname);
			begin
				EXECUTE(unlock_str) into unlock_result;
				if unlock_result = 'f' then
					return_result = false;
					return return_result; 
				end if;
			end;
		end loop;
		return return_result;                                                   
	END; $$                                                            
LANGUAGE 'plpgsql' NOT FENCED;

CREATE TABLE PLAN_TABLE_DATA(
	session_id 	    text NOT NULL,
	user_id		    oid NOT NULL,
	statement_id 	varchar2(30),
	plan_id		    bigint,
	id 		        int,
	operation	    varchar2(30),
	options		    varchar2(255),
	object_name	    name,
	object_type	    varchar2(30),
	object_owner    name,
	projection	    varchar2(4000),
    cost            float8,
    cardinality     float8
);

CREATE VIEW PLAN_TABLE AS
SELECT statement_id,plan_id,id,operation,options,object_name,object_type,object_owner,projection,cost,cardinality
FROM PLAN_TABLE_DATA
WHERE session_id=pg_catalog.pg_current_sessionid()
AND user_id=pg_catalog.pg_current_userid();

-- get pgxc dirty tables stat
CREATE OR REPLACE FUNCTION pg_catalog.pgxc_get_stat_dirty_tables(in dirty_percent int4, in n_tuples int4, out relid oid, out relname name, out schemaname name, out n_tup_ins int8, out n_tup_upd int8, out n_tup_del int8, out n_live_tup int8, out n_dead_tup int8, out dirty_page_rate numeric(5,2))
RETURNS setof record
AS $$
DECLARE
	query_str text;
	row_data record;
	BEGIN
		query_str := 'SELECT oid relid, s.relname,s.schemaname,s.n_tup_ins,s.n_tup_upd,s.n_tup_del,s.n_live_tup,s.n_dead_tup,s.dirty_page_rate
						FROM pg_class p,
						(SELECT  relname, schemaname, pg_catalog.SUM(n_tup_ins) n_tup_ins, pg_catalog.SUM(n_tup_upd) n_tup_upd, pg_catalog.SUM(n_tup_del) n_tup_del, pg_catalog.SUM(n_live_tup) n_live_tup, pg_catalog.SUM(n_dead_tup) n_dead_tup, CAST((pg_catalog.SUM(n_dead_tup) / pg_catalog.SUM(n_dead_tup + n_live_tup + 0.00001) * 100) 
						AS pg_catalog.NUMERIC(5,2)) dirty_page_rate FROM pg_catalog.pgxc_stat_dirty_tables('||dirty_percent||','||n_tuples||') GROUP BY (relname,schemaname)) s
						WHERE p.relname = s.relname AND p.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = s.schemaname) ORDER BY dirty_page_rate DESC';
		FOR row_data IN EXECUTE(query_str) LOOP
			relid = row_data.relid;
			relname = row_data.relname;
			schemaname = row_data.schemaname;
			n_tup_ins = row_data.n_tup_ins;
			n_tup_upd = row_data.n_tup_upd;
			n_tup_del = row_data.n_tup_del;
			n_live_tup = row_data.n_live_tup;
			n_dead_tup = row_data.n_dead_tup;
			dirty_page_rate = row_data.dirty_page_rate;
			return next;
		END LOOP;
	END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.pgxc_get_stat_dirty_tables(in dirty_percent int4, in n_tuples int4,in schema text, out relid oid, out relname name, out schemaname name, out n_tup_ins int8, out n_tup_upd int8, out n_tup_del int8, out n_live_tup int8, out n_dead_tup int8, out dirty_page_rate numeric(5,2))
RETURNS setof record
AS $$
DECLARE
	query_str text;
	row_data record;
	BEGIN
		query_str := 'SELECT oid relid, s.relname,s.schemaname,s.n_tup_ins,s.n_tup_upd,s.n_tup_del,s.n_live_tup,s.n_dead_tup,s.dirty_page_rate
						FROM pg_class p,
						(SELECT  relname, schemaname, pg_catalog.SUM(n_tup_ins) n_tup_ins, pg_catalog.SUM(n_tup_upd) n_tup_upd, pg_catalog.SUM(n_tup_del) n_tup_del, pg_catalog.SUM(n_live_tup) n_live_tup, pg_catalog.SUM(n_dead_tup) n_dead_tup, CAST((pg_catalog.SUM(n_dead_tup) / pg_catalog.SUM(n_dead_tup + n_live_tup + 0.00001) * 100) 
						AS pg_catalog.NUMERIC(5,2)) dirty_page_rate FROM pg_catalog.pgxc_stat_dirty_tables('||dirty_percent||','||n_tuples||','''||schema||''') GROUP BY (relname,schemaname)) s
						WHERE p.relname = s.relname AND p.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = s.schemaname) ORDER BY dirty_page_rate DESC';
		FOR row_data IN EXECUTE(query_str) LOOP
			relid = row_data.relid;
			relname = row_data.relname;
			schemaname = row_data.schemaname;
			n_tup_ins = row_data.n_tup_ins;
			n_tup_upd = row_data.n_tup_upd;
			n_tup_del = row_data.n_tup_del;
			n_live_tup = row_data.n_live_tup;
			n_dead_tup = row_data.n_dead_tup;
			dirty_page_rate = row_data.dirty_page_rate;
			return next;
		END LOOP;
	END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE OR REPLACE VIEW pg_catalog.get_global_prepared_xacts AS
		SELECT p.transaction, p.gid, p.prepared, u.rolname AS owner, d.datname AS database, p.node_name
		FROM pg_catalog.get_local_prepared_xact() p
		LEFT JOIN pg_authid u ON p.ownerid = u.oid
		LEFT JOIN pg_database d ON p.dbid = d.oid
		UNION ALL
		SELECT * FROM pg_catalog.get_remote_prepared_xacts();

CREATE unlogged table statement_history(
    db_name name,
    schema_name name,
    origin_node integer,
    user_name name,
    application_name text,
    client_addr text,
    client_port integer,
    unique_query_id bigint,
    debug_query_id bigint,
    query text,
    start_time timestamp with time zone,
    finish_time timestamp with time zone,
    slow_sql_threshold bigint,
    transaction_id bigint,
    thread_id bigint,
    session_id bigint,
    n_soft_parse bigint,
    n_hard_parse bigint,
    query_plan text,
    n_returned_rows bigint,
    n_tuples_fetched bigint,
    n_tuples_returned bigint,
    n_tuples_inserted bigint,
    n_tuples_updated bigint,
    n_tuples_deleted bigint,
    n_blocks_fetched bigint,
    n_blocks_hit bigint,
    db_time bigint,
    cpu_time bigint,
    execution_time bigint,
    parse_time bigint,
    plan_time bigint,
    rewrite_time bigint,
    pl_execution_time bigint,
    pl_compilation_time bigint,
    data_io_time bigint,
    net_send_info text,
    net_recv_info text,
    net_stream_send_info text,
    net_stream_recv_info text,
    lock_count bigint,
    lock_time bigint,
    lock_wait_count bigint,
    lock_wait_time bigint,
    lock_max_count bigint,
    lwlock_count bigint,
    lwlock_wait_count bigint,
    lwlock_time bigint,
    lwlock_wait_time bigint,
    details bytea,
    is_slow_sql bool,
    trace_id text
);
REVOKE ALL on table pg_catalog.statement_history FROM public;
create index statement_history_time_idx on pg_catalog.statement_history USING btree (start_time, is_slow_sql);

CREATE TABLE DBE_PLDEVELOPER.gs_source
(
    id oid,
    owner bigint,
    nspid oid,
    name name,
    type text,
    status boolean,
    src text
);
CREATE INDEX DBE_PLDEVELOPER.gs_source_id_idx ON DBE_PLDEVELOPER.gs_source USING btree(id);
CREATE INDEX DBE_PLDEVELOPER.gs_source_idx ON DBE_PLDEVELOPER.gs_source USING btree(owner, nspid, name, type);
GRANT SELECT,INSERT,UPDATE,DELETE ON TABLE DBE_PLDEVELOPER.gs_source TO PUBLIC;

CREATE TABLE DBE_PLDEVELOPER.gs_errors
(
    id oid,
    owner bigint,
    nspid oid,
    name name,
    type text,
    line int,
    src text
);
CREATE INDEX DBE_PLDEVELOPER.gs_errors_id_idx ON DBE_PLDEVELOPER.gs_source USING btree(id);
CREATE INDEX DBE_PLDEVELOPER.gs_errors_idx ON DBE_PLDEVELOPER.gs_errors USING btree(owner, nspid, name);
GRANT SELECT,INSERT,UPDATE,DELETE ON TABLE DBE_PLDEVELOPER.gs_errors TO PUBLIC;

CREATE OR REPLACE VIEW PG_CATALOG.SYS_DUMMY AS (SELECT 'X'::TEXT AS DUMMY);
GRANT SELECT ON TABLE SYS_DUMMY TO PUBLIC;

CREATE TYPE pg_catalog.bulk_exception as (error_index integer, error_code integer, error_message text);

CREATE VIEW pg_catalog.gs_db_privileges AS
    SELECT
        pg_catalog.pg_get_userbyid(roleid) AS rolename,
        privilege_type AS privilege_type,
        CASE
            WHEN admin_option THEN
                'yes'
            ELSE
                'no'
        END AS admin_option
    FROM pg_catalog.gs_db_privilege;

CREATE OR REPLACE VIEW pg_catalog.gs_gsc_memory_detail AS
    SELECT db_id, pg_catalog.sum(totalsize) AS totalsize, pg_catalog.sum(freesize) AS freesize, pg_catalog.sum(usedsize) AS usedsize
    FROM (
        SELECT 
        	CASE WHEN contextname like '%GlobalSysDBCacheEntryMemCxt%' THEN pg_catalog.substring(contextname, 29)
        	ELSE pg_catalog.substring(parent, 29) END AS db_id,
        	totalsize,
        	freesize,
        	usedsize
        FROM pg_catalog.pg_shared_memory_detail()  
        WHERE contextname LIKE '%GlobalSysDBCacheEntryMemCxt%' OR parent LIKE '%GlobalSysDBCacheEntryMemCxt%'
        )a 
    GROUP BY db_id;

CREATE OR REPLACE VIEW pg_catalog.gs_lsc_memory_detail AS
SELECT * FROM pg_catalog.pv_thread_memory_detail() WHERE contextname LIKE '%LocalSysCache%' OR parent LIKE '%LocalSysCache%';

CREATE VIEW pg_publication_tables AS
    SELECT
        P.pubname AS pubname,
        N.nspname AS schemaname,
        C.relname AS tablename
    FROM pg_publication P, pg_class C
         JOIN pg_namespace N ON (N.oid = C.relnamespace)
    WHERE C.oid IN (SELECT relid FROM pg_catalog.pg_get_publication_tables(P.pubname));

CREATE VIEW pg_stat_subscription AS
    SELECT
            su.oid AS subid,
            su.subname,
            st.pid,
            st.received_lsn,
            st.last_msg_send_time,
            st.last_msg_receipt_time,
            st.latest_end_lsn,
            st.latest_end_time
    FROM pg_subscription su
            LEFT JOIN pg_catalog.pg_stat_get_subscription(NULL) st
                      ON (st.subid = su.oid);

CREATE VIEW pg_replication_origin_status AS
    SELECT *
    FROM pg_catalog.pg_show_replication_origin_status();

REVOKE ALL ON pg_replication_origin_status FROM public;

REVOKE ALL ON pg_subscription FROM public;
