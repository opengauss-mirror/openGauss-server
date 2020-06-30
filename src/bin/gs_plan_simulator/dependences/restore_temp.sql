--
-- PostgreSQL database dump
--

SET statement_timeout = 0;
SET client_encoding = 'SQL_ASCII';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;

SET search_path = public, pg_catalog;

--
-- Name: restore_pg_class_stats(boolean); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION restore_pg_class_stats(skip_nonexist boolean) RETURNS void
    LANGUAGE plpgsql
    AS $$ DECLARE 	rel_pages INTEGER;
						rel_tuples DOUBLE PRECISION;
						rel_allvisible INTEGER;
						rel_name NAME;
						nsp_name NAME;
						nsp_oid OID;
						rel_oid OID;
						exist_flag INTEGER;
						
						CURSOR stats IS SELECT relname, nspname, relpages, reltuples, relallvisible FROM pg_class_stats;
					BEGIN
						OPEN stats;
						LOOP
							FETCH stats INTO rel_name, nsp_name, rel_pages, rel_tuples, rel_allvisible;
							exit when stats%notfound;

							exist_flag := 0;
							SELECT count(*) FROM pg_namespace WHERE nspname = nsp_name INTO exist_flag;
							IF exist_flag <> 0 THEN
								SELECT oid FROM pg_namespace WHERE nspname = nsp_name INTO nsp_oid;
							ELSIF skip_nonexist THEN
								CONTINUE;
							ELSE
								RAISE EXCEPTION 'Failed to found namespace: %', nsp_name;
							END IF;

							exist_flag := 0;
							SELECT count(*) FROM pg_class WHERE relnamespace = nsp_oid and relname = rel_name INTO exist_flag;
							IF exist_flag <> 0 THEN
								SELECT oid FROM pg_class WHERE relnamespace = nsp_oid and relname = rel_name INTO rel_oid;
							ELSIF skip_nonexist THEN
								CONTINUE;
							ELSE
								RAISE EXCEPTION 'Failed to found table: %.%', nsp_name,rel_name;
							END IF;

							UPDATE pg_class SET (relpages, reltuples, relallvisible) = (rel_pages, rel_tuples, rel_allvisible) WHERE oid = rel_oid;
						END LOOP;
					END$$;


--
-- Name: restore_pg_statistic_ext_stats(boolean); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION restore_pg_statistic_ext_stats(skip_nonexist boolean) RETURNS void
    LANGUAGE plpgsql
    AS $$ DECLARE 	cursor_exist INT;
						cursor_relid OID;
						cursor_nspoid OID;
						cursor_starelkind CHAR;
						cursor_stakey INT2VECTOR;
						cursor_relname NAME;
						cursor_relnspname NAME;
						cursor_typname NAME;
						cursor_typnspname NAME;
						cursor_stainherit BOOL;
						cursor_stanullfrac FLOAT4;
						cursor_stawidth INT4;
						cursor_stadistinct FLOAT4;
						cursor_stadndistinct FLOAT4;
						cursor_stakind1 INT2;
						cursor_stakind2 INT2;
						cursor_stakind3 INT2;
						cursor_stakind4 INT2;
						cursor_stakind5 INT2;
						cursor_staop1 OID;
						cursor_staop2 OID;
						cursor_staop3 OID;
						cursor_staop4 OID;
						cursor_staop5 OID;
						cursor_stanumbers1 TEXT;
						cursor_stanumbers2 TEXT;
						cursor_stanumbers3 TEXT;
						cursor_stanumbers4 TEXT;
						cursor_stanumbers5 TEXT;
						cursor_stavalues1 TEXT;
						cursor_stavalues2 TEXT;
						cursor_stavalues3 TEXT;
						cursor_stavalues4 TEXT;
						cursor_stavalues5 TEXT;

						CURSOR cursor_statistic IS SELECT
						relname,relnspname,stakey,starelkind,stainherit,stanullfrac,stawidth,stadistinct,stadndistinct,
						stakind1,stakind2,stakind3,stakind4,stakind5,
						staop1,staop2,staop3,staop4,staop5,
						stanumbers1,stanumbers2,stanumbers3,stanumbers4,stanumbers5,
						stavalues1,stavalues2,stavalues3,stavalues4,stavalues5
						FROM PG_STATISTIC_EXT_STATS;
					BEGIN
						OPEN cursor_statistic;
						LOOP
							FETCH cursor_statistic INTO cursor_relname,cursor_relnspname,cursor_stakey,cursor_starelkind,cursor_stainherit,cursor_stanullfrac,cursor_stawidth,cursor_stadistinct,cursor_stadndistinct,cursor_stakind1,cursor_stakind2,cursor_stakind3,cursor_stakind4,cursor_stakind5,cursor_staop1,cursor_staop2,cursor_staop3,cursor_staop4,cursor_staop5,cursor_stanumbers1,cursor_stanumbers2,cursor_stanumbers3,cursor_stanumbers4,cursor_stanumbers5,cursor_stavalues1,cursor_stavalues2,cursor_stavalues3,cursor_stavalues4,cursor_stavalues5;
							EXIT WHEN cursor_statistic%NOTFOUND;

							cursor_exist := 0;
							SELECT count(*) FROM pg_namespace WHERE nspname = cursor_relnspname INTO cursor_exist;
							IF cursor_exist <> 0 THEN
								SELECT oid FROM pg_namespace WHERE nspname = cursor_relnspname INTO cursor_nspoid;
							ELSIF skip_nonexist THEN
								CONTINUE;
							ELSE
								RAISE EXCEPTION 'Failed to found namespace: %', cursor_relnspname;
							END IF;

							cursor_exist := 0;
							SELECT count(*) FROM pg_class where relname=cursor_relname AND relnamespace=cursor_nspoid INTO cursor_exist;
							IF cursor_exist <> 0 then
								SELECT oid FROM pg_class where relname=cursor_relname AND relnamespace=cursor_nspoid INTO cursor_relid;
							ELSIF skip_nonexist THEN
								CONTINUE;
							ELSE
								RAISE EXCEPTION 'Failed to found table: %.%', cursor_relname,cursor_relname;
							END IF;

							cursor_exist := 0;
							SELECT count(*) FROM pg_statistic_ext WHERE starelid = cursor_relid AND stakey = cursor_stakey and stainherit = cursor_stainherit INTO cursor_exist;

							IF cursor_exist <> 0 THEN
								UPDATE pg_statistic_ext SET (stainherit,stanullfrac,stawidth,stadistinct,stadndistinct) = (cursor_stainherit,cursor_stanullfrac,cursor_stawidth,cursor_stadistinct,cursor_stadndistinct)
								WHERE starelid = cursor_relid AND stakey = cursor_stakey;

								UPDATE pg_statistic_ext SET (stakind1,stakind2,stakind3,stakind4,stakind5) = (cursor_stakind1,cursor_stakind2,cursor_stakind3,cursor_stakind4,cursor_stakind5)
								WHERE starelid = cursor_relid AND stakey = cursor_stakey;

								UPDATE pg_statistic_ext SET (staop1,staop2,staop3,staop4,staop5) = (cursor_staop1,cursor_staop2,cursor_staop3,cursor_staop4,cursor_staop5)
								WHERE starelid = cursor_relid AND stakey = cursor_stakey;
							ELSE
								INSERT INTO 
								pg_statistic_ext (starelid,stakey,starelkind,stainherit,stanullfrac,stawidth,stadistinct,stadndistinct,stakind1,stakind2,stakind3,stakind4,stakind5,staop1,staop2,staop3,staop4,staop5)
								VALUES
								(cursor_relid,cursor_stakey,cursor_starelkind,cursor_stainherit,cursor_stanullfrac,cursor_stawidth,cursor_stadistinct,cursor_stadndistinct,cursor_stakind1,cursor_stakind2,cursor_stakind3,cursor_stakind4,cursor_stakind5,cursor_staop1,cursor_staop2,cursor_staop3,cursor_staop4,cursor_staop5);
							END IF;

							IF cursor_stanumbers1 != '' THEN
								UPDATE pg_statistic_ext SET stanumbers1 = cursor_stanumbers1::real[] WHERE starelid = cursor_relid AND stakey = cursor_stakey;
							END IF;

							IF cursor_stanumbers2 != '' THEN
								UPDATE pg_statistic_ext SET stanumbers2 = cursor_stanumbers2::real[] WHERE starelid = cursor_relid AND stakey = cursor_stakey;
							END IF;

							IF cursor_stanumbers3 != '' THEN
								UPDATE pg_statistic_ext SET stanumbers3 = cursor_stanumbers3::real[] WHERE starelid = cursor_relid AND stakey = cursor_stakey;
							END IF;

							IF cursor_stanumbers4 != '' THEN
								UPDATE pg_statistic_ext SET stanumbers4 = cursor_stanumbers4::real[] WHERE starelid = cursor_relid AND stakey = cursor_stakey;
							END IF;

							IF cursor_stanumbers5 != '' THEN
								UPDATE pg_statistic_ext SET stanumbers5 = cursor_stanumbers5::real[] WHERE starelid = cursor_relid AND stakey = cursor_stakey;
							END IF;


							if cursor_stavalues1 != '' THEN
								UPDATE pg_statistic_ext SET stavalues1 = complex_array_in(cursor_stavalues1::cstring, cursor_relid, cursor_stakey) WHERE starelid = cursor_relid AND stakey = cursor_stakey;
							END IF;
							if cursor_stavalues2 != '' THEN
								UPDATE pg_statistic_ext SET stavalues2 = complex_array_in(cursor_stavalues2::cstring, cursor_relid, cursor_stakey) WHERE starelid = cursor_relid AND stakey = cursor_stakey;
							END IF;
							if cursor_stavalues3 != '' THEN
								UPDATE pg_statistic_ext SET stavalues3 = complex_array_in(cursor_stavalues3::cstring, cursor_relid, cursor_stakey) WHERE starelid = cursor_relid AND stakey = cursor_stakey;
							END IF;
							if cursor_stavalues4 != '' THEN
								Update pg_statistic_ext SET stavalues4 = complex_array_in(cursor_stavalues4::cstring, cursor_relid, cursor_stakey) WHERE starelid = cursor_relid And stakey = cursor_stakey;
							END IF;
							if cursor_stavalues5 != '' THEN
								UPDATE pg_statistic_ext SET stavalues5 = complex_array_in(cursor_stavalues5::cstring, cursor_relid, cursor_stakey) WHERE starelid = cursor_relid AND stakey = cursor_stakey;
							END IF;
						END LOOP;
						CLOSE cursor_statistic;
					END$$;


--
-- Name: restore_pg_statistic_stats(boolean); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION restore_pg_statistic_stats(skip_nonexist boolean) RETURNS void
    LANGUAGE plpgsql
    AS $$ DECLARE 	cursor_exist INT;
						cursor_relid OID;
						cursor_nspoid OID;
						cursor_starelkind CHAR;
						cursor_staattnum INT2;
						cursor_relname NAME;
						cursor_relnspname NAME;
						cursor_typname NAME;
						cursor_typnspname NAME;
						cursor_stainherit BOOL;
						cursor_stanullfrac FLOAT4;
						cursor_stawidth INT4;
						cursor_stadistinct FLOAT4;
						cursor_stadndistinct FLOAT4;
						cursor_stakind1 INT2;
						cursor_stakind2 INT2;
						cursor_stakind3 INT2;
						cursor_stakind4 INT2;
						cursor_stakind5 INT2;
						cursor_staop1 OID;
						cursor_staop2 OID;
						cursor_staop3 OID;
						cursor_staop4 OID;
						cursor_staop5 OID;
						cursor_stanumbers1 TEXT;
						cursor_stanumbers2 TEXT;
						cursor_stanumbers3 TEXT;
						cursor_stanumbers4 TEXT;
						cursor_stanumbers5 TEXT;
						cursor_stavalues1 TEXT;
						cursor_stavalues2 TEXT;
						cursor_stavalues3 TEXT;
						cursor_stavalues4 TEXT;
						cursor_stavalues5 TEXT;

						atttypid OID;
						attnspid OID;

						CURSOR cursor_statistic IS SELECT
						relname,relnspname,staattnum,starelkind,typname,typnspname,stainherit,stanullfrac,stawidth,stadistinct,stadndistinct,
						stakind1,stakind2,stakind3,stakind4,stakind5,
						staop1,staop2,staop3,staop4,staop5,
						stanumbers1,stanumbers2,stanumbers3,stanumbers4,stanumbers5,
						stavalues1,stavalues2,stavalues3,stavalues4,stavalues5
						FROM PG_STATISTIC_STATS;
					BEGIN
						OPEN cursor_statistic;
						LOOP
							FETCH cursor_statistic INTO cursor_relname,cursor_relnspname,cursor_staattnum,cursor_starelkind,cursor_typname,cursor_typnspname,cursor_stainherit,cursor_stanullfrac,cursor_stawidth,cursor_stadistinct,cursor_stadndistinct,cursor_stakind1,cursor_stakind2,cursor_stakind3,cursor_stakind4,cursor_stakind5,cursor_staop1,cursor_staop2,cursor_staop3,cursor_staop4,cursor_staop5,cursor_stanumbers1,cursor_stanumbers2,cursor_stanumbers3,cursor_stanumbers4,cursor_stanumbers5,cursor_stavalues1,cursor_stavalues2,cursor_stavalues3,cursor_stavalues4,cursor_stavalues5;
							EXIT WHEN cursor_statistic%NOTFOUND;

							cursor_exist := 0;
							SELECT count(*) FROM pg_namespace WHERE nspname = cursor_relnspname INTO cursor_exist;
							IF cursor_exist <> 0 THEN
								SELECT oid FROM pg_namespace WHERE nspname = cursor_relnspname INTO cursor_nspoid;
							ELSIF skip_nonexist THEN
								CONTINUE;
							ELSE
								RAISE EXCEPTION 'Failed to found namespace: %', cursor_relnspname;
							END IF;

							cursor_exist := 0;
							SELECT count(*) FROM pg_class where relname=cursor_relname AND relnamespace=cursor_nspoid INTO cursor_exist;
							IF cursor_exist <> 0 then
								SELECT oid FROM pg_class where relname=cursor_relname AND relnamespace=cursor_nspoid INTO cursor_relid;
							ELSIF skip_nonexist THEN
								CONTINUE;
							ELSE
								RAISE EXCEPTION 'Failed to found table: %.%', cursor_relname,cursor_relname;
							END IF;

							cursor_exist := 0;
							SELECT count(*) FROM pg_statistic WHERE starelid = cursor_relid AND staattnum = cursor_staattnum and stainherit = cursor_stainherit INTO cursor_exist;

							IF cursor_exist <> 0 THEN
								UPDATE pg_statistic SET (stainherit,stanullfrac,stawidth,stadistinct,stadndistinct) = (cursor_stainherit,cursor_stanullfrac,cursor_stawidth,cursor_stadistinct,cursor_stadndistinct)
								WHERE starelid = cursor_relid AND staattnum = cursor_staattnum;

								UPDATE pg_statistic SET (stakind1,stakind2,stakind3,stakind4,stakind5) = (cursor_stakind1,cursor_stakind2,cursor_stakind3,cursor_stakind4,cursor_stakind5)
								WHERE starelid = cursor_relid AND staattnum = cursor_staattnum;

								UPDATE pg_statistic SET (staop1,staop2,staop3,staop4,staop5) = (cursor_staop1,cursor_staop2,cursor_staop3,cursor_staop4,cursor_staop5)
								WHERE starelid = cursor_relid AND staattnum = cursor_staattnum;
							ELSE
								INSERT INTO 
								pg_statistic (starelid,staattnum,starelkind,stainherit,stanullfrac,stawidth,stadistinct,stadndistinct,stakind1,stakind2,stakind3,stakind4,stakind5,staop1,staop2,staop3,staop4,staop5)
								VALUES
								(cursor_relid,cursor_staattnum,cursor_starelkind,cursor_stainherit,cursor_stanullfrac,cursor_stawidth,cursor_stadistinct,cursor_stadndistinct,cursor_stakind1,cursor_stakind2,cursor_stakind3,cursor_stakind4,cursor_stakind5,cursor_staop1,cursor_staop2,cursor_staop3,cursor_staop4,cursor_staop5);
							END IF;

							IF cursor_stanumbers1 != '' THEN
								UPDATE pg_statistic SET stanumbers1 = cursor_stanumbers1::real[] WHERE starelid = cursor_relid AND staattnum = cursor_staattnum;
							END IF;

							IF cursor_stanumbers2 != '' THEN
								UPDATE pg_statistic SET stanumbers2 = cursor_stanumbers2::real[] WHERE starelid = cursor_relid AND staattnum = cursor_staattnum;
							END IF;

							IF cursor_stanumbers3 != '' THEN
								UPDATE pg_statistic SET stanumbers3 = cursor_stanumbers3::real[] WHERE starelid = cursor_relid AND staattnum = cursor_staattnum;
							END IF;

							IF cursor_stanumbers4 != '' THEN
								UPDATE pg_statistic SET stanumbers4 = cursor_stanumbers4::real[] WHERE starelid = cursor_relid AND staattnum = cursor_staattnum;
							END IF;

							IF cursor_stanumbers5 != '' THEN
								UPDATE pg_statistic SET stanumbers5 = cursor_stanumbers5::real[] WHERE starelid = cursor_relid AND staattnum = cursor_staattnum;
							END IF;

							cursor_exist := 0;
							SELECT count(*) FROM pg_namespace where nspname = cursor_typnspname INTO cursor_exist;
							IF cursor_exist <> 0 THEN
								SELECT oid FROM pg_namespace where nspname = cursor_typnspname INTO attnspid;
							ELSIF skip_nonexist THEN
								CONTINUE;
							ELSE
								RAISE EXCEPTION 'Failed to found namespace: %', cursor_typnspname;
							END IF;

							cursor_exist := 0;
							SELECT count(*) FROM pg_type where typname=cursor_typname AND typnamespace=attnspid INTO cursor_exist;
							IF cursor_exist <> 0 then
								SELECT oid FROM pg_type where typname=cursor_typname AND typnamespace=attnspid INTO atttypid;
							ELSIF skip_nonexist THEN
								CONTINUE;
							ELSE
								RAISE EXCEPTION 'Failed to found type: %.%', cursor_typnspname,cursor_typname;
							END IF;

							if cursor_stavalues1 != '' THEN
								UPDATE pg_statistic SET stavalues1 = array_in(cursor_stavalues1::cstring, atttypid, -1) WHERE starelid = cursor_relid AND staattnum = cursor_staattnum;
							END IF;
							if cursor_stavalues2 != '' THEN
								UPDATE pg_statistic SET stavalues2 = array_in(cursor_stavalues2::cstring, atttypid, -1) WHERE starelid = cursor_relid AND staattnum = cursor_staattnum;
							END IF;
							if cursor_stavalues3 != '' THEN
								UPDATE pg_statistic SET stavalues3 = array_in(cursor_stavalues3::cstring, atttypid, -1) WHERE starelid = cursor_relid AND staattnum = cursor_staattnum;
							END IF;
							if cursor_stavalues4 != '' THEN
								Update pg_statistic SET stavalues4 = array_in(cursor_stavalues4::cstring, atttypid, -1) WHERE starelid = cursor_relid And staattnum = cursor_staattnum;
							END IF;
							if cursor_stavalues5 != '' THEN
								UPDATE pg_statistic SET stavalues5 = array_in(cursor_stavalues5::cstring, atttypid, -1) WHERE starelid = cursor_relid AND staattnum = cursor_staattnum;
							END IF;
						END LOOP;
						CLOSE cursor_statistic;
					END$$;


SET default_tablespace = '';

SET default_with_oids = false;




--
-- Name: pg_class_stats; Type: TABLE; Schema: public; Owner: -; Tablespace: 
--

CREATE UNLOGGED TABLE pg_class_stats (
    relname name,
    nspname name,
    relpages double precision,
    reltuples double precision,
    relallvisible integer
)
WITH (orientation=row, compression=no)
DISTRIBUTE BY HASH (relallvisible);


--
-- Name: pg_statistic_ext_stats; Type: TABLE; Schema: public; Owner: -; Tablespace: 
--

CREATE UNLOGGED TABLE pg_statistic_ext_stats (
    relname name,
    relnspname name,
    stakey int2vector,
    starelkind "char",
    stainherit boolean,
    stanullfrac real,
    stawidth integer,
    stadistinct real,
    stadndistinct real,
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
    stanumbers1 text,
    stanumbers2 text,
    stanumbers3 text,
    stanumbers4 text,
    stanumbers5 text,
    stavalues1 text,
    stavalues2 text,
    stavalues3 text,
    stavalues4 text,
    stavalues5 text
)
WITH (orientation=row, compression=no)
DISTRIBUTE BY HASH (starelkind);


--
-- Name: pg_statistic_stats; Type: TABLE; Schema: public; Owner: -; Tablespace: 
--

CREATE UNLOGGED TABLE pg_statistic_stats (
    relname name,
    relnspname name,
    staattnum smallint,
    starelkind "char",
    typname name,
    typnspname name,
    stainherit boolean,
    stanullfrac real,
    stawidth integer,
    stadistinct real,
    stadndistinct real,
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
    stanumbers1 text,
    stanumbers2 text,
    stanumbers3 text,
    stanumbers4 text,
    stanumbers5 text,
    stavalues1 text,
    stavalues2 text,
    stavalues3 text,
    stavalues4 text,
    stavalues5 text
)
WITH (orientation=row, compression=no)
DISTRIBUTE BY HASH (staattnum);

--
-- Name: pg_class_stats; Type: TABLE; Schema: public; Owner: -; Tablespace: 
--

CREATE UNLOGGED TABLE pgxc_class_ng (
    relname name,
    nspname name,
	pgroup name,
	relallvisible integer
)
WITH (orientation=row, compression=no)
DISTRIBUTE BY HASH (relallvisible);

CREATE FUNCTION restore_pgxc_class_ng(skip_nonexist boolean) RETURNS void
	LANGUAGE plpgsql
    AS $$ DECLARE	rel_name NAME;
						nsp_name NAME;
						p_group NAME;
						nsp_oid OID;
						rel_oid OID;
						rel_allvisible INTEGER;
						exist_flag INTEGER;
						
					
						CURSOR stats IS SELECT relname, nspname, pgroup, relallvisible FROM pgxc_class_ng;
					BEGIN
						OPEN stats;
						LOOP
							FETCH stats INTO rel_name, nsp_name, p_group, rel_allvisible;
							exit when stats%notfound;
							
							exist_flag := 0;
							SELECT count(*) FROM pg_class WHERE relname = rel_name INTO exist_flag;
							IF exist_flag <> 0 THEN
								SELECT C.oid FROM pg_class C, pg_namespace N WHERE C.relname = rel_name AND N.oid = C.relnamespace AND N.nspname = nsp_name INTO rel_oid;
								UPDATE pgxc_class SET (pgroup) = (p_group) WHERE pcrelid = rel_oid;
							ELSIF skip_nonexist THEN
								CONTINUE;
								RAISE EXCEPTION 'Failed to find relname: %', rel_name;
							END IF;
							
							
							
						END LOOP;
					END$$;


CREATE FUNCTION restore_nodeoids(skip_nonexist boolean) RETURNS void
	LANGUAGE plpgsql
    AS $$ DECLARE	groupname NAME;
						groupmembers oidvector;
						exist_flag INTEGER;
					
						CURSOR stats IS SELECT group_name, group_members FROM pgxc_group;
					BEGIN
						OPEN stats;
						LOOP
							FETCH stats INTO groupname, groupmembers;
							exit when stats%notfound;
							
							exist_flag := 0;
							SELECT count(*) FROM pgxc_class WHERE pgroup = groupname INTO exist_flag;
							IF exist_flag <> 0 THEN
								UPDATE pgxc_class SET nodeoids = groupmembers WHERE pgroup = groupname;
							ELSIF skip_nonexist THEN
								CONTINUE;
							ELSE
								RAISE EXCEPTION 'Failed to find pgroup: %', groupname;
							END IF;
							
						END LOOP;
					END$$;
