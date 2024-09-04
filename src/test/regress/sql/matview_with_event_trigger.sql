create database matview_with_event_trigger;

\c matview_with_event_trigger

create table testTab1
(
    menu_name text,
    menu_id varchar2(100),
    ISLEAF varchar(100),
    LEVELS varchar(100),
    function_url varchar2(500),
    menu_parent varchar2(100),
    menu_listorder clob,
    menu_departmentid char(5),
    menu_functionid numeric,
    menu_disabled int,
    primary key(menu_name,menu_parent)
);

create table testTab2 (
    function_moduleid int,
    function_id numeric,
    function_interact varchar(100),
    function_name varchar(100),
    function_description varchar(100)
);

insert into testTab1 values('蔬菜','1','0','0','https://www.tapd.cn/60475194/sparrow/tcase/view/1160475194001155488?url_cache_key=from_urlaaaaa%23','6','ASD-20240112001','A1','3.14159',0);
insert into testTab1 values('土豆','2','1','1','https://www.tapd.cn/60475194/sparrow/tcase/view/11604755488?url_cache_key=from_urlaaaaa%23','6','ASD-20240112001','A1','3.1415926535',0);
insert into testTab1 values('动物','3','0','0','htt4788?url_cache_key=from_urlaaaaa%23%%','3','ASD-20240112002','A2','3.14159',0);
insert into testTab1 values('蜗牛','4','1','1','http://4788?url_cache_key=from_urlaaaaa%23%%','3','ASD-20240112002','A2','3.14159',0);
insert into testTab1 values('青菜','5','1','1','http://4788?url_cache_key=fr_%23%%---//###@!','1','ASD-20240112003','A2','3.1415926535',0);

create publication pub_test for all tables with (ddl='all');

CREATE MATERIALIZED VIEW test_mv1
(
    NAME,
    ROOT,
    ISLEAF,
    LEVELS,
    PATH,
    URL,
    ID,
    PID,
    FUNCTION_MODULEID,
    FUNCTION_ID,
    LISTORDER,
    DEPARTMENTID,
    INTERACT,
    MENU_DEPARTMENTID,
    FUNCTION_NAME,
    FUNCTION_DESCRIPTION
) with (STORAGE_TYPE=ustore, INIT_TD=40)
AS
SELECT menu_name AS name, null, null, null, null, function_url AS url
, menu_id AS id, menu_parent AS pid, f.function_moduleid, f.function_id, menu_listorder AS listorder
, m.menu_departmentid AS departmentid, f.function_interact AS interact, menu_departmentid, f.function_name, f.function_description
FROM testTab1 m
LEFT JOIN testTab2 f ON m.menu_functionid = f.function_id;

select * from test_mv1;

\c regression
drop database matview_with_event_trigger;
