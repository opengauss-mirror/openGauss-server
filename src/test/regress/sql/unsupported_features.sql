--unsupported feature: sequence functions.
CREATE TABLE SEQ_CURRVAL_TABLE_009(COL_NAME VARCHAR2(20),COL_NUM INTEGER) ;
ALTER TABLE SEQ_CURRVAL_TABLE_009 ADD PRIMARY KEY(COL_NUM);
CREATE SEQUENCE CURRVAL_SEQUENCE_009 INCREMENT BY 100 START WITH 1;

explain (costs off) INSERT INTO SEQ_CURRVAL_TABLE_009 VALUES('nextval',nextval('CURRVAL_SEQUENCE_009'));
INSERT INTO SEQ_CURRVAL_TABLE_009 VALUES('nextval',nextval('CURRVAL_SEQUENCE_009'));
INSERT INTO SEQ_CURRVAL_TABLE_009 VALUES('nextval',nextval('CURRVAL_SEQUENCE_009'));
SELECT * FROM SEQ_CURRVAL_TABLE_009 ORDER BY COL_NAME,COL_NUM;
explain (costs off) UPDATE  SEQ_CURRVAL_TABLE_009 SET COL_NAME='new' WHERE COL_NUM=currval('CURRVAL_SEQUENCE_009');
UPDATE  SEQ_CURRVAL_TABLE_009 SET COL_NAME='new' WHERE COL_NUM=currval('CURRVAL_SEQUENCE_009');
SELECT * FROM SEQ_CURRVAL_TABLE_009 ORDER BY COL_NUM,COL_NAME;
explain (costs off) UPDATE SEQ_CURRVAL_TABLE_009 SET COL_NUM=nextval('CURRVAL_SEQUENCE_009');
UPDATE SEQ_CURRVAL_TABLE_009 SET COL_NUM=nextval('CURRVAL_SEQUENCE_009');
SELECT * FROM SEQ_CURRVAL_TABLE_009 ORDER BY COL_NUM,COL_NAME;

-- unsupported feature: cannot broadcast hashed results for inner plan of semi join when outer plan is replicated now.
create schema uschema2_ix;
create table uschema2_ix.offers_20050701
(
    PROMO_ID VARCHAR(10) NOT NULL ,
    PARTY_ID VARCHAR(10) NULL,
    LOCATION_ID INTEGER NULL ,
    PARTY_FIRSTNAME VARCHAR(20) NULL ,
    PARTY_LASTNAME VARCHAR(20) NULL ,
    VISITS INTEGER NULL ,
    CLUB_LEVEL CHAR(7) NULL 
)
;

create table uschema2_ix.item_price_history
(
    ITEM_ID INTEGER NOT NULL ,
    ITEM_PRICE_START_DT DATE NOT NULL ,
    PRICE_CHANGE_REASON_CD VARCHAR(50) NULL ,
    ITEM_PRICE_AMT DECIMAL(18,2) NULL ,
    CURRENT_INDICATOR CHAR(1) NULL ,
    LOCATION_ID INTEGER NULL 
)
;

create table uschema2_ix.associate
(
    ASSOCIATE_PARTY_ID INTEGER NOT NULL ,
    MANAGER_ASSOCIATE_ID INTEGER NULL ,
    POSITION_ID NUMBER(10) NULL ,
    LOCATION_ID INTEGER NULL ,
    ASSOC_HR_NUM VARCHAR(50) NULL ,
    ASSOC_HIRE_DT DATE NULL ,
    ASSOC_TERMINATION_DT DATE NULL ,
    ASSOC_INTERN_DURATION interval  
)
;

create table uschema2_ix.associate_benefit_expense
(
    period_end_dt date not null ,
    associate_expns_type_cd varchar(50) not null ,
    associate_party_id integer not null ,
    benefit_hours_qty decimal(18,4) ,
    benefit_cost_amt number(18,4) 
)
;

create table uschema2_ix.associate_position
(
    position_id integer not null ,
    position_desc varchar(250) null ,
    wage_rate_amt real not null ,
    overtime_rate_pct float ,
    position_grade_cd varchar(50) null ,
    position_type_cd varchar(50) null ,
    job_classification_cd varchar(50) null,
    postion_duration timestamp
)
;

create table uschema2_ix.position_type
(
    position_type_cd varchar(50) not null ,
    position_type_desc varchar(250) null
)
;

explain (costs off) 
select t1.ITEM_PRICE_START_DT,
t0.PARTY_ID
from uschema2_ix.OFFERS_20050701 t0
left join uschema2_ix.ITEM_PRICE_HISTORY t1
	on t0.PROMO_ID < t1.ITEM_ID
and t1.ITEM_ID in ( select t0.LOcation_ID from uschema2_ix.ASSOCIATE t0 
                        inner join uschema2_ix.ASSOCIATE_BENEFIT_EXPENSE t1
                        on t0.ASSOCIATE_PARTY_ID = t1.ASSOCIATE_PARTY_ID
                        where t0.POSITION_ID in (select t1.POSITION_ID 
                                                      from uschema2_ix.ASSOCIATE_POSITION t1 
                                                      left join uschema2_ix.POSITION_TYPE t2
                                                      on t1.POSITION_TYPE_CD = t2.POSITION_TYPE_CD
                                                      ));

-- can't push down rowexpr
explain (costs off, verbose on)
select position_type_desc, count(*) from uschema2_ix.position_type
group by case when length(position_type_cd)>5 then (length(position_type_desc)-20, length(position_type_cd)+1)
else (length(position_type_desc)-10, length(position_type_cd)+2) end, 1;
drop schema uschema2_ix cascade;

create schema create_resource;
set current_schema='create_resource';
create table tab(a text);
create resource label name;
alter resource label name add column(tab.a);

drop table tab;
reset current_schema;
drop schema create_resource CASCADE;

-- roach function
select gs_roach_stop_backup('test');
select gs_roach_switch_xlog(false);
select gs_roach_enable_delay_ddl_recycle('test');
select gs_roach_disable_delay_ddl_recycle('test');