-- setup
create table SQLLDR_TBL
(
    ID   NUMBER,
    NAME VARCHAR2(20),
    CON  VARCHAR2(20),
    DT   DATE
);

select copy_summary_create();
select copy_error_log_create();

-- comments of load data
load truncate into table sqlldr_tbl
-- comments in load data
fields terminated by ','
TRAILING NULLCOLS;

load data truncate into table sqlldr_tbl fields terminated by ',';
load append into table sqlldr_tbl fields terminated '|';
load data replace into table sqlldr_tbl fields terminated by '\t' TRAILING NULLCOLS;
load into table sqlldr_tbl fields terminated '|';
load data insert into table sqlldr_tbl fields terminated by '\t' TRAILING NULLCOLS;

load data infile 'test.csv' truncate into table sqlldr_tbl fields terminated by ',' TRAILING NULLCOLS;
load data infile 'test_ver.txt' append into table sqlldr_tbl fields terminated by '|';
load data infile 'test_tab.txt' replace into table sqlldr_tbl fields terminated '\t';
load data infile 'test_tab.txt' into table sqlldr_tbl fields terminated '\t';
load data infile 'test_tab.txt' insert into table sqlldr_tbl fields terminated '\t';

load data infile 'test.csv' truncate into table sqlldr_tbl fields terminated by ',' TRAILING NULLCOLS;
load data infile 'test_ver.txt' append into table sqlldr_tbl fields terminated by '|';
load data infile 'test_tab.txt' replace into table sqlldr_tbl fields terminated '\t';
load data infile 'test_ver.txt' into table sqlldr_tbl fields terminated by '|';
load data infile 'test_tab.txt' insert into table sqlldr_tbl fields terminated '\t';

load data infile 'test.csv' truncate into table sqlldr_tbl fields terminated by ',,' TRAILING NULLCOLS;
load data infile 'test_ver.txt' append into table sqlldr_tbl fields terminated by '||';
load data infile 'test_tab.txt' replace into table sqlldr_tbl fields terminated '\t\t';
load data infile 'test_ver.txt' into table sqlldr_tbl fields terminated by '||';
load data infile 'test_tab.txt' insert into table sqlldr_tbl fields terminated '\t\t';

-- characterset
load data characterset utf8 infile 'test_tab.txt' truncate into table sqlldr_tbl;
load data characterset 'utf8' infile 'test_tab.txt' replace into table sqlldr_tbl;
load data characterset "utf8" infile 'test_tab.txt' replace into table sqlldr_tbl;
load data characterset AL32UTF8 infile 'test_tab.txt' replace into table sqlldr_tbl;
load data characterset al32utf8 infile 'test_tab.txt' replace into table sqlldr_tbl;
load data characterset zhs16gbk infile 'test_tab.txt' replace into table sqlldr_tbl;
load data characterset zhs32gb18030 infile 'test_tab.txt' replace into table sqlldr_tbl;

-- when
load data infile "test.txt" truncate into table sqlldr_tbl WHEN (1-1) = '1' trailing nullcols;
load data infile "test.txt" truncate into table sqlldr_tbl WHEN (2-2) = '|' trailing nullcols;
load data infile "test.txt" truncate into table sqlldr_tbl WHEN (2-4) = 'XY' trailing nullcols;

-- load when exceptions
load data infile "test.txt" truncate into table sqlldr_tbl WHEN (0-1) = '1';
load data infile "test.txt" truncate into table sqlldr_tbl WHEN (2-0) = '|';
load data infile "test.txt" truncate into table sqlldr_tbl WHEN (2-1) = 'XY';
load data infile "test.txt" truncate into table sqlldr_tbl WHEN (-2-1) = 'XY';

-- copy when exceptions
\COPY sqlldr_tbl FROM STDIN ENCODING 'utf8' DELIMITER ',' WHEN (0-1) = '40';
\COPY sqlldr_tbl FROM STDIN ENCODING 'utf8' DELIMITER ',' WHEN (2-0) = '40';
\COPY sqlldr_tbl FROM STDIN ENCODING 'utf8' DELIMITER ',' WHEN (3-1) = '40';
\COPY sqlldr_tbl FROM STDIN ENCODING 'utf8' DELIMITER ',' WHEN (-3-1) = '40';

-- options
OPTIONS() load data infile "test.txt" truncate into table sqlldr_tbl;
OPTIONS(skip=-1) load data infile "test.txt" truncate into table sqlldr_tbl;
OPTIONS(skip=0) load data infile "test.txt" truncate into table sqlldr_tbl;
OPTIONS(skip=100) load data infile "test.txt" truncate into table sqlldr_tbl;
OPTIONS(errors=-1) load data infile "test.txt" truncate into table sqlldr_tbl;
OPTIONS(errors=2) load data infile "test.txt" truncate into table sqlldr_tbl;
OPTIONS(errors=10) load data infile "test.txt" truncate into table sqlldr_tbl;
OPTIONS(data='file.csv') load data infile "test.txt" truncate into table sqlldr_tbl;
OPTIONS(data="file.csv") load data infile "test.txt" truncate into table sqlldr_tbl;
OPTIONS(data="file.csv", skip=10, errors=64)  load data infile "test.txt" truncate into table sqlldr_tbl;

-- teardown
drop table sqlldr_tbl;
