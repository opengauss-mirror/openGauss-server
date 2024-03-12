create database prefix_index_db WITH ENCODING 'UTF-8' LC_COLLATE='C' LC_CTYPE='C' dbcompatibility 'B';
\c prefix_index_db

--
-- CREATE_INDEX
--
-- test datatype
CREATE TABLE test_prefix_datatype (
    f1 int,
    f2 money,
    f3 boolean,
    f4 date,
    f5 point,
    f6 cidr,
    f7 bit(64),
    f8 json,
    f9 NAME,
    f10 CHAR(64),
    f11 VARCHAR(64),
    f12 VARCHAR2(64),
    f13 NVARCHAR2(64),
    f14 NVARCHAR(64),
    f15 TEXT,
    f16 CLOB,
    f17 "char",
    f18 BLOB,
    f19 RAW,
    f20 BYTEA
);

CREATE INDEX idx_prefix_f1 ON test_prefix_datatype (f1(4));
CREATE INDEX idx_prefix_f2 ON test_prefix_datatype (f2(4));
CREATE INDEX idx_prefix_f3 ON test_prefix_datatype (f3(4));
CREATE INDEX idx_prefix_f4 ON test_prefix_datatype (f4(4));
CREATE INDEX idx_prefix_f5 ON test_prefix_datatype (f5(4));
CREATE INDEX idx_prefix_f6 ON test_prefix_datatype (f6(4));
CREATE INDEX idx_prefix_f7 ON test_prefix_datatype (f7(4));
CREATE INDEX idx_prefix_f8 ON test_prefix_datatype (f8(4));
CREATE INDEX idx_prefix_f9 ON test_prefix_datatype (f9(4));
CREATE INDEX idx_prefix_f10 ON test_prefix_datatype (f10(65));
CREATE INDEX idx_prefix_f10 ON test_prefix_datatype (f10(4));
CREATE INDEX idx_prefix_f11 ON test_prefix_datatype (f11(65));
CREATE INDEX idx_prefix_f11 ON test_prefix_datatype (f11(4));
CREATE INDEX idx_prefix_f12 ON test_prefix_datatype (f12(65));
CREATE INDEX idx_prefix_f12 ON test_prefix_datatype (f12(4));
CREATE INDEX idx_prefix_f13 ON test_prefix_datatype (f13(65));
CREATE INDEX idx_prefix_f13 ON test_prefix_datatype (f13(4));
CREATE INDEX idx_prefix_f14 ON test_prefix_datatype (f14(65));
CREATE INDEX idx_prefix_f14 ON test_prefix_datatype (f14(4));
CREATE INDEX idx_prefix_f15 ON test_prefix_datatype (f15(4));
CREATE INDEX idx_prefix_f16 ON test_prefix_datatype (f16(4));
CREATE INDEX idx_prefix_f17 ON test_prefix_datatype (f17(4));
CREATE INDEX idx_prefix_f18 ON test_prefix_datatype (f18(4));
CREATE INDEX idx_prefix_f19 ON test_prefix_datatype (f19(4));
CREATE INDEX idx_prefix_f20 ON test_prefix_datatype (f20(4));

DROP TABLE test_prefix_datatype;

-- test syntax
CREATE TABLE test_prefix_syntax (
    fchar CHAR(64),
    BETWEEN VARCHAR(64),
    GREATEST TEXT,
    fblob BLOB
) ;

CREATE INDEX error_index_fchar ON test_prefix_syntax (fchar(0));
CREATE INDEX error_index_fchar ON test_prefix_syntax (fchar(-1));
CREATE INDEX error_index_fchar ON test_prefix_syntax (fchar(1+1));
CREATE INDEX error_index_fchar ON test_prefix_syntax (fchar(6.4));

CREATE INDEX error_index_BETWEEN ON test_prefix_syntax (BETWEEN(0));
CREATE INDEX error_index_BETWEEN ON test_prefix_syntax (BETWEEN(-1));
CREATE INDEX error_index_BETWEEN ON test_prefix_syntax (BETWEEN(1+1));
CREATE INDEX error_index_BETWEEN ON test_prefix_syntax (BETWEEN(6.4));
-- GREATEST cannot be parsed as prefix key yet
CREATE INDEX error_index_GREATEST1 ON test_prefix_syntax (GREATEST(0));
CREATE INDEX error_index_GREATEST2 ON test_prefix_syntax (GREATEST(-1));
CREATE INDEX error_index_GREATEST3 ON test_prefix_syntax (GREATEST(1+1));
CREATE INDEX error_index_GREATEST4 ON test_prefix_syntax (GREATEST(6.4));

CREATE INDEX error_index_fblob ON test_prefix_syntax using hash (fblob(5));
CREATE INDEX error_index_fblob ON test_prefix_syntax using gin (fblob(5));
CREATE INDEX error_index_fblob ON test_prefix_syntax using gist (fblob(5));

DROP TABLE test_prefix_syntax;

-- test btree prefix length
CREATE TABLE test_prefix_key_len (
    fchar CHAR(4096),
    ftext TEXT,
    fbytea BYTEA
);
insert into test_prefix_key_len
  select array_to_string(array(
    select '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmn'
    FROM generate_series(1, 80)), '') as col1, col1 as col2, convert_to(col2, 'UTF8') as col3;
select length(fchar),length(ftext),length(fbytea) from test_prefix_key_len;
select lengthb(fchar),lengthb(ftext),octet_length(fbytea) from test_prefix_key_len;
CREATE INDEX idx_prefix_len_test_fchar ON test_prefix_key_len (fchar(2677));
CREATE INDEX idx_prefix_len_test_ftext ON test_prefix_key_len (ftext(2677));
CREATE INDEX idx_prefix_len_test_fbytea ON test_prefix_key_len (fbytea(2677));
CREATE INDEX idx_prefix_len_test_fchar ON test_prefix_key_len (fchar(2676));
CREATE INDEX idx_prefix_len_test_ftext ON test_prefix_key_len (ftext(2676));
CREATE INDEX idx_prefix_len_test_fbytea ON test_prefix_key_len (fbytea(2676));
CREATE INDEX idx_prefix_len_test_comb ON test_prefix_key_len (fchar(2676),ftext(2676),fbytea(2676));
EXPLAIN (costs false)
SELECT /*+ indexscan(test_prefix_key_len idx_prefix_len_test_fchar) */count(1) FROM test_prefix_key_len WHERE fchar LIKE '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmn%';
SELECT /*+ indexscan(test_prefix_key_len idx_prefix_len_test_fchar) */count(1) FROM test_prefix_key_len WHERE fchar LIKE '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmn%';
EXPLAIN (costs false)
SELECT /*+ indexscan(test_prefix_key_len idx_prefix_len_test_ftext) */count(1) FROM test_prefix_key_len WHERE ftext LIKE '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmn%';
SELECT /*+ indexscan(test_prefix_key_len idx_prefix_len_test_ftext) */count(1) FROM test_prefix_key_len WHERE ftext LIKE '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmn%';
EXPLAIN (costs false)
SELECT /*+ indexscan(test_prefix_key_len idx_prefix_len_test_fbytea) */count(1) FROM test_prefix_key_len WHERE fbytea LIKE '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmn%';
SELECT /*+ indexscan(test_prefix_key_len idx_prefix_len_test_fbytea) */count(1) FROM test_prefix_key_len WHERE fbytea LIKE '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmn%';
EXPLAIN (costs false)
SELECT /*+ indexscan(test_prefix_key_len idx_prefix_len_test_comb) */count(1) FROM test_prefix_key_len WHERE ftext LIKE '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmn%';
SELECT /*+ indexscan(test_prefix_key_len idx_prefix_len_test_comb) */count(1) FROM test_prefix_key_len WHERE ftext LIKE '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmn%';
DROP TABLE test_prefix_key_len;

-- test btree prefix length with multibyte characters
CREATE TABLE test_prefix_key_len (
    fchar CHAR(4096),
    ftext TEXT,
    fbytea BYTEA
);
insert into test_prefix_key_len
  select array_to_string(array(
    select '一二三四五六七八九十百千万亿兆'
    FROM generate_series(1, 200)), '') as col1, col1 as col2, convert_to(col2, 'UTF8') as col3;
select length(fchar),length(ftext),length(fbytea) from test_prefix_key_len;
select lengthb(fchar),lengthb(ftext),octet_length(fbytea) from test_prefix_key_len;
CREATE INDEX idx_prefix_len_test_fchar ON test_prefix_key_len (fchar(2677));
CREATE INDEX idx_prefix_len_test_ftext ON test_prefix_key_len (ftext(2677));
CREATE INDEX idx_prefix_len_test_fbytea ON test_prefix_key_len (fbytea(2677));
CREATE INDEX idx_prefix_len_test_fchar ON test_prefix_key_len (fchar(2676));
CREATE INDEX idx_prefix_len_test_ftext ON test_prefix_key_len (ftext(2676));
CREATE INDEX idx_prefix_len_test_fbytea ON test_prefix_key_len (fbytea(2676));
CREATE INDEX idx_prefix_len_test_comb ON test_prefix_key_len (fchar(2676),ftext(2676),fbytea(2676));
EXPLAIN (costs false)
SELECT /*+ indexscan(test_prefix_key_len idx_prefix_len_test_fchar) */count(1) FROM test_prefix_key_len WHERE fchar LIKE '一二三四五六七八九十百千万亿兆%';
SELECT /*+ indexscan(test_prefix_key_len idx_prefix_len_test_fchar) */count(1) FROM test_prefix_key_len WHERE fchar LIKE '一二三四五六七八九十百千万亿兆%';
EXPLAIN (costs false)
SELECT /*+ indexscan(test_prefix_key_len idx_prefix_len_test_ftext) */count(1) FROM test_prefix_key_len WHERE ftext LIKE '一二三四五六七八九十百千万亿兆%';
SELECT /*+ indexscan(test_prefix_key_len idx_prefix_len_test_ftext) */count(1) FROM test_prefix_key_len WHERE ftext LIKE '一二三四五六七八九十百千万亿兆%';
EXPLAIN (costs false)
SELECT /*+ indexscan(test_prefix_key_len idx_prefix_len_test_fbytea) */count(1) FROM test_prefix_key_len WHERE fbytea LIKE '一二三四五六七八九十百千万亿兆%';
SELECT /*+ indexscan(test_prefix_key_len idx_prefix_len_test_fbytea) */count(1) FROM test_prefix_key_len WHERE fbytea LIKE '一二三四五六七八九十百千万亿兆%';
EXPLAIN (costs false)
SELECT /*+ indexscan(test_prefix_key_len idx_prefix_len_test_comb) */count(1) FROM test_prefix_key_len WHERE ftext LIKE '一二三四五六七八九十百千万亿兆%';
SELECT /*+ indexscan(test_prefix_key_len idx_prefix_len_test_comb) */count(1) FROM test_prefix_key_len WHERE ftext LIKE '一二三四五六七八九十百千万亿兆%';
DROP TABLE test_prefix_key_len;

-- test ubtree prefix length
CREATE TABLE test_prefix_key_len (
    fchar CHAR(4096),
    ftext TEXT,
    fbytea BYTEA
) WITH (STORAGE_TYPE=USTORE);
insert into test_prefix_key_len
  select array_to_string(array(
    select '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmn'
    FROM generate_series(1, 80)), '') as col1, col1 as col2, convert_to(col2, 'UTF8') as col3;
select length(fchar),length(ftext),length(fbytea) from test_prefix_key_len;
select lengthb(fchar),lengthb(ftext),octet_length(fbytea) from test_prefix_key_len;
CREATE INDEX idx_prefix_len_test_fchar ON test_prefix_key_len (fchar(2677));
CREATE INDEX idx_prefix_len_test_ftext ON test_prefix_key_len (ftext(2677));
CREATE INDEX idx_prefix_len_test_fbytea ON test_prefix_key_len (fbytea(2677));
CREATE INDEX idx_prefix_len_test_fchar ON test_prefix_key_len (fchar(2676));
CREATE INDEX idx_prefix_len_test_ftext ON test_prefix_key_len (ftext(2676));
CREATE INDEX idx_prefix_len_test_fbytea ON test_prefix_key_len (fbytea(2676));
CREATE INDEX idx_prefix_len_test_comb ON test_prefix_key_len (fchar(2676),ftext(2676),fbytea(2676));
EXPLAIN (costs false)
SELECT /*+ indexscan(test_prefix_key_len idx_prefix_len_test_fchar) */count(1) FROM test_prefix_key_len WHERE fchar LIKE '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmn%';
SELECT /*+ indexscan(test_prefix_key_len idx_prefix_len_test_fchar) */count(1) FROM test_prefix_key_len WHERE fchar LIKE '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmn%';
EXPLAIN (costs false)
SELECT /*+ indexscan(test_prefix_key_len idx_prefix_len_test_ftext) */count(1) FROM test_prefix_key_len WHERE ftext LIKE '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmn%';
SELECT /*+ indexscan(test_prefix_key_len idx_prefix_len_test_ftext) */count(1) FROM test_prefix_key_len WHERE ftext LIKE '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmn%';
EXPLAIN (costs false)
SELECT /*+ indexscan(test_prefix_key_len idx_prefix_len_test_fbytea) */count(1) FROM test_prefix_key_len WHERE fbytea LIKE '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmn%';
SELECT /*+ indexscan(test_prefix_key_len idx_prefix_len_test_fbytea) */count(1) FROM test_prefix_key_len WHERE fbytea LIKE '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmn%';
EXPLAIN (costs false)
SELECT /*+ indexscan(test_prefix_key_len idx_prefix_len_test_comb) */count(1) FROM test_prefix_key_len WHERE ftext LIKE '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmn%';
SELECT /*+ indexscan(test_prefix_key_len idx_prefix_len_test_comb) */count(1) FROM test_prefix_key_len WHERE ftext LIKE '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmn%';
DROP TABLE test_prefix_key_len;

--
-- CREATE_INDEX and USE INDEX
--
CREATE TABLE test_prefix_table (
    id INTEGER,
    fchar CHAR(64) collate utf8mb4_bin,
    fvchar VARCHAR(64) collate utf8mb4_bin,
    ftext TEXT collate utf8mb4_bin,
    fclob CLOB collate utf8mb4_bin,
    fblob BLOB,
    fraw RAW,
    fbytea BYTEA
) ;
INSERT INTO test_prefix_table VALUES(0, NULL, NULL, NULL, NULL , NULL, NULL, NULL);
INSERT INTO test_prefix_table VALUES(30, 'ZZZZZ-123', 'ZZZZZ-123', 'ZZZZZ-123', 'ZZZZZ-123', '5A5A5A5A5A2D313233', HEXTORAW('5A5A5A5A5A2D313233'),E'\\x5A5A5A5A5A2D313233');
INSERT INTO test_prefix_table VALUES(30, 'ZZZZZ-300', 'ZZZZZ-300', 'ZZZZZ-300', 'ZZZZZ-300', '5A5A5A5A5A2D333030', HEXTORAW('5A5A5A5A5A2D333030'),E'\\x5A5A5A5A5A2D333030');
INSERT INTO test_prefix_table VALUES(30, 'ZZZZZ-000', 'ZZZZZ-000', 'ZZZZZ-000', 'ZZZZZ-000', '5A5A5A5A5A2D303030', HEXTORAW('5A5A5A5A5A2D303030'),E'\\x5A5A5A5A5A2D303030');
INSERT INTO test_prefix_table VALUES(30, 'ZZZZZ-211', 'ZZZZZ-211', 'ZZZZZ-211', 'ZZZZZ-211', '5A5A5A5A5A2D323131', HEXTORAW('5A5A5A5A5A2D323131'),E'\\x5A5A5A5A5A2D323131');
INSERT INTO test_prefix_table VALUES(30, 'ZZZZZ-111', 'ZZZZZ-111', 'ZZZZZ-111', 'ZZZZZ-111', '5A5A5A5A5A2D313131', HEXTORAW('5A5A5A5A5A2D313131'),E'\\x5A5A5A5A5A2D313131');
INSERT INTO test_prefix_table VALUES(30, 'ZZZZZ-210', 'ZZZZZ-210', 'ZZZZZ-210', 'ZZZZZ-210', '5A5A5A5A5A2D323130', HEXTORAW('5A5A5A5A5A2D323130'),E'\\x5A5A5A5A5A2D323130');
INSERT INTO test_prefix_table VALUES(30, 'Z', 'Z', 'Z', 'Z', '5A', HEXTORAW('5A'),E'\\x5A');
INSERT INTO test_prefix_table VALUES(0, NULL, NULL, NULL, NULL , NULL, NULL, NULL);
INSERT INTO test_prefix_table VALUES(20, 'YYYYY-300', 'YYYYY-300', 'YYYYY-300', 'YYYYY-300', '59595959592D333030', HEXTORAW('59595959592D333030'),E'\\x59595959592D333030');
INSERT INTO test_prefix_table VALUES(20, 'YYYYY-000', 'YYYYY-000', 'YYYYY-000', 'YYYYY-000', '59595959592D303030', HEXTORAW('59595959592D303030'),E'\\x59595959592D303030');
INSERT INTO test_prefix_table VALUES(20, 'YYYYY-211', 'YYYYY-211', 'YYYYY-211', 'YYYYY-211', '59595959592D323131', HEXTORAW('59595959592D323131'),E'\\x59595959592D323131');
INSERT INTO test_prefix_table VALUES(20, 'YYYYY-111', 'YYYYY-111', 'YYYYY-111', 'YYYYY-111', '59595959592D313131', HEXTORAW('59595959592D313131'),E'\\x59595959592D313131');
INSERT INTO test_prefix_table VALUES(20, 'YYYYY-210', 'YYYYY-210', 'YYYYY-210', 'YYYYY-210', '59595959592D323130', HEXTORAW('59595959592D323130'),E'\\x59595959592D323130');
INSERT INTO test_prefix_table VALUES(20, 'Y', 'Y', 'Y', 'Y', '59', HEXTORAW('59'),E'\\x59');
INSERT INTO test_prefix_table VALUES(20, '高斯数据库-210', '高斯数据库-210', '高斯数据库-210', '高斯数据库-210', 'e9ab98e696afe695b0e68daee5ba932d323130', HEXTORAW('e9ab98e696afe695b0e68daee5ba932d323130'),E'\\xe9ab98e696afe695b0e68daee5ba932d323130');
INSERT INTO test_prefix_table VALUES(20, '开源数据库-210', '开源数据库-210', '开源数据库-210', '开源数据库-210', 'e5bc80e6ba90e695b0e68daee5ba932d323130', HEXTORAW('e5bc80e6ba90e695b0e68daee5ba932d323130'),E'\\xe5bc80e6ba90e695b0e68daee5ba932d323130');
INSERT INTO test_prefix_table VALUES(20, '高', '高', '高', '高', 'e9ab98', HEXTORAW('e9ab98'),E'\\xe9ab98');

CREATE INDEX prefix_index_fchar_fbytea ON test_prefix_table (fchar(5), fbytea(5));
CREATE INDEX prefix_index_fvchar ON test_prefix_table (fvchar(5));
CREATE INDEX prefix_index_ftext ON test_prefix_table (ftext(5));
CREATE INDEX prefix_index_fclob ON test_prefix_table (fclob(5));
CREATE INDEX prefix_index_fblob ON test_prefix_table (fblob(5));
CREATE UNIQUE INDEX prefix_index_fraw ON test_prefix_table (fraw(9));
\d+ test_prefix_table
select pg_get_tabledef('test_prefix_table'::regclass);

set enable_seqscan=false;
set enable_opfusion=false;
set enable_partition_opfusion=false;


SELECT ftext FROM test_prefix_table where ftext like 'XXXXX%' ORDER BY 1;
SELECT fblob FROM test_prefix_table where fblob < '58585858582D333030' ORDER BY 1;
SELECT fchar, fbytea FROM test_prefix_table where (fchar, fbytea)= ('XXXXX-211', 'XXXXX-211') ORDER BY 1,2;
--insert
INSERT INTO test_prefix_table VALUES(0, NULL, NULL, NULL, NULL , NULL, NULL, NULL);
INSERT INTO test_prefix_table VALUES(10, 'XXXXX-300', 'XXXXX-300', 'XXXXX-300', 'XXXXX-300', '58585858582D333030', HEXTORAW('58585858582D333030'),E'\\x58585858582D333030');
INSERT INTO test_prefix_table VALUES(10, 'XXXXX-000', 'XXXXX-000', 'XXXXX-000', 'XXXXX-000', '58585858582D303030', HEXTORAW('58585858582D303030'),E'\\x58585858582D303030');
INSERT INTO test_prefix_table VALUES(10, 'XXXXX-211', 'XXXXX-211', 'XXXXX-211', 'XXXXX-211', '58585858582D323131', HEXTORAW('58585858582D323131'),E'\\x58585858582D323131');
INSERT INTO test_prefix_table VALUES(10, 'XXXXX-111', 'XXXXX-111', 'XXXXX-111', 'XXXXX-111', '58585858582D313131', HEXTORAW('58585858582D313131'),E'\\x58585858582D313131');
INSERT INTO test_prefix_table VALUES(10, 'XXXXX-210', 'XXXXX-210', 'XXXXX-210', 'XXXXX-210', '58585858582D323130', HEXTORAW('58585858582D323130'),E'\\x58585858582D323130');

SELECT ftext FROM test_prefix_table where ftext like 'XXXXX%' ORDER BY 1;
SELECT fblob FROM test_prefix_table where fblob < '58585858582D333030' ORDER BY 1;
SELECT fchar, fbytea FROM test_prefix_table where (fchar, fbytea)= ('XXXXX-211', 'XXXXX-211') ORDER BY 1,2;

--update
UPDATE test_prefix_table SET fchar=replace(fchar, 'XXXXX', 'AAAAA'), ftext=replace(ftext, 'XXXXX', 'AAAAA') where fvchar like 'XXXXX%';

SELECT ftext FROM test_prefix_table where ftext like 'AAAAA%' ORDER BY 1;
SELECT fblob FROM test_prefix_table where fblob < '58585858582D333030' ORDER BY 1;
SELECT fchar, fbytea FROM test_prefix_table where (fchar, fbytea)= ('AAAAA-211', 'XXXXX-211') ORDER BY 1,2;

--delete
DELETE FROM test_prefix_table where (fchar, fbytea)= ('AAAAA-211', 'XXXXX-211');

SELECT ftext FROM test_prefix_table where ftext like 'AAAAA%' ORDER BY 1;
SELECT fblob FROM test_prefix_table where fblob < '58585858582D333030' ORDER BY 1;
SELECT fchar, fbytea FROM test_prefix_table where (fchar, fbytea)= ('AAAAA-211', 'XXXXX-211') ORDER BY 1,2;

--check query plan
analyze test_prefix_table;
--single table index scan
EXPLAIN (costs false)
SELECT ftext FROM test_prefix_table where ftext like 'YYYY%' ORDER BY 1;
SELECT ftext FROM test_prefix_table where ftext like 'YYYY%' ORDER BY 1;
EXPLAIN (costs false)
SELECT ftext FROM test_prefix_table where ftext like 'YYYYY%' ORDER BY 1;
SELECT ftext FROM test_prefix_table where ftext like 'YYYYY%' ORDER BY 1;
EXPLAIN (costs false)
SELECT ftext FROM test_prefix_table where ftext like 'YYYYY-1%' ORDER BY 1;
SELECT ftext FROM test_prefix_table where ftext like 'YYYYY-1%' ORDER BY 1;
EXPLAIN (costs false)
SELECT ftext FROM test_prefix_table where ftext like 'YYYYY-210' ORDER BY 1;
SELECT ftext FROM test_prefix_table where ftext like 'YYYYY-210' ORDER BY 1;
EXPLAIN (costs false)
SELECT ftext FROM test_prefix_table where ftext like '开%' ORDER BY 1;
SELECT ftext FROM test_prefix_table where ftext like '开%' ORDER BY 1;
EXPLAIN (costs false)
SELECT ftext FROM test_prefix_table where ftext like '开源数据库%' ORDER BY 1;
SELECT ftext FROM test_prefix_table where ftext like '开源数据库%' ORDER BY 1;
EXPLAIN (costs false)
SELECT ftext FROM test_prefix_table where ftext like '开源数据库-2%' ORDER BY 1;
SELECT ftext FROM test_prefix_table where ftext like '开源数据库-2%' ORDER BY 1;
EXPLAIN (costs false)
SELECT ftext FROM test_prefix_table where ftext = '开源数据库-210' ORDER BY 1;
SELECT ftext FROM test_prefix_table where ftext = '开源数据库-210' ORDER BY 1;
EXPLAIN (costs false)
SELECT ftext FROM test_prefix_table where ftext > '开' ORDER BY 1;
SELECT ftext FROM test_prefix_table where ftext > '开' ORDER BY 1;
EXPLAIN (costs false)
SELECT ftext FROM test_prefix_table where ftext > '开源数据库' ORDER BY 1;
SELECT ftext FROM test_prefix_table where ftext > '开源数据库' ORDER BY 1;
EXPLAIN (costs false)
SELECT ftext FROM test_prefix_table where ftext > '开源数据库-210' ORDER BY 1;
SELECT ftext FROM test_prefix_table where ftext > '开源数据库-210' ORDER BY 1;
EXPLAIN (costs false)
SELECT ftext FROM test_prefix_table where ftext < '开' ORDER BY 1;
SELECT ftext FROM test_prefix_table where ftext < '开' ORDER BY 1;
EXPLAIN (costs false)
SELECT ftext FROM test_prefix_table where ftext < '开源数据库' ORDER BY 1;
SELECT ftext FROM test_prefix_table where ftext < '开源数据库' ORDER BY 1;
EXPLAIN (costs false)
SELECT ftext FROM test_prefix_table where ftext < '开源数据库-210' ORDER BY 1;
SELECT ftext FROM test_prefix_table where ftext < '开源数据库-210' ORDER BY 1;
EXPLAIN (costs false)
SELECT fbytea FROM test_prefix_table t where fbytea ~~ 'YY%' ORDER BY 1;
SELECT fbytea FROM test_prefix_table t where fbytea ~~ 'YY%' ORDER BY 1;
EXPLAIN (costs false)
SELECT fbytea FROM test_prefix_table t where fbytea like 'YYYYY%' ORDER BY 1;
SELECT fbytea FROM test_prefix_table t where fbytea like 'YYYYY%' ORDER BY 1;
EXPLAIN (costs false)
SELECT fbytea FROM test_prefix_table t where fbytea ~~ 'YYYYY-%' ORDER BY 1;
SELECT fbytea FROM test_prefix_table t where fbytea ~~ 'YYYYY-%' ORDER BY 1;
EXPLAIN (costs false)
SELECT fbytea FROM test_prefix_table t where fbytea like 'YYYYY-111' ORDER BY 1;
SELECT fbytea FROM test_prefix_table t where fbytea like 'YYYYY-111' ORDER BY 1;
EXPLAIN (costs false)
SELECT fbytea FROM test_prefix_table t where fbytea like '高%' ORDER BY 1;
SELECT fbytea FROM test_prefix_table t where fbytea like '高%' ORDER BY 1;
EXPLAIN (costs false)
SELECT fbytea FROM test_prefix_table t where fbytea ~~ '开源数据库%' ORDER BY 1;
SELECT fbytea FROM test_prefix_table t where fbytea ~~ '开源数据库%' ORDER BY 1;
EXPLAIN (costs false)
SELECT fbytea FROM test_prefix_table t where fbytea like '高斯数据库-%' ORDER BY 1;
SELECT fbytea FROM test_prefix_table t where fbytea like '高斯数据库-%' ORDER BY 1;
EXPLAIN (costs false)
SELECT fbytea FROM test_prefix_table t where fbytea ~~ '开源数据库-210' ORDER BY 1;
SELECT fbytea FROM test_prefix_table t where fbytea ~~ '开源数据库-210' ORDER BY 1;
EXPLAIN (costs false)
SELECT fbytea FROM test_prefix_table t where fbytea = E'\\x59595959592D323131' ORDER BY 1;
SELECT fbytea FROM test_prefix_table t where fbytea = E'\\x59595959592D323131' ORDER BY 1;
EXPLAIN (costs false)
SELECT fbytea FROM test_prefix_table t where fbytea = E'\\x59' ORDER BY 1;
SELECT fbytea FROM test_prefix_table t where fbytea = E'\\x59' ORDER BY 1;
EXPLAIN (costs false)
SELECT fbytea FROM test_prefix_table t where fbytea > 'Y' ORDER BY 1;
SELECT fbytea FROM test_prefix_table t where fbytea > 'Y' ORDER BY 1;
EXPLAIN (costs false)
SELECT fbytea FROM test_prefix_table t where fbytea > 'YYYYY' ORDER BY 1;
SELECT fbytea FROM test_prefix_table t where fbytea > 'YYYYY' ORDER BY 1;
EXPLAIN (costs false)
SELECT fbytea FROM test_prefix_table t where fbytea > 'YYYYY-210' ORDER BY 1;
SELECT fbytea FROM test_prefix_table t where fbytea > 'YYYYY-210' ORDER BY 1;
EXPLAIN (costs false)
SELECT fbytea FROM test_prefix_table t where fbytea < 'Y' ORDER BY 1;
SELECT fbytea FROM test_prefix_table t where fbytea < 'Y' ORDER BY 1;
EXPLAIN (costs false)
SELECT fbytea FROM test_prefix_table t where fbytea < 'YYYYY' ORDER BY 1;
SELECT fbytea FROM test_prefix_table t where fbytea < 'YYYYY' ORDER BY 1;
EXPLAIN (costs false)
SELECT fbytea FROM test_prefix_table t where fbytea < 'YYYYY-210' ORDER BY 1;
SELECT fbytea FROM test_prefix_table t where fbytea < 'YYYYY-210' ORDER BY 1;
EXPLAIN (costs false)
SELECT fchar FROM test_prefix_table where fchar ~ '^开' ORDER BY 1;
SELECT fchar FROM test_prefix_table where fchar ~ '^开' ORDER BY 1;
EXPLAIN (costs false)
SELECT fchar FROM test_prefix_table where fchar <= 'YYYYY-100' ORDER BY 1;
SELECT fchar FROM test_prefix_table where fchar <= 'YYYYY-100' ORDER BY 1;
EXPLAIN (costs false)
SELECT fchar FROM test_prefix_table where fchar <= 'YYYYY' ORDER BY 1;
SELECT fchar FROM test_prefix_table where fchar <= 'YYYYY' ORDER BY 1;
EXPLAIN (costs false)
SELECT fclob FROM test_prefix_table t where fclob < 'ZZZZZ-210' ORDER BY 1;
SELECT fclob FROM test_prefix_table t where fclob < 'ZZZZZ-210' ORDER BY 1;
EXPLAIN (costs false)
SELECT fclob FROM test_prefix_table t where fclob < 'Z' ORDER BY 1;
SELECT fclob FROM test_prefix_table t where fclob < 'Z' ORDER BY 1;
EXPLAIN (costs false)
SELECT ftext FROM test_prefix_table t where fvchar >= 'ZZZZZ-210' ORDER BY 1;
SELECT ftext FROM test_prefix_table t where fvchar >= 'ZZZZZ-210' ORDER BY 1;
EXPLAIN (costs false)
SELECT ftext FROM test_prefix_table t where fvchar >= 'ZZZZZ' ORDER BY 1;
SELECT ftext FROM test_prefix_table t where fvchar >= 'ZZZZZ' ORDER BY 1;
EXPLAIN (costs false)
SELECT ftext FROM test_prefix_table t where fvchar > 'ZZZZZ-' and fvchar <> 'ZZZZZ-210' ORDER BY 1;
SELECT ftext FROM test_prefix_table t where fvchar > 'ZZZZZ-' and fvchar <> 'ZZZZZ-210' ORDER BY 1;
EXPLAIN (costs false)
SELECT ftext FROM test_prefix_table t where fvchar > left('ZZZZZ-211', 7) and fvchar <> 'ZZZZZ-211' ORDER BY 1;
SELECT ftext FROM test_prefix_table t where fvchar > left('ZZZZZ-211', 7) and fvchar <> 'ZZZZZ-211' ORDER BY 1;
EXPLAIN (costs false)
SELECT fblob FROM test_prefix_table t where fblob IS NOT NULL ORDER BY 1;
SELECT fblob FROM test_prefix_table t where fblob IS NOT NULL ORDER BY 1;
EXPLAIN (costs false)
SELECT DISTINCT fraw FROM test_prefix_table t where fraw IS NOT NULL ORDER BY 1;
SELECT DISTINCT fraw FROM test_prefix_table t where fraw IS NOT NULL ORDER BY 1;
EXPLAIN (costs false)
SELECT fraw FROM test_prefix_table t where fraw IS NULL or ftext like '高%' ORDER BY 1;
SELECT fraw FROM test_prefix_table t where fraw IS NULL or ftext like '高%' ORDER BY 1;
EXPLAIN (costs false)
SELECT fvchar FROM test_prefix_table t where fraw IS NULL or fvchar > 'ZZZZZ-200' and fvchar <> 'ZZZZZ-210' ORDER BY 1;
SELECT fvchar FROM test_prefix_table t where fraw IS NULL or fvchar > 'ZZZZZ-200' and fvchar <> 'ZZZZZ-210' ORDER BY 1;
EXPLAIN (costs false)
SELECT fvchar FROM test_prefix_table t where NOT fvchar <> 'ZZZZZ-211' ORDER BY 1;
SELECT fvchar FROM test_prefix_table t where NOT fvchar <>'ZZZZZ-211' ORDER BY 1;
EXPLAIN (costs false)
SELECT fvchar FROM test_prefix_table t where (fvchar <> 'ZZZZZ-211') = false ORDER BY 1;
SELECT fvchar FROM test_prefix_table t where (fvchar <> 'ZZZZZ-211') = false ORDER BY 1;
PREPARE testprefixindex(text) as SELECT fvchar FROM test_prefix_table t where fraw IS NULL or fvchar > $1 and fvchar <> 'ZZZZZ-210' ORDER BY 1;
EXPLAIN (costs false) execute testprefixindex('ZZZZZ-200');
EXECUTE testprefixindex('ZZZZZ-200');
--prefix index not used
EXPLAIN (costs false)
SELECT fvchar FROM test_prefix_table t where (fvchar <> 'ZZZZZ-211') IS false ORDER BY 1;
SELECT fvchar FROM test_prefix_table t where (fvchar <> 'ZZZZZ-211') IS false ORDER BY 1;
EXPLAIN (costs false)
SELECT fvchar FROM test_prefix_table t where fvchar <> 'ZZZZZ-210';
EXPLAIN (costs false)
SELECT left(ftext, 5) FROM test_prefix_table where left(ftext, 5) = 'YYYYY';
EXPLAIN (costs false)
SELECT ftext FROM test_prefix_table ORDER BY ftext;
EXPLAIN (costs false)
select * from test_prefix_table tab1 where (fchar, fbytea)<('YYYYY-210', E'\\x59595959592D323130');
EXPLAIN (costs false)
SELECT ftext FROM test_prefix_table where ftext not like 'YYYY%' ORDER BY 1;
EXPLAIN (costs false)
SELECT fvchar FROM test_prefix_table where fvchar = 'YYYYY-100'::name;
-- single table bitmapscan
EXPLAIN (costs false)
SELECT /*+ set(enable_bitmapscan on)  set(enable_indexscan off) */ ftext FROM test_prefix_table where ftext like 'YYYYY-2%' ORDER BY 1;
SELECT /*+ set(enable_bitmapscan on)  set(enable_indexscan off) */ ftext FROM test_prefix_table where ftext like 'YYYYY-2%' ORDER BY 1;
EXPLAIN (costs false)
SELECT /*+ set(enable_bitmapscan on)  set(enable_indexscan off) */ ftext FROM test_prefix_table where ftext like 'YYYYY%' ORDER BY 1;
SELECT /*+ set(enable_bitmapscan on)  set(enable_indexscan off) */ ftext FROM test_prefix_table where ftext like 'YYYYY%' ORDER BY 1;
EXPLAIN (costs false)
SELECT /*+ set(enable_bitmapscan on)  set(enable_indexscan off) */ ftext FROM test_prefix_table where ftext like 'YYYY%' ORDER BY 1;
SELECT /*+ set(enable_bitmapscan on)  set(enable_indexscan off) */ ftext FROM test_prefix_table where ftext like 'YYYY%' ORDER BY 1;

EXPLAIN (costs false)
SELECT /*+ set(enable_bitmapscan on)  set(enable_indexscan off) */ ftext FROM test_prefix_table where ftext > 'YYYYY-100' ORDER BY 1;
SELECT /*+ set(enable_bitmapscan on)  set(enable_indexscan off) */ ftext FROM test_prefix_table where ftext > 'YYYYY-100' ORDER BY 1;
EXPLAIN (costs false)
SELECT /*+ set(enable_bitmapscan on)  set(enable_indexscan off) */ ftext FROM test_prefix_table where ftext > 'YYYYY' ORDER BY 1;
SELECT /*+ set(enable_bitmapscan on)  set(enable_indexscan off) */ ftext FROM test_prefix_table where ftext > 'YYYYY' ORDER BY 1;
EXPLAIN (costs false)
SELECT /*+ set(enable_bitmapscan on)  set(enable_indexscan off) */ ftext FROM test_prefix_table where ftext > 'YYYY' ORDER BY 1;
SELECT /*+ set(enable_bitmapscan on)  set(enable_indexscan off) */ ftext FROM test_prefix_table where ftext > 'YYYY' ORDER BY 1;

EXPLAIN (costs false)
SELECT /*+ set(enable_bitmapscan on)  set(enable_indexscan off) */ ftext FROM test_prefix_table where ftext < 'YYYYY-100' ORDER BY 1;
SELECT /*+ set(enable_bitmapscan on)  set(enable_indexscan off) */ ftext FROM test_prefix_table where ftext < 'YYYYY-100' ORDER BY 1;
EXPLAIN (costs false)
SELECT /*+ set(enable_bitmapscan on)  set(enable_indexscan off) */ ftext FROM test_prefix_table where ftext < 'YYYYY' ORDER BY 1;
SELECT /*+ set(enable_bitmapscan on)  set(enable_indexscan off) */ ftext FROM test_prefix_table where ftext < 'YYYYY' ORDER BY 1;
EXPLAIN (costs false)
SELECT /*+ set(enable_bitmapscan on)  set(enable_indexscan off) */ ftext FROM test_prefix_table where ftext < 'YYYY' ORDER BY 1;
SELECT /*+ set(enable_bitmapscan on)  set(enable_indexscan off) */ ftext FROM test_prefix_table where ftext < 'YYYY' ORDER BY 1;

EXPLAIN (costs false)
SELECT /*+ set(enable_bitmapscan on)  set(enable_indexscan off) */ ftext FROM test_prefix_table where ftext >= 'YYYYY-1' and ftext <= 'YYYYY-3' ORDER BY 1;
SELECT /*+ set(enable_bitmapscan on)  set(enable_indexscan off) */ ftext FROM test_prefix_table where ftext >= 'YYYYY-1' and ftext <= 'YYYYY-3' ORDER BY 1;
EXPLAIN (costs false)
SELECT /*+ set(enable_bitmapscan on)  set(enable_indexscan off) */ ftext FROM test_prefix_table where fchar = 'YYYYY-300' and fbytea = E'\\x59595959592D333030' ORDER BY 1;
SELECT /*+ set(enable_bitmapscan on)  set(enable_indexscan off) */ ftext FROM test_prefix_table where fchar = 'YYYYY-300' and fbytea = E'\\x59595959592D333030' ORDER BY 1;
EXPLAIN (costs false)
SELECT /*+ set(enable_bitmapscan on)  set(enable_indexscan off) */ ftext FROM test_prefix_table where fchar = 'ZZZZZ-300' or fbytea = E'\\x59595959592D333030' ORDER BY 1;
SELECT /*+ set(enable_bitmapscan on)  set(enable_indexscan off) */ ftext FROM test_prefix_table where fchar = 'ZZZZZ-300' or fbytea = E'\\x59595959592D333030' ORDER BY 1;
EXPLAIN (costs false)
SELECT /*+ set(enable_bitmapscan on)  set(enable_indexscan off) */ ftext FROM test_prefix_table where fchar = 'YYYYY-300' and ftext = 'YYYYY-300' ORDER BY 1;
SELECT /*+ set(enable_bitmapscan on)  set(enable_indexscan off) */ ftext FROM test_prefix_table where fchar = 'YYYYY-300' and ftext = 'YYYYY-300' ORDER BY 1;
EXPLAIN (costs false)
SELECT /*+ set(enable_bitmapscan on)  set(enable_indexscan off) */ ftext FROM test_prefix_table where fchar = 'YYYYY-300' or ftext = 'ZZZZZ-300' ORDER BY 1;
SELECT /*+ set(enable_bitmapscan on)  set(enable_indexscan off) */ ftext FROM test_prefix_table where fchar = 'YYYYY-300' or ftext = 'ZZZZZ-300' ORDER BY 1;
EXPLAIN (costs false)
SELECT /*+ set(enable_bitmapscan on)  set(enable_indexscan off) */ ftext FROM test_prefix_table where (fchar = 'YYYYY-300' or ftext = 'ZZZZZ-300') AND fvchar = 'YYYYY-300' ORDER BY 1;
SELECT /*+ set(enable_bitmapscan on)  set(enable_indexscan off) */ ftext FROM test_prefix_table where (fchar = 'YYYYY-300' or ftext = 'ZZZZZ-300') AND fvchar = 'YYYYY-300' ORDER BY 1;
EXPLAIN (costs false)
SELECT /*+ set(enable_bitmapscan on)  set(enable_indexscan off) */ ftext FROM test_prefix_table where ftext = 'ZZZZZ-300' or (fchar = 'YYYYY-300' AND fvchar = 'YYYYY-300') ORDER BY 1;
SELECT /*+ set(enable_bitmapscan on)  set(enable_indexscan off) */ ftext FROM test_prefix_table where ftext = 'ZZZZZ-300' or (fchar = 'YYYYY-300' AND fvchar = 'YYYYY-300') ORDER BY 1;
EXPLAIN (costs false)
SELECT /*+ set(enable_bitmapscan on)  set(enable_indexscan off) */ ftext FROM test_prefix_table where
    fchar = 'AAAAA-300' OR
    fvchar = 'YYYYY-300' OR
    ftext = 'ZZZZZ-300' OR
    fclob = '高斯数据库-210' OR
    fblob = 'e5bc80e6ba90e695b0e68daee5ba932d323130'
ORDER BY 1;
SELECT /*+ set(enable_bitmapscan on)  set(enable_indexscan off) */ ftext FROM test_prefix_table where
    fchar = 'AAAAA-300' OR
    fvchar = 'YYYYY-300' OR
    ftext = 'ZZZZZ-300' OR
    fclob = '高斯数据库-210' OR
    fblob = 'e5bc80e6ba90e695b0e68daee5ba932d323130'
ORDER BY 1;
EXPLAIN (costs false)
SELECT /*+ set(enable_bitmapscan on)  set(enable_indexscan off) */ ftext FROM test_prefix_table where
    fchar like 'AAAAA%' OR
    fvchar like 'YYYYY%' OR
    ftext like 'ZZZZZ%' OR
    fclob like '高斯数据库%'
ORDER BY 1;
SELECT /*+ set(enable_bitmapscan on)  set(enable_indexscan off) */ ftext FROM test_prefix_table where
    fchar like 'AAAAA%' OR
    fvchar like 'YYYYY%' OR
    ftext like 'ZZZZZ%' OR
    fclob like '高斯数据库%'
ORDER BY 1;

set enable_bitmapscan=false;
set enable_material=false;
set enable_hashjoin=false;
set enable_mergejoin=false;
--join index scan
EXPLAIN (costs false)
SELECT t1.ftext,t2.fchar FROM test_prefix_table t1 join test_prefix_table t2 on  t1.ftext = t2.fchar ORDER BY 1,2;
SELECT t1.ftext,t2.fchar FROM test_prefix_table t1 join test_prefix_table t2 on  t1.ftext = t2.fchar ORDER BY 1,2;
EXPLAIN (costs false)
SELECT count(1) FROM test_prefix_table t1 join test_prefix_table t2 on (t2.fvchar LIKE 'X%' AND t1.ftext > t2.fvchar);
SELECT count(1) FROM test_prefix_table t1 join test_prefix_table t2 on (t2.fvchar LIKE 'X%' AND t1.ftext > t2.fvchar);
EXPLAIN (costs false)
SELECT t1.ftext FROM test_prefix_table t1 join test_prefix_table t2 on  t1.ftext = t2.fvchar where t1.id=30 and t2.id > 10 ORDER BY 1;
SELECT t1.ftext FROM test_prefix_table t1 join test_prefix_table t2 on  t1.ftext = t2.fvchar where t1.id=30 and t2.id > 10 ORDER BY 1;
EXPLAIN (costs false)
SELECT t1.ftext,t2.fvchar FROM test_prefix_table t1, test_prefix_table t2 where t1.ftext = t2.fvchar and t1.id=30 ORDER BY 1,2;
SELECT t1.ftext,t2.fvchar FROM test_prefix_table t1, test_prefix_table t2 where t1.ftext = t2.fvchar and t1.id=30 ORDER BY 1,2;
EXPLAIN (costs false)
SELECT t1.fvchar,t2.fvchar FROM test_prefix_table t1 left join test_prefix_table t2 on (t1.fvchar = t2.fvchar and t2.fvchar > 'ZZZZZ-3' ) ORDER BY 1,2;
SELECT t1.fvchar,t2.fvchar FROM test_prefix_table t1 left join test_prefix_table t2 on (t1.fvchar = t2.fvchar and t2.fvchar > 'ZZZZZ-3' ) ORDER BY 1,2;
--prefix index not used
EXPLAIN (costs false)
SELECT t1.ftext,t2.fvchar FROM test_prefix_table t1 join test_prefix_table t2 on  t1.ftext like t2.fvchar;
EXPLAIN (costs false)
SELECT t1.ftext,t2.fvchar FROM test_prefix_table t1 join test_prefix_table t2 on  t1.ftext not like t2.fvchar;
EXPLAIN (costs false)
SELECT t1.ftext,t2.fvchar FROM test_prefix_table t1 join test_prefix_table t2 on  t1.ftext <> t2.fvchar;

--alter table
ALTER TABLE test_prefix_table MODIFY ftext varchar(64);
ALTER TABLE test_prefix_table ALTER COLUMN ftext TYPE text;
EXPLAIN (costs false)
DELETE FROM test_prefix_table WHERE ftext IS NULL;
DELETE FROM test_prefix_table WHERE ftext IS NULL;
ALTER TABLE test_prefix_table ALTER COLUMN ftext SET NOT NULL;
ALTER TABLE test_prefix_table ALTER COLUMN ftext DROP NOT NULL;
EXPLAIN (costs false)
SELECT ftext FROM test_prefix_table where ftext = '高斯数据库-210' ORDER BY 1;
SELECT ftext FROM test_prefix_table where ftext = '高斯数据库-210' ORDER BY 1;

CREATE SCHEMA prefix_index_schema;
ALTER TABLE test_prefix_table SET SCHEMA prefix_index_schema;
set current_schema = prefix_index_schema;
set enable_seqscan=false;
set enable_opfusion=false;
set enable_partition_opfusion=false;
EXPLAIN (costs false)
SELECT ftext FROM test_prefix_table where ftext >= '高斯数据库-210' ORDER BY 1;
SELECT ftext FROM test_prefix_table where ftext >= '高斯数据库-210' ORDER BY 1;

ALTER TABLE test_prefix_table RENAME TO test_prefix_tb;
ALTER TABLE test_prefix_tb RENAME COLUMN fchar TO fbpchar;
ALTER TABLE test_prefix_tb DROP ftext;
EXPLAIN (costs false)
SELECT fbpchar FROM test_prefix_tb where fbpchar > '开源' ORDER BY 1;
SELECT fbpchar FROM test_prefix_tb where fbpchar > '开源' ORDER BY 1;
ALTER INDEX prefix_index_fchar_fbytea UNUSABLE;
EXPLAIN (costs false)
SELECT fbpchar FROM test_prefix_tb where fbpchar like '高斯数据库-%' ORDER BY 1;

RESET enable_seqscan;
RESET enable_opfusion;
RESET enable_partition_opfusion;
DROP TABLE IF EXISTS test_prefix_tb;
RESET current_schema;
DROP SCHEMA prefix_index_schema CASCADE;

RESET enable_bitmapscan;
RESET enable_material;
RESET enable_hashjoin;
RESET enable_mergejoin;
RESET enable_seqscan;
RESET enable_opfusion;
RESET enable_partition_opfusion;

--partition table
CREATE TABLE test_prefix_parttable (
    id INTEGER,
    fchar CHAR(64),
    fvchar VARCHAR(64),
    ftext TEXT,
    fclob CLOB,
    fblob BLOB,
    fraw RAW,
    fbytea BYTEA
) PARTITION BY RANGE (ftext) SUBPARTITION BY LIST (fvchar)
(
  PARTITION p_xxxxx VALUES LESS THAN( 'YYYYY' )
  (
    SUBPARTITION p_xxxxx_1 values ('XXXXX-111'),
    SUBPARTITION p_xxxxx_2 values ('XXXXX-211'),
    SUBPARTITION p_xxxxx_3 values ('XXXXX-300'),
    SUBPARTITION p_xxxxx_4 values ('高斯数据库-210')
  ),
  PARTITION p_yyyyy VALUES LESS THAN( 'ZZZZZ' )
  (
    SUBPARTITION p_yyyyy_1 values ('YYYYY-111'),
    SUBPARTITION p_yyyyy_2 values ('YYYYY-211'),
    SUBPARTITION p_yyyyy_3 values ('YYYYY-300'),
    SUBPARTITION p_yyyyy_4 values ('高斯数据库-210')
  ),
  PARTITION p_zzzzz VALUES LESS THAN( MAXVALUE )
  (
    SUBPARTITION p_zzzzz_1 values ('ZZZZZ-111'),
    SUBPARTITION p_zzzzz_2 values ('ZZZZZ-211'),
    SUBPARTITION p_zzzzz_3 values ('ZZZZZ-300'),
    SUBPARTITION p_zzzzz_4 values ('高斯数据库-210')
  )
);

INSERT INTO test_prefix_parttable VALUES(30, 'ZZZZZ-300', 'ZZZZZ-300', 'ZZZZZ-300', 'ZZZZZ-300', '5A5A5A5A5A2D333030', HEXTORAW('5A5A5A5A5A2D333030'),E'\\x5A5A5A5A5A2D333030');
INSERT INTO test_prefix_parttable VALUES(30, 'ZZZZZ-211', 'ZZZZZ-211', 'ZZZZZ-211', 'ZZZZZ-211', '5A5A5A5A5A2D323131', HEXTORAW('5A5A5A5A5A2D323131'),E'\\x5A5A5A5A5A2D323131');
INSERT INTO test_prefix_parttable VALUES(30, 'ZZZZZ-111', 'ZZZZZ-111', 'ZZZZZ-111', 'ZZZZZ-111', '5A5A5A5A5A2D313131', HEXTORAW('5A5A5A5A5A2D313131'),E'\\x5A5A5A5A5A2D313131');
INSERT INTO test_prefix_parttable VALUES(20, 'YYYYY-300', 'YYYYY-300', 'YYYYY-300', 'YYYYY-300', '59595959592D333030', HEXTORAW('59595959592D333030'),E'\\x59595959592D333030');
INSERT INTO test_prefix_parttable VALUES(20, 'YYYYY-211', 'YYYYY-211', 'YYYYY-211', 'YYYYY-211', '59595959592D323131', HEXTORAW('59595959592D323131'),E'\\x59595959592D323131');
INSERT INTO test_prefix_parttable VALUES(20, 'YYYYY-111', 'YYYYY-111', 'YYYYY-111', 'YYYYY-111', '59595959592D313131', HEXTORAW('59595959592D313131'),E'\\x59595959592D313131');
INSERT INTO test_prefix_parttable VALUES(20, '高斯数据库-210', '高斯数据库-210', '高斯数据库-210', '高斯数据库-210', 'e9ab98e696afe695b0e68daee5ba932d323130', HEXTORAW('e9ab98e696afe695b0e68daee5ba932d323130'),E'\\xe9ab98e696afe695b0e68daee5ba932d323130');
INSERT INTO test_prefix_parttable VALUES(10, 'XXXXX-300', 'XXXXX-300', 'XXXXX-300', 'XXXXX-300', '58585858582D333030', HEXTORAW('58585858582D333030'),E'\\x58585858582D333030');
INSERT INTO test_prefix_parttable VALUES(10, 'XXXXX-211', 'XXXXX-211', 'XXXXX-211', 'XXXXX-211', '58585858582D323131', HEXTORAW('58585858582D323131'),E'\\x58585858582D323131');
INSERT INTO test_prefix_parttable VALUES(10, 'XXXXX-111', 'XXXXX-111', 'XXXXX-111', 'XXXXX-111', '58585858582D313131', HEXTORAW('58585858582D313131'),E'\\x58585858582D313131');
--global index not support
CREATE INDEX error_index_ftext_global ON test_prefix_parttable(ftext(5)) GLOBAL;
--local index
CREATE INDEX prefix_index_ftext_part ON test_prefix_parttable(ftext(5)) LOCAL
(
PARTITION prefix_index_ftext_part1 (SUBPARTITION prefix_index_ftext_subpart11, SUBPARTITION prefix_index_ftext_subpart12,SUBPARTITION prefix_index_ftext_subpart13, SUBPARTITION prefix_index_ftext_subpart14),
PARTITION prefix_index_ftext_part2 (SUBPARTITION prefix_index_ftext_subpart21, SUBPARTITION prefix_index_ftext_subpart22,SUBPARTITION prefix_index_ftext_subpart23, SUBPARTITION prefix_index_ftext_subpart24),
PARTITION prefix_index_ftext_part3 (SUBPARTITION prefix_index_ftext_subpart31, SUBPARTITION prefix_index_ftext_subpart32,SUBPARTITION prefix_index_ftext_subpart33, SUBPARTITION prefix_index_ftext_subpart34)
);
set enable_seqscan=false;

EXPLAIN (costs false)
SELECT ftext FROM test_prefix_parttable where ftext like '高斯%' ORDER BY 1;
SELECT ftext FROM test_prefix_parttable where ftext like '高斯%' ORDER BY 1;

--unique
CREATE UNIQUE INDEX prefix_index_ftext_part_unique ON test_prefix_parttable(ftext, fvchar, fbytea(5)) LOCAL;
INSERT INTO test_prefix_parttable VALUES(10, 'XXXXX-111', 'XXXXX-111', 'XXXXX-111', 'XXXXX-111', '58585858582D313131', HEXTORAW('58585858582D313131'),E'\\x58585858582D313131');
EXPLAIN (costs false)
SELECT fbytea FROM test_prefix_parttable t where fbytea = E'\\x59595959592D323131' ORDER BY 1;
SELECT fbytea FROM test_prefix_parttable t where fbytea = E'\\x59595959592D323131' ORDER BY 1;

RESET enable_seqscan;
DROP TABLE IF EXISTS test_prefix_parttable;

--ustore
CREATE TABLE test_prefix_ustore(
    id INTEGER,
    fchar CHAR(64),
    fvchar VARCHAR(64),
    ftext TEXT,
    fclob CLOB,
    fblob BLOB,
    fraw RAW,
    fbytea BYTEA
) WITH (STORAGE_TYPE=USTORE);

INSERT INTO test_prefix_ustore VALUES(0, NULL, NULL, NULL, NULL , NULL, NULL, NULL);
INSERT INTO test_prefix_ustore VALUES(30, 'ZZZZZ-123', 'ZZZZZ-123', 'ZZZZZ-123', 'ZZZZZ-123', '5A5A5A5A5A2D313233', HEXTORAW('5A5A5A5A5A2D313233'),E'\\x5A5A5A5A5A2D313233');
INSERT INTO test_prefix_ustore VALUES(30, 'ZZZZZ-300', 'ZZZZZ-300', 'ZZZZZ-300', 'ZZZZZ-300', '5A5A5A5A5A2D333030', HEXTORAW('5A5A5A5A5A2D333030'),E'\\x5A5A5A5A5A2D333030');
INSERT INTO test_prefix_ustore VALUES(30, 'ZZZZZ-000', 'ZZZZZ-000', 'ZZZZZ-000', 'ZZZZZ-000', '5A5A5A5A5A2D303030', HEXTORAW('5A5A5A5A5A2D303030'),E'\\x5A5A5A5A5A2D303030');
INSERT INTO test_prefix_ustore VALUES(30, 'ZZZZZ-211', 'ZZZZZ-211', 'ZZZZZ-211', 'ZZZZZ-211', '5A5A5A5A5A2D323131', HEXTORAW('5A5A5A5A5A2D323131'),E'\\x5A5A5A5A5A2D323131');
INSERT INTO test_prefix_ustore VALUES(30, 'ZZZZZ-111', 'ZZZZZ-111', 'ZZZZZ-111', 'ZZZZZ-111', '5A5A5A5A5A2D313131', HEXTORAW('5A5A5A5A5A2D313131'),E'\\x5A5A5A5A5A2D313131');
INSERT INTO test_prefix_ustore VALUES(30, 'ZZZZZ-210', 'ZZZZZ-210', 'ZZZZZ-210', 'ZZZZZ-210', '5A5A5A5A5A2D323130', HEXTORAW('5A5A5A5A5A2D323130'),E'\\x5A5A5A5A5A2D323130');
INSERT INTO test_prefix_ustore VALUES(30, 'Z', 'Z', 'Z', 'Z', '5A', HEXTORAW('5A'),E'\\x5A');
INSERT INTO test_prefix_ustore VALUES(0, NULL, NULL, NULL, NULL , NULL, NULL, NULL);
INSERT INTO test_prefix_ustore VALUES(20, 'YYYYY-300', 'YYYYY-300', 'YYYYY-300', 'YYYYY-300', '59595959592D333030', HEXTORAW('59595959592D333030'),E'\\x59595959592D333030');
INSERT INTO test_prefix_ustore VALUES(20, 'YYYYY-000', 'YYYYY-000', 'YYYYY-000', 'YYYYY-000', '59595959592D303030', HEXTORAW('59595959592D303030'),E'\\x59595959592D303030');
INSERT INTO test_prefix_ustore VALUES(20, 'YYYYY-211', 'YYYYY-211', 'YYYYY-211', 'YYYYY-211', '59595959592D323131', HEXTORAW('59595959592D323131'),E'\\x59595959592D323131');
INSERT INTO test_prefix_ustore VALUES(20, 'YYYYY-111', 'YYYYY-111', 'YYYYY-111', 'YYYYY-111', '59595959592D313131', HEXTORAW('59595959592D313131'),E'\\x59595959592D313131');
INSERT INTO test_prefix_ustore VALUES(20, 'YYYYY-210', 'YYYYY-210', 'YYYYY-210', 'YYYYY-210', '59595959592D323130', HEXTORAW('59595959592D323130'),E'\\x59595959592D323130');
INSERT INTO test_prefix_ustore VALUES(20, 'Y', 'Y', 'Y', 'Y', '59', HEXTORAW('59'),E'\\x59');
INSERT INTO test_prefix_ustore VALUES(20, '高斯数据库-210', '高斯数据库-210', '高斯数据库-210', '高斯数据库-210', 'e9ab98e696afe695b0e68daee5ba932d323130', HEXTORAW('e9ab98e696afe695b0e68daee5ba932d323130'),E'\\xe9ab98e696afe695b0e68daee5ba932d323130');
INSERT INTO test_prefix_ustore VALUES(20, '开源数据库-210', '开源数据库-210', '开源数据库-210', '开源数据库-210', 'e5bc80e6ba90e695b0e68daee5ba932d323130', HEXTORAW('e5bc80e6ba90e695b0e68daee5ba932d323130'),E'\\xe5bc80e6ba90e695b0e68daee5ba932d323130');
INSERT INTO test_prefix_ustore VALUES(20, '高', '高', '高', '高', 'e9ab98', HEXTORAW('e9ab98'),E'\\xe9ab98');
--err
CREATE INDEX error_index_fchar ON test_prefix_ustore (fchar(0));
CREATE INDEX error_index_fchar ON test_prefix_ustore (fchar(-1));
CREATE INDEX error_index_fchar ON test_prefix_ustore (fchar(1+1));
CREATE INDEX error_index_fvchar ON test_prefix_ustore (fvchar(80));
CREATE INDEX error_index_ftext ON test_prefix_ustore (ftext(4096));
CREATE INDEX error_index_id ON test_prefix_ustore (id(5));
CREATE INDEX error_index_fchar ON test_prefix_ustore using hash (fchar(5));
CREATE INDEX error_index_fchar ON test_prefix_ustore using gin (fchar(5));
CREATE INDEX error_index_fchar ON test_prefix_ustore using gist (fchar(5));
--success
CREATE INDEX prefix_index_fchar_fbytea ON test_prefix_ustore (fchar(5), fbytea(5));
CREATE INDEX prefix_index_fvchar ON test_prefix_ustore (fvchar(5));
CREATE INDEX prefix_index_ftext ON test_prefix_ustore (ftext(5));
CREATE INDEX prefix_index_fblob ON test_prefix_ustore (fblob(5));
CREATE UNIQUE INDEX prefix_index_fraw ON test_prefix_ustore (fraw(9));
\d+ test_prefix_ustore
select pg_get_tabledef('test_prefix_ustore'::regclass);


set enable_seqscan=false;
set enable_opfusion=false;
set enable_partition_opfusion=false;


SELECT ftext FROM test_prefix_ustore where ftext like 'XXXXX%' ORDER BY 1;
SELECT fblob FROM test_prefix_ustore where fblob < '58585858582D333030' ORDER BY 1;
SELECT fchar, fbytea FROM test_prefix_ustore where (fchar, fbytea)= ('XXXXX-211', 'XXXXX-211') ORDER BY 1,2;
--insert
INSERT INTO test_prefix_ustore VALUES(0, NULL, NULL, NULL, NULL , NULL, NULL, NULL);
INSERT INTO test_prefix_ustore VALUES(10, 'XXXXX-300', 'XXXXX-300', 'XXXXX-300', 'XXXXX-300', '58585858582D333030', HEXTORAW('58585858582D333030'),E'\\x58585858582D333030');
INSERT INTO test_prefix_ustore VALUES(10, 'XXXXX-000', 'XXXXX-000', 'XXXXX-000', 'XXXXX-000', '58585858582D303030', HEXTORAW('58585858582D303030'),E'\\x58585858582D303030');
INSERT INTO test_prefix_ustore VALUES(10, 'XXXXX-211', 'XXXXX-211', 'XXXXX-211', 'XXXXX-211', '58585858582D323131', HEXTORAW('58585858582D323131'),E'\\x58585858582D323131');
INSERT INTO test_prefix_ustore VALUES(10, 'XXXXX-111', 'XXXXX-111', 'XXXXX-111', 'XXXXX-111', '58585858582D313131', HEXTORAW('58585858582D313131'),E'\\x58585858582D313131');
INSERT INTO test_prefix_ustore VALUES(10, 'XXXXX-210', 'XXXXX-210', 'XXXXX-210', 'XXXXX-210', '58585858582D323130', HEXTORAW('58585858582D323130'),E'\\x58585858582D323130');

SELECT ftext FROM test_prefix_ustore where ftext like 'XXXXX%' ORDER BY 1;
SELECT fblob FROM test_prefix_ustore where fblob < '58585858582D333030' ORDER BY 1;
SELECT fchar, fbytea FROM test_prefix_ustore where (fchar, fbytea)= ('XXXXX-211', 'XXXXX-211') ORDER BY 1,2;

--update
UPDATE test_prefix_ustore SET fchar=replace(fchar, 'XXXXX', 'AAAAA'), ftext=replace(ftext, 'XXXXX', 'AAAAA') where fvchar like 'XXXXX%';

SELECT ftext FROM test_prefix_ustore where ftext like 'AAAAA%' ORDER BY 1;
SELECT fblob FROM test_prefix_ustore where fblob < '58585858582D333030' ORDER BY 1;
SELECT fchar, fbytea FROM test_prefix_ustore where (fchar, fbytea)= ('AAAAA-211', 'XXXXX-211') ORDER BY 1,2;

--delete
DELETE FROM test_prefix_ustore where (fchar, fbytea)= ('AAAAA-211', 'XXXXX-211');

SELECT ftext FROM test_prefix_ustore where ftext like 'AAAAA%' ORDER BY 1;
SELECT fblob FROM test_prefix_ustore where fblob < '58585858582D333030' ORDER BY 1;
SELECT fchar, fbytea FROM test_prefix_ustore where (fchar, fbytea)= ('AAAAA-211', 'XXXXX-211') ORDER BY 1,2;

--check query plan
analyze test_prefix_ustore;
--single table index scan
EXPLAIN (costs false)
SELECT ftext FROM test_prefix_ustore t where ftext like 'YYYY%' ORDER BY 1;
SELECT ftext FROM test_prefix_ustore t where ftext like 'YYYY%' ORDER BY 1;
EXPLAIN (costs false)
SELECT ftext FROM test_prefix_ustore t where ftext like '开源数据库-210' ORDER BY 1;
SELECT ftext FROM test_prefix_ustore t where ftext like '开源数据库-210' ORDER BY 1;
EXPLAIN (costs false)
SELECT fchar FROM test_prefix_ustore t where fchar ~~ '高斯数据库' ORDER BY 1;
SELECT fchar FROM test_prefix_ustore t where fchar ~~ '高斯数据库' ORDER BY 1;
EXPLAIN (costs false)
SELECT fchar FROM test_prefix_ustore t where fchar ~ '^开' ORDER BY 1;
SELECT fchar FROM test_prefix_ustore t where fchar ~ '^开' ORDER BY 1;
EXPLAIN (costs false)
SELECT fbytea FROM test_prefix_ustore t where fbytea like '高%' ORDER BY 1;
SELECT fbytea FROM test_prefix_ustore t where fbytea like '高%' ORDER BY 1;
EXPLAIN (costs false)
SELECT fbytea FROM test_prefix_ustore t where fbytea ~~ '开源数据库%' ORDER BY 1;
SELECT fbytea FROM test_prefix_ustore t where fbytea ~~ '开源数据库%' ORDER BY 1;
EXPLAIN (costs false)
SELECT fbytea FROM test_prefix_ustore t where fbytea ~~ 'YYYYY-%' ORDER BY 1;
SELECT fbytea FROM test_prefix_ustore t where fbytea ~~ 'YYYYY-%' ORDER BY 1;
EXPLAIN (costs false)
SELECT ftext FROM test_prefix_ustore t where ftext <= 'YYYYY-100' ORDER BY 1;
SELECT ftext FROM test_prefix_ustore t where ftext <= 'YYYYY-100' ORDER BY 1;
EXPLAIN (costs false)
SELECT ftext FROM test_prefix_ustore t where ftext <= 'YYYYY' ORDER BY 1;
SELECT ftext FROM test_prefix_ustore t where ftext <= 'YYYYY' ORDER BY 1;
EXPLAIN (costs false)
SELECT fvchar FROM test_prefix_ustore t where fvchar < 'ZZZZZ-210' ORDER BY 1;
SELECT fvchar FROM test_prefix_ustore t where fvchar < 'ZZZZZ-210' ORDER BY 1;
EXPLAIN (costs false)
SELECT fvchar FROM test_prefix_ustore t where fvchar < 'Z' ORDER BY 1;
SELECT fvchar FROM test_prefix_ustore t where fvchar < 'Z' ORDER BY 1;
EXPLAIN (costs false)
SELECT ftext FROM test_prefix_ustore t where fvchar >= 'ZZZZZ-210' ORDER BY 1;
SELECT ftext FROM test_prefix_ustore t where fvchar >= 'ZZZZZ-210' ORDER BY 1;
EXPLAIN (costs false)
SELECT ftext FROM test_prefix_ustore t where fvchar >= 'ZZZZZ' ORDER BY 1;
SELECT ftext FROM test_prefix_ustore t where fvchar >= 'ZZZZZ' ORDER BY 1;
EXPLAIN (costs false)
SELECT ftext FROM test_prefix_ustore t where fvchar > 'ZZZZZ-' and fvchar <> 'ZZZZZ-210' ORDER BY 1;
SELECT ftext FROM test_prefix_ustore t where fvchar > 'ZZZZZ-' and fvchar <> 'ZZZZZ-210' ORDER BY 1;
EXPLAIN (costs false)
SELECT ftext FROM test_prefix_ustore t where fvchar > left('ZZZZZ-211', 7) and fvchar <> 'ZZZZZ-211' ORDER BY 1;
SELECT ftext FROM test_prefix_ustore t where fvchar > left('ZZZZZ-211', 7) and fvchar <> 'ZZZZZ-211' ORDER BY 1;
EXPLAIN (costs false)
SELECT fbytea FROM test_prefix_ustore t where fbytea = E'\\x59595959592D323131' ORDER BY 1;
SELECT fbytea FROM test_prefix_ustore t where fbytea = E'\\x59595959592D323131' ORDER BY 1;
EXPLAIN (costs false)
SELECT fbytea FROM test_prefix_ustore t where fbytea = E'\\x59' ORDER BY 1;
SELECT fbytea FROM test_prefix_ustore t where fbytea = E'\\x59' ORDER BY 1;
EXPLAIN (costs false)
SELECT fblob FROM test_prefix_ustore t where fblob IS NOT NULL ORDER BY 1;
SELECT fblob FROM test_prefix_ustore t where fblob IS NOT NULL ORDER BY 1;
EXPLAIN (costs false)
SELECT DISTINCT fraw FROM test_prefix_ustore t where fraw IS NOT NULL ORDER BY 1;
SELECT DISTINCT fraw FROM test_prefix_ustore t where fraw IS NOT NULL ORDER BY 1;
EXPLAIN (costs false)
SELECT fraw FROM test_prefix_ustore t where fraw IS NULL or ftext like '高%' ORDER BY 1;
SELECT fraw FROM test_prefix_ustore t where fraw IS NULL or ftext like '高%' ORDER BY 1;
EXPLAIN (costs false)
SELECT fvchar FROM test_prefix_ustore t where fraw IS NULL or fvchar > 'ZZZZZ-200' and fvchar <> 'ZZZZZ-210' ORDER BY 1;
SELECT fvchar FROM test_prefix_ustore t where fraw IS NULL or fvchar > 'ZZZZZ-200' and fvchar <> 'ZZZZZ-210' ORDER BY 1;
--prefix index not used
EXPLAIN (costs false)
SELECT fvchar FROM test_prefix_ustore t where fvchar <> 'ZZZZZ-210';
EXPLAIN (costs false)
SELECT left(ftext, 5) FROM test_prefix_ustore where left(ftext, 5) = 'YYYYY';
EXPLAIN (costs false)
SELECT ftext FROM test_prefix_ustore ORDER BY ftext;
EXPLAIN (costs false)
select * from test_prefix_ustore tab1 where (fchar, fbytea)<('YYYYY-210', E'\\x59595959592D323130');

set enable_bitmapscan=false;
set enable_material=false;
set enable_hashjoin=false;
set enable_mergejoin=false;
--join index scan
EXPLAIN (costs false)
SELECT t1.ftext,t2.fchar FROM test_prefix_ustore t1 join test_prefix_ustore t2 on  t1.ftext = t2.fchar ORDER BY 1,2;
SELECT t1.ftext,t2.fchar FROM test_prefix_ustore t1 join test_prefix_ustore t2 on  t1.ftext = t2.fchar ORDER BY 1,2;
EXPLAIN (costs false)
SELECT count(1) FROM test_prefix_ustore t1 join test_prefix_ustore t2 on (t2.fvchar LIKE 'X%' AND t1.ftext > t2.fvchar);
SELECT count(1) FROM test_prefix_ustore t1 join test_prefix_ustore t2 on (t2.fvchar LIKE 'X%' AND t1.ftext > t2.fvchar);
EXPLAIN (costs false)
SELECT t1.ftext FROM test_prefix_ustore t1 join test_prefix_ustore t2 on  t1.ftext = t2.fvchar where t1.id=30 and t2.id > 10 ORDER BY 1;
SELECT t1.ftext FROM test_prefix_ustore t1 join test_prefix_ustore t2 on  t1.ftext = t2.fvchar where t1.id=30 and t2.id > 10 ORDER BY 1;
EXPLAIN (costs false)
SELECT t1.ftext,t2.fvchar FROM test_prefix_ustore t1, test_prefix_ustore t2 where t1.ftext = t2.fvchar and t1.id=30 ORDER BY 1,2;
SELECT t1.ftext,t2.fvchar FROM test_prefix_ustore t1, test_prefix_ustore t2 where t1.ftext = t2.fvchar and t1.id=30 ORDER BY 1,2;
EXPLAIN (costs false)
SELECT t1.fvchar,t2.fvchar FROM test_prefix_ustore t1 left join test_prefix_ustore t2 on (t1.fvchar = t2.fvchar and t2.fvchar > 'ZZZZZ-3' ) ORDER BY 1,2;
SELECT t1.fvchar,t2.fvchar FROM test_prefix_ustore t1 left join test_prefix_ustore t2 on (t1.fvchar = t2.fvchar and t2.fvchar > 'ZZZZZ-3' ) ORDER BY 1,2;
--prefix index not used
EXPLAIN (costs false)
SELECT t1.ftext,t2.fvchar FROM test_prefix_ustore t1 join test_prefix_ustore t2 on  t1.ftext like t2.fvchar;
EXPLAIN (costs false)
SELECT t1.ftext,t2.fvchar FROM test_prefix_ustore t1 join test_prefix_ustore t2 on  t1.ftext <> t2.fvchar;

--alter table
ALTER TABLE test_prefix_ustore MODIFY ftext varchar(64);
ALTER TABLE test_prefix_ustore ALTER COLUMN ftext TYPE text;
EXPLAIN (costs false)
DELETE FROM test_prefix_ustore WHERE ftext IS NULL;
DELETE FROM test_prefix_ustore WHERE ftext IS NULL;
ALTER TABLE test_prefix_ustore ALTER COLUMN ftext SET NOT NULL;
ALTER TABLE test_prefix_ustore ALTER COLUMN ftext DROP NOT NULL;
EXPLAIN (costs false)
SELECT ftext FROM test_prefix_ustore where ftext = '高斯数据库-210' ORDER BY 1;
SELECT ftext FROM test_prefix_ustore where ftext = '高斯数据库-210' ORDER BY 1;


CREATE SCHEMA prefix_index_schema;
ALTER TABLE test_prefix_ustore SET SCHEMA prefix_index_schema;
set current_schema = prefix_index_schema;
set enable_seqscan=false;
set enable_opfusion=false;
set enable_partition_opfusion=false;
EXPLAIN (costs false)
SELECT ftext FROM test_prefix_ustore where ftext >= '高斯数据库-210' ORDER BY 1;
SELECT ftext FROM test_prefix_ustore where ftext >= '高斯数据库-210' ORDER BY 1;

ALTER TABLE test_prefix_ustore RENAME TO test_prefix_utb;
ALTER TABLE test_prefix_utb RENAME COLUMN fchar TO fbpchar;
ALTER TABLE test_prefix_utb DROP ftext;
EXPLAIN (costs false)
SELECT fbpchar FROM test_prefix_utb where fbpchar > '开源' ORDER BY 1;
SELECT fbpchar FROM test_prefix_utb where fbpchar > '开源' ORDER BY 1;
ALTER INDEX prefix_index_fchar_fbytea UNUSABLE;
EXPLAIN (costs false)
SELECT fbpchar FROM test_prefix_utb where fbpchar like '高斯数据库-%' ORDER BY 1;

RESET enable_seqscan;
RESET enable_opfusion;
RESET enable_partition_opfusion;
DROP TABLE IF EXISTS test_prefix_utb;
RESET current_schema;
DROP SCHEMA prefix_index_schema CASCADE;

RESET enable_bitmapscan;
RESET enable_material;
RESET enable_hashjoin;
RESET enable_mergejoin;
RESET enable_seqscan;
RESET enable_opfusion;
RESET enable_partition_opfusion;

--cstore not support
DROP TABLE IF EXISTS test_prefix_cstore;
CREATE TABLE test_prefix_cstore(
    id INTEGER,
    fchar CHAR(64),
    fvchar VARCHAR(64),
    ftext TEXT,
    fclob CLOB,
    fbytea BYTEA
) WITH (ORIENTATION=column, COMPRESSION=high, COMPRESSLEVEL=2);

CREATE INDEX prefix_cindex_fchar_fbytea ON test_prefix_cstore using btree (fchar(5), fbytea(5));
CREATE INDEX prefix_cindex_ftext ON test_prefix_cstore (ftext(5));
DROP TABLE IF EXISTS test_prefix_cstore;

-- fk not supported
CREATE TABLE test_foreign(
    a text,
    FOREIGN KEY (a(9)) REFERENCES test_prefix_table(ftext) ON DELETE SET NULL
);
CREATE TABLE test_foreign(
    a text,
    FOREIGN KEY (a(9)) REFERENCES test_prefix_table(ftext(9)) ON DELETE SET NULL
);
CREATE TABLE test_foreign(
    a varchar(64),
    FOREIGN KEY (a(9)) REFERENCES test_prefix_table(ftext) ON DELETE SET NULL
);
CREATE TABLE test_foreign(
    a varchar(64),
    FOREIGN KEY (a(9)) REFERENCES test_prefix_table(ftext(9)) ON DELETE SET NULL
);
CREATE TABLE test_foreign(
    a varchar(64),
    FOREIGN KEY (a) REFERENCES test_prefix_table(ftext) ON DELETE SET NULL
);

-- alter table add CONSTRAINT
CREATE TABLE test_prefix_table(
    id INTEGER,
    fchar CHAR(64),
    "Fvchar" VARCHAR(64),
    ftext TEXT
);
INSERT INTO test_prefix_table VALUES(20, 'YYYYY-300', 'YYYYY-300', 'YYYYY-300');
INSERT INTO test_prefix_table VALUES(20, 'YYYYY-000', 'YYYYY-000', 'YYYYY-000');
INSERT INTO test_prefix_table VALUES(20, 'YYYYY-211', 'YYYYY-211', 'YYYYY-211');
INSERT INTO test_prefix_table VALUES(20, 'YYYYY-111', 'YYYYY-111', 'YYYYY-111');
INSERT INTO test_prefix_table VALUES(20, 'YYYYY-210', 'YYYYY-210', 'YYYYY-210');
INSERT INTO test_prefix_table VALUES(20, 'Y', 'Y', 'Y');
INSERT INTO test_prefix_table VALUES(20, '高斯数据库-210', '高斯数据库-210', '高斯数据库-210');
INSERT INTO test_prefix_table VALUES(20, '开源数据库-210', '开源数据库-210', '开源数据库-210');
INSERT INTO test_prefix_table VALUES(20, '高-210', '高-210', '高-210');

-- -- ADD CONSTRAINT
ALTER TABLE test_prefix_table ADD CONSTRAINT prefix_pk1 PRIMARY KEY(ftext(5)); -- not supported
ALTER TABLE test_prefix_table ADD CONSTRAINT prefix_uk1 UNIQUE(ftext(5)); -- duplicate key
ALTER TABLE test_prefix_table ADD CONSTRAINT prefix_uk1 UNIQUE(ftext(9));
ALTER TABLE test_prefix_table ADD CONSTRAINT prefix_uk2 UNIQUE("Fvchar"(9));
SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conname='prefix_uk2';
SELECT pg_get_indexdef('prefix_uk2'::regclass);
SELECT pg_get_tabledef('test_prefix_table'::regclass);
\d test_prefix_table

-- -- -- constraint TEST
INSERT INTO test_prefix_table VALUES(20, 'YYYYY-300', 'YYYYY-300', 'YYYYY-300'); -- duplicate key
INSERT INTO test_prefix_table VALUES(20, '高斯数据库-210', '高斯数据库-210', '高斯数据库-210'); -- duplicate key
INSERT INTO test_prefix_table VALUES(20, '开源数据库-2101', '开源数据库-2101', '开源数据库-2101'); -- duplicate key

-- -- -- QUERY PLAN
set enable_seqscan=false;
EXPLAIN (costs false)
SELECT ftext FROM test_prefix_table t where ftext like 'YYYY%' ORDER BY 1;
SELECT ftext FROM test_prefix_table t where ftext like 'YYYY%' ORDER BY 1;
EXPLAIN (costs false)
SELECT ftext FROM test_prefix_table t where ftext like '开源数据库-210%' ORDER BY 1;
SELECT ftext FROM test_prefix_table t where ftext like '开源数据库-210%' ORDER BY 1;
EXPLAIN (costs false)
SELECT ftext FROM test_prefix_table t where ftext < 'YYYYY-210' ORDER BY 1;
SELECT ftext FROM test_prefix_table t where ftext < 'YYYYY-210' ORDER BY 1;
EXPLAIN (costs false)
SELECT ftext FROM test_prefix_table t where ftext >= 'YYYYY' ORDER BY 1;
SELECT ftext FROM test_prefix_table t where ftext >= 'YYYYY' ORDER BY 1;
EXPLAIN (costs false)
SELECT ftext FROM test_prefix_table t where ftext IS NULL ORDER BY 1;
SELECT ftext FROM test_prefix_table t where ftext IS NULL ORDER BY 1;

DROP TABLE test_prefix_table;

-- create table CONSTRAINTS
CREATE TABLE test_prefix_table(
    id INTEGER,
    fchar CHAR(64),
    "Fvchar" VARCHAR(64),
    ftext TEXT,
    CONSTRAINT prefix_pk1 PRIMARY KEY(ftext(5))
); -- error
CREATE TABLE test_prefix_table(
    id INTEGER,
    fchar CHAR(64),
    "Fvchar" VARCHAR(64),
    ftext TEXT,
    CONSTRAINT prefix_pk1 UNIQUE(ftext(5))
);
INSERT INTO test_prefix_table VALUES(20, '高斯数据库-210', '高斯数据库-210', '高斯数据库-210');
INSERT INTO test_prefix_table VALUES(20, '高斯数据库-210', '高斯数据库-210', '高斯数据库-210'); -- duplicate key
DROP TABLE test_prefix_table;

\c regression
drop database prefix_index_db;