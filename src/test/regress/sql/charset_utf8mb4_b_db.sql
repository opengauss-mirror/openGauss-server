CREATE DATABASE c_utf8mb4_b_db WITH ENCODING 'utf8mb4' LC_COLLATE='C' LC_CTYPE='C' DBCOMPATIBILITY 'B';
\c c_utf8mb4_b_db

-- test _charset syntax
-- -- test _charset COMPATIBILITY
SET b_format_behavior_compat_options = '';
SHOW b_format_behavior_compat_options;
SELECT _gbk'ABCD' = _utf8mb4'ABCD'; -- ERROR
CREATE TABLE test_charset_syntax(_utf8 text);
DROP TABLE test_charset_syntax;

SET b_format_behavior_compat_options = 'all';
SHOW b_format_behavior_compat_options;
SELECT _gbk'ABCD' = _utf8mb4'ABCD';
CREATE TABLE test_charset_syntax(_utf8 text); -- ERROR
CREATE TABLE test_charset_syntax(_column text);
DROP TABLE test_charset_syntax;

-- -- test _charset + expression
SELECT _utf8mb4 10; -- ERROR
SELECT _utf8mb4 CONCAT('高斯', 'DB'); -- ERROR
SELECT _utf8_mb4'ABCD' = _Utf-8mB4'ABCD'; -- ERROR
SELECT __utf8mb4'ABCD' = __Utf8mB4'ABCD'; -- special
SELECT _gbk'ABCD' = _GbK'ABCD';
SELECT _binary E'', _binary E'' IS NULL;
SELECT _binary E'\\xe9ab98e696af';
SELECT _utf8mb4 E'\\xe9ab98e696af';
SELECT _gbk E'\\xe9ab98e696af';
SELECT _utf8mb4 X'\\xe9ab98e696af'; -- ERROR, X'string' equal 'string'
SELECT _binary X'', _binary X'' IS NULL;
SELECT _binary X'E9AB98E696AF';
SELECT _utf8mb4 X'E9AB98E696AF';
SELECT _gbk X'E9AB98E696AF';
SELECT _binary B'', _binary B'' IS NULL;
SELECT _binary B'0';
SELECT _binary B'111010011010101110011000111001101001011010101111';
SELECT _utf8mb4 B'111010011010101110011000111001101001011010101111';
SELECT _gbk B'111010011010101110011000111001101001011010101111';
SELECT _utf8mb4 B'111010011010101110011000111001101001011010101111高斯'; -- ERROR
SELECT _binary X'000D' | X'0BC0'; -- ERROR not support yet

-- ------------------------------------------
-- SET NAMES utf8mb4;
SET NAMES 'utf8mb4';
SHOW client_encoding;
SHOW server_encoding;
SHOW character_set_connection;
SHOW collation_connection;

-- -- CONST charset and collation
SELECT _utf8mb4'高斯' COLLATE gbk_chinese_ci; -- ERROR
SELECT _utf8mb4'高斯' COLLATE 'binary'; -- ERROR
SELECT _utf8mb4'高斯' COLLATE "zh_CN.utf8"; -- support origin collation
SELECT _utf8mb4'高斯' COLLATE "C"; -- support origin collation
SELECT _gbk'高斯' COLLATE gbk_chinese_ci;
SELECT _gbk'高斯' COLLATE 'binary'; -- ERROR
SELECT _gbk'高斯' COLLATE "zh_CN.gbk"; -- ERROR
SELECT _gbk'高斯' COLLATE "C"; -- ERROR
SELECT _gb18030'高斯' COLLATE gb18030_chinese_ci;
SELECT _gb18030'高斯' COLLATE gbk_chinese_ci; -- ERROR
SELECT _gb18030'高斯' COLLATE 'binary'; -- ERROR
SELECT _gb18030'高斯' COLLATE "zh_CN.gb18030"; -- ERROR
SELECT _gb18030'高斯' COLLATE "C"; -- ERROR
SELECT _binary'高斯' COLLATE gbk_chinese_ci; -- ERROR
SELECT _binary'高斯' COLLATE 'binary';
SELECT _binary'高斯' COLLATE "zh_CN.utf8"; -- ERROR
SELECT _binary'高斯' COLLATE "C"; -- ERROR
SELECT _binary'' COLLATE utf8mb4_bin; -- ERROR
SELECT (_utf8mb4'高斯' COLLATE utf8mb4_bin) COLLATE gbk_chinese_ci; -- ERROR
SELECT (_utf8mb4'高斯' COLLATE utf8mb4_bin) COLLATE 'binary'; -- ERROR
SELECT (_utf8mb4'高斯' COLLATE utf8mb4_bin) COLLATE "zh_CN.utf8"; -- support origin collation
SELECT (_utf8mb4'高斯' COLLATE utf8mb4_bin) COLLATE "C"; -- support origin collation
SELECT (_utf8mb4'高斯' COLLATE utf8mb4_bin) COLLATE utf8mb4_unicode_ci;
SELECT (_gbk'高斯' COLLATE "C") COLLATE utf8mb4_bin; -- ERROR
SELECT (_gbk'高斯' COLLATE "C") COLLATE gbk_chinese_ci; -- ERROR
SELECT (_binary'高斯' COLLATE "binary") COLLATE utf8mb4_bin; -- ERROR

-- -- CONST collation only
SELECT X'E9AB98E696AF' COLLATE "utf8mb4_unicode_ci"; -- ERROR
SELECT X'E9AB98E696AF' COLLATE "binary";
SELECT B'111010011010101110011000111001101001011010101111' COLLATE "utf8mb4_unicode_ci"; -- ERROR
SELECT B'111010011010101110011000111001101001011010101111' COLLATE "binary";
SELECT 1 COLLATE "utf8mb4_unicode_ci"; -- not support yet
SELECT 1 COLLATE "binary"; -- not support yet
SELECT CAST('高斯' AS bytea) COLLATE "utf8mb4_unicode_ci"; -- ERROR
SELECT CAST('高斯' AS bytea) COLLATE "binary";
SELECT CAST('E9AB98E696AF' AS blob) COLLATE "utf8mb4_unicode_ci"; -- ERROR
SELECT CAST('E9AB98E696AF' AS blob) COLLATE "binary";
SELECT '高斯' COLLATE "utf8mb4_unicode_ci";
SELECT '高斯' COLLATE "gbk_chinese_ci"; -- ERROR
SELECT '高斯' COLLATE "gb18030_chinese_ci"; -- ERROR
SELECT '高斯' COLLATE "binary"; -- ERROR

-- test CollateExpr
CREATE TABLE t_collate_expr(
    ftext text collate utf8mb4_bin,
    fbytea bytea,
    fvbit varbit(8),
    fint int
);
-- -- test INSERT
INSERT INTO t_collate_expr(ftext) VALUES('01100001' collate "binary"); -- ERROR
INSERT INTO t_collate_expr(ftext) VALUES('01100001' collate gbk_bin); -- ERROR
INSERT INTO t_collate_expr(ftext) VALUES('01100001' collate utf8mb4_unicode_ci);
INSERT INTO t_collate_expr(ftext) VALUES('01100001' collate gbk_bin collate utf8mb4_unicode_ci); -- only reserve top collate
INSERT INTO t_collate_expr(fbytea) VALUES('01100001' collate "binary"); -- do not check collate
INSERT INTO t_collate_expr(fbytea) VALUES('01100001' collate gbk_bin); -- do not check collate
INSERT INTO t_collate_expr(fbytea) VALUES('01100001' collate utf8mb4_unicode_ci);
INSERT INTO t_collate_expr(fvbit) VALUES('01100001' collate "binary"); -- do not check collate
INSERT INTO t_collate_expr(fvbit) VALUES('01100001' collate gbk_bin); -- do not check collate
INSERT INTO t_collate_expr(fvbit) VALUES('01100001' collate utf8mb4_unicode_ci);
INSERT INTO t_collate_expr(fint) VALUES('01100001' collate "binary"); -- do not check collate
INSERT INTO t_collate_expr(fint) VALUES('01100001' collate gbk_bin); -- do not check collate
INSERT INTO t_collate_expr(fint) VALUES('01100001' collate utf8mb4_unicode_ci);
INSERT INTO t_collate_expr(fbytea) VALUES('01100001' collate gbk_bin collate utf8mb4_unicode_ci); -- do not check collate
INSERT INTO t_collate_expr(fbytea) VALUES('01100001' collate utf8mb4_general_ci collate gbk_bin); -- do not check collate
INSERT INTO t_collate_expr(fvbit) VALUES('01100001' collate gbk_bin collate utf8mb4_unicode_ci); -- do not check collate
INSERT INTO t_collate_expr(fvbit) VALUES('01100001' collate utf8mb4_general_ci collate gbk_bin); -- do not check collate
INSERT INTO t_collate_expr(fint) VALUES('01100001' collate gbk_bin collate utf8mb4_unicode_ci); -- do not check collate
INSERT INTO t_collate_expr(fint) VALUES('01100001' collate utf8mb4_general_ci collate gbk_bin); -- do not check collate

-- -- test limit
select 1 from t_collate_expr limit(to_hex('11') collate "binary");
select 1 from t_collate_expr limit(to_hex('11') collate gbk_bin);
select 1 from t_collate_expr limit(to_hex('11') collate utf8mb4_unicode_ci);
select 1 from t_collate_expr limit(to_hex('11') collate gbk_bin collate utf8mb4_unicode_ci);  -- do not check collate

DROP TABLE t_collate_expr;

-- 中文 const charset
SELECT CAST('高斯' AS bytea);
SELECT CAST(_binary'高斯' AS bytea);
SELECT CAST(_utf8mb4'高斯' AS bytea);
SELECT CAST(_gbk'高斯' AS bytea);
SELECT _binary'高斯';
SELECT _utf8mb4'高斯';
SELECT _gbk'高斯';
SELECT _gb18030'高斯';
SELECT _gbk X'e9ab98e696af';
SELECT CONVERT_TO(_utf8mb4'楂樻柉', 'gbk');
SELECT CONVERT_TO(_utf8mb4'高斯', 'gbk');

-- test datatype
-- -- ARRAY TEST
SELECT array[_utf8mb4'高斯'];
SELECT array[_gbk'高斯'];
create table tarray(a text[]);
insert into tarray values(array[_utf8mb4'高斯']);
insert into tarray values(array[_gbk'高斯']); -- ERROR
insert into tarray values(_utf8mb4'{高斯}'::text[]);
insert into tarray values(_gbk'{高斯}'::text[]); -- ERROR
drop table tarray;
-- -- JSON TEST
SELECT _utf8mb4'"高斯"'::json;
SELECT  _gbk'"高斯"'::json; -- ERROR
create table tjson(a json);
insert into tjson values(_utf8mb4'"高斯"'::json);
insert into tjson values(_gbk'"高斯"'::json); -- ERROR
drop table tjson;
-- -- XML
SELECT xmlelement(NAME a, _utf8mb4'高斯');
SELECT xmlelement(NAME a, _gbk'高斯'); -- ERROR
create table txml(a xml);
insert into txml values(_utf8mb4'高斯'::xml);
insert into txml values(_gbk'高斯'::xml); -- ERROR
drop table txml;
-- -- tsvector
SELECT to_tsvector( _utf8mb4'高斯');
SELECT to_tsvector(_gbk'高斯'); -- ERROR
create table ttsvector(a tsvector);
insert into ttsvector values(_utf8mb4'高斯'::tsvector);
insert into ttsvector values(_gbk'高斯'::tsvector); -- ERROR
drop table ttsvector;
-- -- hll
SELECT hll_hash_text( _utf8mb4'高斯');
SELECT hll_hash_text(_gbk'高斯'); -- ERROR

-- test implicit type converion
select _gbk'111' collate "gbk_bin" = 111;

-- -- 中文 const compare
-- -- -- same charset & explicit collation
SELECT _utf8mb4'高斯DB' COLLATE utf8mb4_bin = _utf8mb4'高斯DB' COLLATE utf8mb4_unicode_ci; -- ERROR
SELECT _utf8mb4'高斯DB' COLLATE utf8mb4_general_ci = _utf8mb4'高斯DB' COLLATE utf8mb4_unicode_ci; -- ERROR
SELECT _utf8mb4'高斯DB' COLLATE utf8mb4_bin = _utf8mb4'高斯DB' COLLATE "C"; -- ERROR
SELECT _utf8mb4'高斯DB' COLLATE utf8mb4_bin = _utf8mb4'高斯DB' COLLATE "zh_CN.utf8"; -- ERROR
SELECT _utf8mb4'高斯DB' COLLATE utf8mb4_bin = _utf8mb4'高斯DB' COLLATE "DEFAULT"; -- ERROR
SELECT _utf8mb4'高斯DB' COLLATE utf8mb4_bin = '高斯DB' COLLATE utf8mb4_unicode_ci; -- ERROR
SELECT _utf8mb4'高斯DB' COLLATE utf8mb4_bin = '高斯DB' COLLATE utf8mb4_bin;
SELECT _utf8mb4'高斯db' COLLATE utf8mb4_unicode_ci = '高斯DB' COLLATE "C"; -- ERROR
SELECT _utf8mb4'高斯db' COLLATE utf8mb4_unicode_ci = '高斯DB' COLLATE "zh_CN.utf8"; -- ERROR
SELECT _gbk'高斯db' COLLATE gbk_bin = _gbk'高斯DB' COLLATE gbk_chinese_ci; -- ERROR
SELECT _gb18030'高斯db' COLLATE gb18030_bin = _gbk'高斯DB' COLLATE gbk_chinese_ci; -- ERROR
-- -- -- same charset & implicit collation
SELECT _utf8mb4'高斯DB' = '高斯DB';
-- -- -- diff charset & explicit collation
SELECT _utf8mb4'高斯db' COLLATE utf8mb4_unicode_ci = _gbk'高斯DB' COLLATE gbk_chinese_ci;
SELECT _utf8mb4'高斯db' COLLATE utf8mb4_unicode_ci = _gbk'高斯DB' COLLATE gbk_bin;
SELECT _utf8mb4'高斯db' COLLATE utf8mb4_unicode_ci = _gb18030'高斯DB' COLLATE gb18030_chinese_ci;
SELECT _utf8mb4'高斯db' COLLATE utf8mb4_unicode_ci = _gb18030'高斯DB' COLLATE gb18030_bin;
SELECT _utf8mb4'高斯db' COLLATE utf8mb4_unicode_ci = _binary'高斯DB' COLLATE 'binary'; -- not support yet
SELECT _utf8mb4'高斯DB' COLLATE utf8mb4_bin = _gbk'高斯db' COLLATE gbk_chinese_ci;
SELECT _utf8mb4'高斯DB' COLLATE utf8mb4_bin = _gbk'高斯DB' COLLATE gbk_bin;
SELECT _utf8mb4'高斯DB' COLLATE utf8mb4_bin = _gb18030'高斯db' COLLATE gb18030_chinese_ci;
SELECT _utf8mb4'高斯DB' COLLATE utf8mb4_bin = _gb18030'高斯DB' COLLATE gb18030_bin;
SELECT _gbk'高斯db' COLLATE gbk_chinese_ci = _gb18030'高斯DB' COLLATE gb18030_chinese_ci; -- ERROR
SELECT _gbk'高斯db' COLLATE gbk_chinese_ci = _gb18030'高斯DB' COLLATE gb18030_bin; -- ERROR
SELECT _gbk'高斯DB' COLLATE gbk_bin = _gb18030'高斯db' COLLATE gb18030_chinese_ci; -- ERROR
SELECT _gbk'高斯DB' COLLATE gbk_bin = _gb18030'高斯DB' COLLATE gb18030_bin; -- ERROR
-- -- -- diff charset & implicit collation
SELECT _utf8mb4'高斯DB' = _binary'高斯DB';
SELECT _utf8mb4'高斯DB' = _gbk'高斯DB';
SELECT _utf8mb4'高斯DB' = _gb18030'高斯DB';
SELECT _gbk'高斯DB' = '高斯DB';
SELECT _gbk'高斯DB' = _utf8mb4'高斯DB';
SELECT _gbk'高斯DB' = _binary'高斯DB'; -- not support yet
SELECT _gbk'高斯DB' = _gb18030'高斯DB'; -- ERROR
SELECT _gb18030'高斯DB' = '高斯DB';
SELECT _gb18030'高斯DB' = _utf8mb4'高斯DB';
SELECT _gb18030'高斯DB' = _binary'高斯DB'; -- not support yet
SELECT '高斯DB' = _utf8mb4'高斯DB';
SELECT '高斯DB' = _gbk'高斯DB';
SELECT '高斯DB' = _gb18030'高斯DB';
SELECT _binary'高斯DB' = _utf8mb4'高斯DB'; -- not support yet
SELECT _binary'高斯DB' = _gbk'高斯DB'; -- not support yet
SELECT _binary'高斯DB' = _gb18030'高斯DB'; -- not support yet
-- -- -- explicit & implicit
SELECT _utf8mb4'高斯DB' = '高斯DB' COLLATE utf8mb4_unicode_ci;
SELECT _utf8mb4'高斯DB' = '高斯DB' COLLATE utf8mb4_bin;
SELECT _utf8mb4'高斯DB' = '高斯DB' COLLATE "C";
SELECT _utf8mb4'高斯DB' = '高斯DB' COLLATE "zh_CN.utf8";
SELECT _utf8mb4'高斯DB' = _gbk'高斯DB' COLLATE gbk_chinese_ci;
SELECT _utf8mb4'楂樻柉DB' = _gbk'高斯db' COLLATE gbk_chinese_ci;
SELECT _utf8mb4'高斯DB' = _gbk'高斯DB' COLLATE gbk_bin;
SELECT _utf8mb4'高斯DB' = _gb18030'高斯DB' COLLATE gb18030_chinese_ci;
SELECT _utf8mb4'楂樻柉DB' = _gb18030'高斯db' COLLATE gb18030_chinese_ci;
SELECT _utf8mb4'高斯DB' = _gb18030'高斯DB' COLLATE gb18030_bin;
SELECT _utf8mb4'高斯db' COLLATE utf8mb4_unicode_ci = '高斯DB';
SELECT _utf8mb4'高斯db' COLLATE utf8mb4_unicode_ci = _binary'高斯DB';
SELECT _utf8mb4'高斯db' COLLATE utf8mb4_unicode_ci = _gbk'高斯DB';
SELECT _utf8mb4'高斯db' COLLATE utf8mb4_unicode_ci = _gb18030'高斯DB';
SELECT _utf8mb4'高斯DB' COLLATE utf8mb4_bin = '高斯DB';
SELECT _utf8mb4'高斯DB' COLLATE utf8mb4_bin = _binary'高斯DB';
SELECT _utf8mb4'高斯DB' COLLATE utf8mb4_bin = _gbk'高斯DB';
SELECT _utf8mb4'高斯DB' COLLATE utf8mb4_bin = _gb18030'高斯DB';
SELECT _gbk'高斯DB' = _gb18030'高斯db' COLLATE gb18030_chinese_ci;
SELECT _gbk'高斯DB' = _gb18030'高斯DB' COLLATE gb18030_bin;
SELECT _gbk'高斯db' COLLATE gbk_chinese_ci = '高斯DB';
SELECT _gbk'高斯db' COLLATE gbk_chinese_ci = _gb18030'高斯DB';
SELECT _gbk'高斯DB' COLLATE gbk_bin = '高斯DB';
SELECT _gbk'高斯DB' COLLATE gbk_bin = _gb18030'高斯DB';

-- -- 中文 const concat
-- -- -- same charset & explicit collation
SELECT CONCAT(_utf8mb4'高斯DB' COLLATE utf8mb4_unicode_ci , '高斯DB' COLLATE "C"); -- ERROR
SELECT CONCAT(_utf8mb4'高斯DB' COLLATE utf8mb4_unicode_ci , '高斯DB' COLLATE "zh_CN.utf8"); -- ERROR
SELECT CONCAT(_utf8mb4'高斯DB' COLLATE utf8mb4_unicode_ci , '高斯DB' COLLATE utf8mb4_unicode_ci);
SELECT CONCAT(_utf8mb4'高斯DB' COLLATE utf8mb4_unicode_ci , '高斯DB' COLLATE utf8mb4_bin); -- ERROR
SELECT CONCAT(_utf8mb4'高斯DB' COLLATE utf8mb4_bin , '高斯DB' COLLATE utf8mb4_unicode_ci); -- ERROR
SELECT CONCAT(_utf8mb4'高斯DB' COLLATE utf8mb4_bin , '高斯DB' COLLATE utf8mb4_bin);
-- -- -- same charset & implicit collation
SELECT CONCAT(_utf8mb4'高斯DB' , '高斯DB') result, collation for(result);
-- -- -- diff charset & explicit collation
SELECT CONCAT(_utf8mb4'高斯DB' COLLATE utf8mb4_unicode_ci , _gbk'高斯DB' COLLATE gbk_chinese_ci) result, collation for(result);
SELECT CONCAT(_utf8mb4'高斯DB' COLLATE utf8mb4_unicode_ci , _gbk'高斯DB' COLLATE gbk_bin) result, collation for(result);
SELECT CONCAT(_utf8mb4'高斯DB' COLLATE utf8mb4_unicode_ci , _gb18030'高斯DB' COLLATE gb18030_chinese_ci) result, collation for(result);
SELECT CONCAT(_utf8mb4'高斯DB' COLLATE utf8mb4_unicode_ci , _gb18030'高斯DB' COLLATE gb18030_bin) result, collation for(result);
SELECT CONCAT(_utf8mb4'高斯DB' COLLATE utf8mb4_bin , _gbk'高斯DB' COLLATE gbk_chinese_ci) result, collation for(result);
SELECT CONCAT(_utf8mb4'高斯DB' COLLATE utf8mb4_bin , _gbk'高斯DB' COLLATE gbk_bin) result, collation for(result);
SELECT CONCAT(_utf8mb4'高斯DB' COLLATE utf8mb4_bin , _gb18030'高斯DB' COLLATE gb18030_chinese_ci) result, collation for(result);
SELECT CONCAT(_utf8mb4'高斯DB' COLLATE utf8mb4_bin , _gb18030'高斯DB' COLLATE gb18030_bin) result, collation for(result);
SELECT CONCAT(_gbk'高斯DB' COLLATE gbk_chinese_ci , _gb18030'高斯DB' COLLATE gb18030_chinese_ci) result, collation for(result); -- ERROR
SELECT CONCAT(_gbk'高斯DB' COLLATE gbk_chinese_ci , _gb18030'高斯DB' COLLATE gb18030_bin) result, collation for(result); -- ERROR
SELECT CONCAT(_gbk'高斯DB' COLLATE gbk_bin , _gb18030'高斯DB' COLLATE gb18030_chinese_ci) result, collation for(result); -- ERROR
SELECT CONCAT(_gbk'高斯DB' COLLATE gbk_bin , _gb18030'高斯DB' COLLATE gb18030_bin) result, collation for(result); -- ERROR
-- -- -- diff charset & implicit collation
SELECT CONCAT(_utf8mb4'高斯DB' , _binary'高斯DB') result, collation for(result);
SELECT CONCAT(_utf8mb4'高斯DB' , _gbk'高斯DB') result, collation for(result);
SELECT CONCAT(_utf8mb4'高斯DB' , _gb18030'高斯DB') result, collation for(result);
SELECT CONCAT(_gbk'高斯DB' , '高斯DB') result, collation for(result);
SELECT CONCAT(_gbk'高斯DB' , _binary'高斯DB') result, collation for(result);
SELECT CONCAT(_gbk'高斯DB' , _gb18030'高斯DB') result, collation for(result); -- ERROR
SELECT CONCAT(_gb18030'高斯DB' , '高斯DB') result, collation for(result);
SELECT CONCAT(_gb18030'高斯DB' , _binary'高斯DB') result, collation for(result);
SELECT CONCAT( _binary'高斯DB', _utf8mb4'高斯DB') result, collation for(result);
SELECT CONCAT( _binary'高斯DB', '高斯DB') result, collation for(result);
-- -- -- explicit & implicit
SELECT CONCAT(_utf8mb4'高斯DB' , '高斯DB' COLLATE "C") result, collation for(result);
SELECT CONCAT(_utf8mb4'高斯DB' , '高斯DB' COLLATE "zh_CN.utf8") result, collation for(result);
SELECT CONCAT(_utf8mb4'高斯DB' , '高斯DB' COLLATE utf8mb4_unicode_ci) result, collation for(result);
SELECT CONCAT(_utf8mb4'高斯DB' , '高斯DB' COLLATE utf8mb4_bin) result, collation for(result);
SELECT CONCAT(_utf8mb4'高斯DB' , _gbk'高斯DB' COLLATE gbk_chinese_ci) result, collation for(result);
SELECT CONCAT(_utf8mb4'楂樻柉DB' , _gbk'高斯db' COLLATE gbk_chinese_ci) result, collation for(result);
SELECT CONCAT(_utf8mb4'高斯DB' , _gbk'高斯DB' COLLATE gbk_bin) result, collation for(result);
SELECT CONCAT(_utf8mb4'高斯DB' , _gb18030'高斯DB' COLLATE gb18030_chinese_ci) result, collation for(result);
SELECT CONCAT(_utf8mb4'楂樻柉DB' , _gb18030'高斯db' COLLATE gb18030_chinese_ci) result, collation for(result);
SELECT CONCAT(_utf8mb4'高斯DB' , _gb18030'高斯DB' COLLATE gb18030_bin) result, collation for(result);
SELECT CONCAT(_utf8mb4'高斯DB' COLLATE utf8mb4_unicode_ci , '高斯DB') result, collation for(result);
SELECT CONCAT(_utf8mb4'高斯DB' COLLATE utf8mb4_unicode_ci , _binary'高斯DB') result, collation for(result);
SELECT CONCAT(_utf8mb4'高斯DB' COLLATE utf8mb4_unicode_ci , _gbk'高斯DB') result, collation for(result);
SELECT CONCAT(_utf8mb4'高斯DB' COLLATE utf8mb4_unicode_ci , _gb18030'高斯DB') result, collation for(result);
SELECT CONCAT(_utf8mb4'高斯DB' COLLATE utf8mb4_bin , '高斯DB') result, collation for(result);
SELECT CONCAT(_utf8mb4'高斯DB' COLLATE utf8mb4_bin , _binary'高斯DB') result, collation for(result);
SELECT CONCAT(_utf8mb4'高斯DB' COLLATE utf8mb4_bin , _gbk'高斯DB') result, collation for(result);
SELECT CONCAT(_utf8mb4'高斯DB' COLLATE utf8mb4_bin , _gb18030'高斯DB') result, collation for(result);
SELECT CONCAT(_gbk'高斯DB' , _gb18030'高斯DB' COLLATE gb18030_chinese_ci) result, collation for(result);
SELECT CONCAT(_gbk'高斯DB' , _gb18030'高斯DB' COLLATE gb18030_bin) result, collation for(result);
SELECT CONCAT(_gbk'高斯DB' COLLATE gbk_chinese_ci , '高斯DB') result, collation for(result);
SELECT CONCAT(_gbk'高斯DB' COLLATE gbk_chinese_ci , _gb18030'高斯DB') result, collation for(result);
SELECT CONCAT(_gbk'高斯DB' COLLATE gbk_bin , '高斯DB') result, collation for(result);
SELECT CONCAT(_gbk'高斯DB' COLLATE gbk_bin , _gb18030'高斯DB') result, collation for(result);
SELECT CONCAT(_binary'高斯DB', _utf8mb4'高斯DB' COLLATE utf8mb4_unicode_ci) result, collation for(result);
SELECT CONCAT(_binary'高斯DB' COLLATE 'binary', _gbk'高斯DB') result, collation for(result);

-- -- -- concat 3 args
SELECT CONCAT(_binary'高斯DB', _gb18030'高斯DB', _gbk'高斯DB') result, collation for(result);
SELECT CONCAT(_binary'高斯DB', _gb18030'高斯DB', _gbk'高斯DB' COLLATE gbk_chinese_ci) result, collation for(result);
SELECT CONCAT(_utf8mb4'高斯DB', _gb18030'高斯DB', _gbk'高斯DB') result, collation for(result);
SELECT CONCAT(_utf8mb4'高斯DB', _gb18030'高斯DB', _gbk'高斯DB' COLLATE gbk_chinese_ci) result, collation for(result);
SELECT CONCAT(_utf8mb4'高斯DB', _gb18030'高斯DB', _binary'高斯DB') result, collation for(result);
SELECT CONCAT(_utf8mb4'高斯DB', _gb18030'高斯DB' COLLATE gb18030_chinese_ci, _binary'高斯DB') result, collation for(result);
SELECT CONCAT(_gb18030'高斯DB', _gbk'高斯DB', _utf8mb4'高斯DB') result, collation for(result); -- ERROR
SELECT CONCAT(_gb18030'高斯DB', _gbk'高斯DB', _binary'高斯DB') result, collation for(result); -- ERROR
SELECT CONCAT(_gb18030'高斯DB' COLLATE gb18030_chinese_ci, _gbk'高斯DB', _utf8mb4'高斯DB') result, collation for(result);
SELECT CONCAT(_gb18030'高斯DB', _gbk'高斯DB' COLLATE gbk_chinese_ci, _binary'高斯DB') result, collation for(result);
-- -- -- const compare CONCAT
SELECT _utf8mb4'楂樻柉DB' = CONCAT(_gbk'高斯DB');
SELECT _utf8mb4'楂樻柉DB楂樻柉DB' = CONCAT(_gbk'高斯DB', _gb18030'高斯DB' COLLATE gb18030_chinese_ci);
SELECT _utf8mb4'楂樻柉DB' COLLATE utf8mb4_bin = CONCAT(_gbk'高斯DB');
SELECT _utf8mb4'楂樻柉DB楂樻柉DB' COLLATE utf8mb4_bin = CONCAT(_gbk'高斯DB', _gb18030'高斯DB' COLLATE gb18030_chinese_ci);
SELECT _utf8mb4'楂樻柉DB楂樻柉DB' = CONCAT(_gbk'高斯DB', _gb18030'高斯DB'); -- ERROR
-- -- -- const CONCAT CONCAT
SELECT CONCAT(_utf8mb4'楂樻柉DB', CONCAT(_gbk'高斯DB')) result, collation for(result);
SELECT CONCAT(_utf8mb4'楂樻柉DB楂樻柉DB', CONCAT(_gbk'高斯DB', _gb18030'高斯DB' COLLATE gb18030_chinese_ci)) result, collation for(result);
SELECT CONCAT(_utf8mb4'楂樻柉DB' COLLATE utf8mb4_bin, CONCAT(_gbk'高斯DB')) result, collation for(result);
SELECT CONCAT(_utf8mb4'楂樻柉DB楂樻柉DB' COLLATE utf8mb4_bin, CONCAT(_gbk'高斯DB', _gb18030'高斯DB' COLLATE gb18030_chinese_ci)) result, collation for(result);
-- -- -- const CONCAT with other diff DERIVATION
-- -- -- -- same charset
SELECT CONCAT('高斯DB', opengauss_version()) result, collation for(result);
SELECT CONCAT(opengauss_version(), '高斯DB') result, collation for(result);
SELECT CONCAT('高斯DB', 123) result, collation for(result);
SELECT CONCAT(123, '高斯DB') result, collation for(result);
SELECT CONCAT('高斯DB', DATE '2023-05-01') result, collation for(result);
SELECT CONCAT(DATE '2023-05-01', '高斯DB') result, collation for(result);
SELECT CONCAT('高斯DB', NULL) result, collation for(result);
SELECT CONCAT(NULL, '高斯DB') result, collation for(result);
-- -- -- -- diff charset
SELECT CONCAT(_gbk'高斯DB', opengauss_version()) result, collation for(result);
SELECT CONCAT(opengauss_version(), _gbk'高斯DB') result, collation for(result);
SELECT CONCAT(_gbk'高斯DB', 123) result, collation for(result);
SELECT CONCAT(123, _gbk'高斯DB') result, collation for(result);
SELECT CONCAT(_gbk'高斯DB', DATE '2023-05-01') result, collation for(result);
SELECT CONCAT(DATE '2023-05-01', _gbk'高斯DB') result, collation for(result);
SELECT CONCAT(_gbk'高斯DB', NULL) result, collation for(result);
SELECT CONCAT(NULL, _gbk'高斯DB') result, collation for(result);
-- -- -- CONCAT CONCAT with other diff DERIVATION
-- -- -- -- same charset
SELECT CONCAT(CONCAT('高斯DB'), opengauss_version()) result, collation for(result);
SELECT CONCAT(opengauss_version(), CONCAT('高斯DB')) result, collation for(result);
SELECT CONCAT(CONCAT('高斯DB'), 123) result, collation for(result);
SELECT CONCAT(123, CONCAT('高斯DB')) result, collation for(result);
SELECT CONCAT(CONCAT('高斯DB'), DATE '2023-05-01') result, collation for(result);
SELECT CONCAT(DATE '2023-05-01', CONCAT('高斯DB')) result, collation for(result);
SELECT CONCAT(CONCAT('高斯DB'), NULL) result, collation for(result);
SELECT CONCAT(NULL, CONCAT('高斯DB')) result, collation for(result);
-- -- -- -- diff charset
SELECT CONCAT(CONCAT(_gbk'高斯DB'), opengauss_version()) result, collation for(result);
SELECT CONCAT(opengauss_version(), CONCAT(_gbk'高斯DB')) result, collation for(result);
SELECT CONCAT(CONCAT(_gbk'高斯DB'), 123) result, collation for(result);
SELECT CONCAT(123, CONCAT(_gbk'高斯DB')) result, collation for(result);
SELECT CONCAT(CONCAT(_gbk'高斯DB'), DATE '2023-05-01') result, collation for(result);
SELECT CONCAT(DATE '2023-05-01', CONCAT(_gbk'高斯DB')) result, collation for(result);
SELECT CONCAT(CONCAT(_gbk'高斯DB'), NULL) result, collation for(result);
SELECT CONCAT(NULL, CONCAT(_gbk'高斯DB')) result, collation for(result);
-- -- -- CONCAT NUMBERS
SELECT CONCAT('100', 200) result, collation for(result);
SELECT CONCAT('100', date'2021-01-01') result, collation for(result);
SELECT CONCAT('100', NULL) result, collation for(result);
SELECT CONCAT('100', NULL::bytea) result, collation for(result);
SELECT CONCAT('100', NULL::text) result, collation for(result);
SELECT CONCAT(100, 200) result, collation for(result);
SELECT CONCAT(100, date'2021-01-01') result, collation for(result);
SELECT CONCAT(100, NULL) result, collation for(result);
SELECT CONCAT(100, NULL::bytea) result, collation for(result);
SELECT CONCAT(100, NULL::text) result, collation for(result);
SELECT CONCAT(NULL, NULL::bytea) result, collation for(result);
SELECT CONCAT(NULL, NULL::text) result, collation for(result);
SELECT CONCAT(CONCAT(100, NULL), '100') result, collation for(result);
SELECT CONCAT(CONCAT(100, NULL::bytea), '100') result, collation for(result);
SELECT CONCAT(CONCAT(100, NULL::text), '100') result, collation for(result);
SELECT CONCAT(CONCAT(100, NULL), 100) result, collation for(result);
SELECT CONCAT(CONCAT(100, NULL::bytea), 100) result, collation for(result);
SELECT CONCAT(CONCAT(100, NULL::text), 100) result, collation for(result);

-- -- 中文 with column charset
CREATE TABLE t_diff_charset_columns(
    futf8_bin varchar(16) CHARSET utf8mb4 COLLATE 'utf8mb4_bin',
    futf8_uni varchar(16) CHARSET utf8mb4 COLLATE 'utf8mb4_unicode_ci',
    futf8_gen varchar(16) CHARSET utf8mb4 COLLATE 'utf8mb4_general_ci',
    fgbk_bin varchar(16) CHARSET gbk COLLATE 'gbk_bin',
    fgbk_chi varchar(16) CHARSET gbk COLLATE 'gbk_chinese_ci',
    fgb18030_bin varchar(16) CHARSET gb18030 COLLATE 'gb18030_bin',
    fgb18030_chi varchar(16) CHARSET gb18030 COLLATE 'gb18030_chinese_ci',
    fbytea bytea,
    fblob blob,
    fbit bit varying(64)
);
INSERT INTO t_diff_charset_columns(futf8_bin, futf8_uni, futf8_gen, fgbk_bin, fgbk_chi, fgb18030_bin, fgb18030_chi, fbytea, fblob, fbit)
    VALUES('高斯DB', '高斯db', '高斯db', '高斯DB', '高斯db', '高斯DB', '高斯db', '高斯DB', 'E9AB98E696AF', B'111010011010101110011000111001101001011010101111');
-- -- test character length
SELECT futf8_bin,
       futf8_uni,
       fgbk_bin,
       fgbk_chi,
       fgb18030_bin,
       fgb18030_chi,
       fbytea FROM t_diff_charset_columns;
SELECT length(futf8_bin),
       length(futf8_uni),
       length(fgbk_bin),
       length(fgbk_chi),
       length(fgb18030_bin),
       length(fgb18030_chi),
       length(fbytea) FROM t_diff_charset_columns;
SELECT lengthb(futf8_bin),
       lengthb(futf8_uni),
       lengthb(fgbk_bin),
       lengthb(fgbk_chi),
       lengthb(fgb18030_bin),
       lengthb(fgb18030_chi),
       length(fbytea) FROM t_diff_charset_columns;

-- test prefixkey index
CREATE INDEX idx_prefixkey_futf8_bin on t_diff_charset_columns (futf8_bin(2) text_pattern_ops);
set enable_seqscan=off;
EXPLAIN (costs false)
SELECT /*+ indexscan(t_diff_charset_columns idx_prefixkey_futf8_bin) */ futf8_bin FROM t_diff_charset_columns WHERE futf8_bin LIKE '高斯%';
SELECT /*+ indexscan(t_diff_charset_columns idx_prefixkey_futf8_bin) */ futf8_bin FROM t_diff_charset_columns WHERE futf8_bin LIKE '高斯%';
EXPLAIN (costs false)
SELECT /*+ indexscan(t_diff_charset_columns idx_prefixkey_futf8_bin) */ futf8_bin FROM t_diff_charset_columns WHERE futf8_bin = '高斯DB';
SELECT /*+ indexscan(t_diff_charset_columns idx_prefixkey_futf8_bin) */ futf8_bin FROM t_diff_charset_columns WHERE futf8_bin = '高斯DB';
CREATE INDEX idx_prefixkey_fgbk_bin on t_diff_charset_columns (fgbk_bin(2) text_pattern_ops);
EXPLAIN (costs false)
SELECT /*+ indexscan(t_diff_charset_columns idx_prefixkey_fgbk_bin) */ fgbk_bin FROM t_diff_charset_columns WHERE fgbk_bin LIKE '高斯%';
SELECT /*+ indexscan(t_diff_charset_columns idx_prefixkey_fgbk_bin) */ fgbk_bin FROM t_diff_charset_columns WHERE fgbk_bin LIKE '高斯%';
EXPLAIN (costs false)
SELECT /*+ indexscan(t_diff_charset_columns idx_prefixkey_fgbk_bin) */ fgbk_bin FROM t_diff_charset_columns WHERE fgbk_bin = '高斯DB';
SELECT /*+ indexscan(t_diff_charset_columns idx_prefixkey_fgbk_bin) */ fgbk_bin FROM t_diff_charset_columns WHERE fgbk_bin = '高斯DB';
CREATE INDEX idx_prefixkey_fgb18030_bin on t_diff_charset_columns (fgb18030_bin(2) text_pattern_ops);
EXPLAIN (costs false)
SELECT /*+ indexscan(t_diff_charset_columns idx_prefixkey_fgb18030_bin) */ fgb18030_bin FROM t_diff_charset_columns WHERE fgb18030_bin LIKE '高斯%';
SELECT /*+ indexscan(t_diff_charset_columns idx_prefixkey_fgb18030_bin) */ fgb18030_bin FROM t_diff_charset_columns WHERE fgb18030_bin LIKE '高斯%';
EXPLAIN (costs false)
SELECT /*+ indexscan(t_diff_charset_columns idx_prefixkey_fgb18030_bin) */ fgb18030_bin FROM t_diff_charset_columns WHERE fgb18030_bin = '高斯DB';
SELECT /*+ indexscan(t_diff_charset_columns idx_prefixkey_fgb18030_bin) */ fgb18030_bin FROM t_diff_charset_columns WHERE fgb18030_bin = '高斯DB';
reset enable_seqscan;

-- -- COLUMN collation only
SELECT futf8_bin COLLATE gbk_chinese_ci FROM t_diff_charset_columns; -- ERROR
SELECT futf8_bin COLLATE 'binary' FROM t_diff_charset_columns; -- ERROR
SELECT futf8_bin COLLATE "zh_CN.utf8" FROM t_diff_charset_columns; -- support origin collation
SELECT futf8_bin COLLATE "C" FROM t_diff_charset_columns; -- support origin collation
SELECT fgbk_bin COLLATE gbk_chinese_ci FROM t_diff_charset_columns;
SELECT fgbk_bin COLLATE 'binary' FROM t_diff_charset_columns; -- ERROR
SELECT fgbk_bin COLLATE "zh_CN.gbk" FROM t_diff_charset_columns; -- ERROR
SELECT fgbk_bin COLLATE "C" FROM t_diff_charset_columns; -- ERROR
SELECT fgb18030_bin COLLATE gb18030_chinese_ci FROM t_diff_charset_columns;
SELECT fgb18030_bin COLLATE gbk_chinese_ci FROM t_diff_charset_columns; -- ERROR
SELECT fgb18030_bin COLLATE 'binary' FROM t_diff_charset_columns; -- ERROR
SELECT fgb18030_bin COLLATE "zh_CN.gb18030" FROM t_diff_charset_columns; -- ERROR
SELECT fgb18030_bin COLLATE "C" FROM t_diff_charset_columns; -- ERROR
SELECT fbytea COLLATE gbk_chinese_ci FROM t_diff_charset_columns; -- ERROR
SELECT fbytea COLLATE 'binary' FROM t_diff_charset_columns;
SELECT fbytea COLLATE "zh_CN.utf8" FROM t_diff_charset_columns; -- ERROR
SELECT fbytea COLLATE "C" FROM t_diff_charset_columns; -- ERROR
SELECT fblob COLLATE gbk_chinese_ci FROM t_diff_charset_columns; -- ERROR
SELECT fblob COLLATE 'binary' FROM t_diff_charset_columns;
SELECT fblob COLLATE "zh_CN.utf8" FROM t_diff_charset_columns; -- ERROR
SELECT fblob COLLATE "C" FROM t_diff_charset_columns; -- ERROR
SELECT (futf8_uni COLLATE utf8mb4_bin) COLLATE gbk_chinese_ci FROM t_diff_charset_columns; -- ERROR
SELECT (futf8_uni COLLATE utf8mb4_bin) COLLATE 'binary' FROM t_diff_charset_columns; -- ERROR
SELECT (futf8_uni COLLATE utf8mb4_bin) COLLATE "zh_CN.utf8" FROM t_diff_charset_columns; -- support origin collation
SELECT (futf8_uni COLLATE utf8mb4_bin) COLLATE "C" FROM t_diff_charset_columns; -- support origin collation
SELECT (futf8_uni COLLATE utf8mb4_bin) COLLATE utf8mb4_unicode_ci FROM t_diff_charset_columns;
SELECT (fgbk_chi COLLATE "C") COLLATE utf8mb4_bin FROM t_diff_charset_columns; -- ERROR
SELECT (fgbk_chi COLLATE "C") COLLATE gbk_chinese_ci FROM t_diff_charset_columns; -- ERROR
SELECT (fblob COLLATE "binary") COLLATE utf8mb4_bin FROM t_diff_charset_columns; -- ERROR
SELECT fbit COLLATE 'binary' FROM t_diff_charset_columns; -- ERROR

-- -- COLUMN compare COLUMN
-- -- -- same charset & implicit collation
SELECT futf8_bin = futf8_uni, futf8_uni = futf8_bin FROM t_diff_charset_columns;
SELECT fgbk_bin = fgbk_chi, fgbk_chi = fgbk_bin FROM t_diff_charset_columns;
SELECT fgb18030_bin = fgb18030_chi, fgb18030_chi = fgb18030_bin FROM t_diff_charset_columns;
SELECT futf8_gen = futf8_uni, futf8_uni = futf8_gen FROM t_diff_charset_columns;
SELECT futf8_gen > futf8_uni, futf8_uni > futf8_gen FROM t_diff_charset_columns; -- ERROR
-- -- -- diff charset & implicit collation
SELECT futf8_bin = fgbk_bin, fgbk_bin = futf8_bin FROM t_diff_charset_columns;
SELECT futf8_bin = fgbk_chi, fgbk_chi = futf8_bin FROM t_diff_charset_columns;
SELECT futf8_bin = fgb18030_bin, fgb18030_bin = futf8_bin FROM t_diff_charset_columns;
SELECT futf8_bin = fgb18030_chi, fgb18030_chi = futf8_bin FROM t_diff_charset_columns;
SELECT futf8_uni = fgbk_bin, fgbk_bin = futf8_uni FROM t_diff_charset_columns;
SELECT futf8_uni = fgbk_chi, fgbk_chi = futf8_uni FROM t_diff_charset_columns;
SELECT futf8_uni = fgb18030_bin, fgb18030_bin = futf8_uni FROM t_diff_charset_columns;
SELECT futf8_uni = fgb18030_chi, fgb18030_chi = futf8_uni FROM t_diff_charset_columns;
SELECT fgbk_bin = fgb18030_bin, fgb18030_bin = fgbk_bin FROM t_diff_charset_columns; -- ERROR
SELECT fgbk_bin = fgb18030_chi, fgb18030_chi = fgbk_bin FROM t_diff_charset_columns; -- ERROR
SELECT fgbk_chi = fgb18030_chi, fgb18030_chi = fgbk_chi FROM t_diff_charset_columns; -- ERROR
-- -- -- with binary & implicit collation
SELECT futf8_bin = fbytea, fbytea = futf8_bin FROM t_diff_charset_columns; -- NOT SUPPORTED
SELECT futf8_bin = fblob, fblob = futf8_bin FROM t_diff_charset_columns; -- NOT SUPPORTED
SELECT futf8_uni = fbytea, fbytea = futf8_uni FROM t_diff_charset_columns; -- NOT SUPPORTED
SELECT futf8_uni = fblob, fblob = futf8_uni FROM t_diff_charset_columns; -- NOT SUPPORTED
SELECT fgbk_bin = fbytea, fbytea = fgbk_bin FROM t_diff_charset_columns; -- NOT SUPPORTED
SELECT fgbk_bin = fblob, fblob = fgbk_bin FROM t_diff_charset_columns; -- NOT SUPPORTED
SELECT fgb18030_bin = fbytea, fbytea = fgb18030_bin FROM t_diff_charset_columns; -- NOT SUPPORTED
SELECT fgb18030_bin = fblob, fblob = fgb18030_bin FROM t_diff_charset_columns; -- NOT SUPPORTED
SELECT fbytea = fblob, fblob = fbytea FROM t_diff_charset_columns;

-- -- COLUMN concat COLUMN
-- -- -- same charset & implicit collation
SELECT CONCAT(futf8_bin, futf8_uni) result, collation for(result) FROM t_diff_charset_columns;
SELECT CONCAT(fgbk_bin, fgbk_chi) result, collation for(result) FROM t_diff_charset_columns;
SELECT CONCAT(fgb18030_bin, fgb18030_chi) result, collation for(result) FROM t_diff_charset_columns;
SELECT CONCAT(futf8_gen, futf8_uni) result, collation for(result) FROM t_diff_charset_columns; -- result is _bin
-- -- -- diff charset & implicit collation
SELECT CONCAT(futf8_bin, fgbk_bin) result, collation for(result) FROM t_diff_charset_columns;
SELECT CONCAT(futf8_bin, fgbk_chi) result, collation for(result) FROM t_diff_charset_columns;
SELECT CONCAT(futf8_bin, fgb18030_bin) result, collation for(result) FROM t_diff_charset_columns;
SELECT CONCAT(futf8_bin, fgb18030_chi) result, collation for(result) FROM t_diff_charset_columns;
SELECT CONCAT(futf8_uni, fgbk_bin) result, collation for(result) FROM t_diff_charset_columns;
SELECT CONCAT(futf8_uni, fgbk_chi) result, collation for(result) FROM t_diff_charset_columns;
SELECT CONCAT(futf8_uni, fgb18030_bin) result, collation for(result) FROM t_diff_charset_columns;
SELECT CONCAT(futf8_uni, fgb18030_chi) result, collation for(result) FROM t_diff_charset_columns;
SELECT CONCAT(fgbk_bin, fgb18030_bin) result, collation for(result) FROM t_diff_charset_columns; -- ERROR
SELECT CONCAT(fgbk_bin, fgb18030_chi) result, collation for(result) FROM t_diff_charset_columns; -- ERROR
SELECT CONCAT(fgbk_chi, fgb18030_chi) result, collation for(result) FROM t_diff_charset_columns; -- ERROR
-- -- -- with binary & implicit collation
SELECT CONCAT(futf8_bin, fbytea) result, collation for(result) FROM t_diff_charset_columns; -- NOT SUPPORTED
SELECT CONCAT(futf8_bin, fblob) result, collation for(result) FROM t_diff_charset_columns; -- NOT SUPPORTED
SELECT CONCAT(futf8_uni, fbytea) result, collation for(result) FROM t_diff_charset_columns; -- NOT SUPPORTED
SELECT CONCAT(futf8_uni, fblob) result, collation for(result) FROM t_diff_charset_columns; -- NOT SUPPORTED
SELECT CONCAT(fgbk_bin, fbytea) result, collation for(result) FROM t_diff_charset_columns; -- NOT SUPPORTED
SELECT CONCAT(fgbk_bin, fblob) result, collation for(result) FROM t_diff_charset_columns; -- NOT SUPPORTED
SELECT CONCAT(fgb18030_bin, fbytea) result, collation for(result) FROM t_diff_charset_columns; -- NOT SUPPORTED
SELECT CONCAT(fgb18030_bin, fblob) result, collation for(result) FROM t_diff_charset_columns; -- NOT SUPPORTED
SELECT CONCAT(fbytea, fblob) result, collation for(result) FROM t_diff_charset_columns;

-- -- concat column and @uservar
set enable_set_variable_b_format=on;
-- -- -- string var utf8mb4_general_ci
set @var_utf8_uni = '高斯DB' COLLATE utf8mb4_unicode_ci; -- should support
SELECT collation for(@var_utf8_uni);
SELECT CONCAT(futf8_gen, @var_utf8_uni) result, collation for(result) FROM t_diff_charset_columns; -- null collation
SELECT CONCAT(fgbk_bin, @var_utf8_uni) result, collation for(result) FROM t_diff_charset_columns; -- utf8mb4_unicode_ci
SELECT CONCAT(@var_utf8_uni, futf8_bin) result, collation for(result) FROM t_diff_charset_columns; -- _bin
-- -- -- string var gbk_chinese_ci
set @var_gbk_chi = _gbk'高斯DB' COLLATE gbk_chinese_ci; -- should support
SELECT collation for(@var_gbk_chi);
SELECT CONCAT(futf8_uni, @var_gbk_chi) result, collation for(result) FROM t_diff_charset_columns; -- futf8_uni
SELECT CONCAT(@var_gbk_chi, fgbk_bin) result, collation for(result) FROM t_diff_charset_columns; -- fgbk_bin
-- -- -- number var
set @var_num = 5.0;
SELECT CONCAT(futf8_bin, @var_num) result, collation for(result) FROM t_diff_charset_columns; -- futf8_bin
SELECT CONCAT(@var_num, fgbk_bin) result, collation for(result) FROM t_diff_charset_columns; -- fgbk_bin
-- -- -- bytea var
set @var_binary = _binary'高斯DB'; -- not support yet
SELECT CONCAT(futf8_bin, @var_binary) result, collation for(result) FROM t_diff_charset_columns;
SELECT CONCAT(@var_binary, fgbk_bin) result, collation for(result) FROM t_diff_charset_columns;

-- -- concat column and bind parameter
-- -- bind parameter collation is fixed as collation_connection, collation level is same as a const
-- -- -- -- PBE with implicit collation
PREPARE test_merge_collation(text) AS
SELECT CONCAT(futf8_uni, $1) result, collation for(result) FROM t_diff_charset_columns;
-- -- -- -- -- _utf8mb4
SET @pbe_param1 = _utf8mb4'高斯DB';
EXECUTE test_merge_collation(@pbe_param1); -- futf8_uni collation has priority
EXECUTE test_merge_collation(_utf8mb4'高斯DB'); -- same as above
SELECT CONCAT(futf8_uni, _utf8mb4'高斯DB') result, collation for(result) FROM t_diff_charset_columns; -- same as above
-- -- -- -- -- _gbk
SET @pbe_param1 = _gbk'高斯DB';
EXECUTE test_merge_collation(@pbe_param1); -- _gbk noneffective, futf8_uni collation has priority,  _gbk'高斯DB' will not convert to utf8mb4
EXECUTE test_merge_collation(_gbk'高斯DB'); -- same as above
SELECT CONCAT(futf8_uni, _gbk'高斯DB') result, collation for(result) FROM t_diff_charset_columns; -- same as above
-- -- -- -- -- _utf8mb4 utf8mb4_unicode_ci
SET @pbe_param1 = _utf8mb4'高斯DB' collate utf8mb4_bin;
EXECUTE test_merge_collation(@pbe_param1); -- explicit noneffective, futf8_uni collation has priority
EXECUTE test_merge_collation(_utf8mb4'高斯DB' collate utf8mb4_unicode_ci); -- explicit noneffective, futf8_uni collation has priority
DEALLOCATE test_merge_collation;
-- -- -- -- PBE with explicit collation,
PREPARE test_merge_collation(text) AS
SELECT CONCAT($1 collate utf8mb4_unicode_ci, futf8_bin) result, collation for(result) FROM t_diff_charset_columns;
-- -- -- -- -- _utf8mb4
SET @pbe_param1 = _utf8mb4'高斯DB';
EXECUTE test_merge_collation(@pbe_param1);
EXECUTE test_merge_collation(_utf8mb4'高斯DB'); -- utf8mb4_unicode_ci
-- -- -- -- -- _gbk
SET @pbe_param1 = _gbk'高斯DB';
EXECUTE test_merge_collation(@pbe_param1);
EXECUTE test_merge_collation(_gbk'高斯DB'); -- utf8mb4_unicode_ci
DEALLOCATE test_merge_collation;
-- -- -- -- PBE with explicit collation,
PREPARE test_merge_collation(text) AS
SELECT CONCAT($1 collate gbk_chinese_ci, futf8_bin) result, collation for(result) FROM t_diff_charset_columns; -- $1 use collation_connection, ERROR
-- -- -- -- test revalidate
SELECT fgbk_chi result FROM t_diff_charset_columns WHERE fgbk_chi=_utf8mb4'高斯db'; -- 1 rows
PREPARE test_revalidate(text) AS
SELECT fgbk_chi result FROM t_diff_charset_columns WHERE fgbk_chi=$1;
EXECUTE test_revalidate(_utf8mb4'高斯db'); -- fgbk_chi collation has priority, 1 rows
ALTER INDEX idx_prefixkey_futf8_bin UNUSABLE;
EXECUTE test_revalidate(_utf8mb4'高斯db'); -- fgbk_chi collation has priority, 1 rows
SET NAMES utf8mb4 COLLATE utf8mb4_bin;
EXECUTE test_revalidate(_utf8mb4'高斯db'); -- fgbk_chi collation has priority, 1 rows
ALTER INDEX idx_prefixkey_futf8_bin REBUILD;
EXECUTE test_revalidate(_utf8mb4'高斯db'); -- fgbk_chi collation has priority, 1 rows
SET NAMES gbk COLLATE gbk_bin;
EXECUTE test_revalidate(_utf8mb4'高斯db'); -- fgbk_chi collation has priority, 1 rows
ALTER INDEX idx_prefixkey_futf8_bin REBUILD;
EXECUTE test_revalidate(_utf8mb4'高斯db'); -- fgbk_chi collation has priority, 1 rows
DEALLOCATE test_revalidate;
SET NAMES utf8mb4;

-- -- concat column and PROCEDURE parameter with CURSOR
-- -- -- implicit collation && string
create or replace procedure merge_collation_func(p1 text, p2 text)
as
DECLARE
    concat_res text;
    collation_res text;
    concat_res_assign varchar(64);
    CURSOR c1 IS
        SELECT CONCAT(p1, p2) result, collation for(result);
begin
    OPEN c1;
    LOOP
        FETCH c1 INTO concat_res, collation_res;
        EXIT;
    END LOOP;
    CLOSE c1;
    concat_res_assign = concat_res;
    set @concat_res = concat_res;
    set @collation_res = collation_res;
    set @concat_res_assign = concat_res_assign;
end;
/

CALL merge_collation_func('高斯DB', _gbk'高斯DB');
SELECT @concat_res,@collation_res,collation for(@concat_res) real_collation,@concat_res_assign; -- utf8mb4_general_ci
CALL merge_collation_func(_gb18030'高斯DB', _gbk'高斯DB');
SELECT @concat_res,@collation_res,collation for(@concat_res) real_collation,@concat_res_assign; -- utf8mb4_general_ci
CALL merge_collation_func(_gb18030'高斯DB', _gbk'高斯DB' collate gbk_chinese_ci);
SELECT @concat_res,@collation_res,collation for(@concat_res) real_collation,@concat_res_assign; -- utf8mb4_general_ci
DROP procedure merge_collation_func;
-- -- -- implicit collation && string
create or replace procedure merge_collation_func(p1 text)
as
DECLARE
    concat_res text;
    collation_res text;
    CURSOR c1 IS
        SELECT CONCAT(futf8_uni, p1) result, collation for(result) FROM t_diff_charset_columns;
begin
    OPEN c1;
    LOOP
        FETCH c1 INTO concat_res, collation_res;
        EXIT;
    END LOOP;
    CLOSE c1;
    set @concat_res = concat_res;
    set @collation_res = collation_res;
end;
/

CALL merge_collation_func('高斯DB');
SELECT @concat_res,@collation_res,collation for(@concat_res) real_collation; -- conflict
CALL merge_collation_func('高斯DB' collate utf8mb4_general_ci);
SELECT @concat_res,@collation_res,collation for(@concat_res) real_collation; -- conflict
CALL merge_collation_func(_gbk'高斯DB');
SELECT @concat_res,@collation_res,collation for(@concat_res) real_collation;; -- conflict
CALL merge_collation_func(_gbk'高斯DB' collate gbk_chinese_ci);
SELECT @concat_res,@collation_res,collation for(@concat_res) real_collation;; -- conflict
DROP procedure merge_collation_func;
-- -- -- implicit collation && string
create or replace procedure merge_collation_func(p1 text)
as
DECLARE
    concat_res text;
    collation_res text;
    CURSOR c1 IS
        SELECT CONCAT(p1, fgbk_bin) result, collation for(result) FROM t_diff_charset_columns;
begin
    OPEN c1;
    LOOP
        FETCH c1 INTO concat_res, collation_res;
        EXIT;
    END LOOP;
    CLOSE c1;
    set @concat_res = concat_res;
    set @collation_res = collation_res;
end;
/


CALL merge_collation_func('高斯DB');
SELECT @concat_res,@collation_res,collation for(@concat_res) real_collation; -- utf8mb4_general_ci
CALL merge_collation_func('高斯DB' collate utf8mb4_general_ci);
SELECT @concat_res,@collation_res,collation for(@concat_res) real_collation; -- utf8mb4_general_ci
CALL merge_collation_func(_gbk'高斯DB');
SELECT @concat_res,@collation_res,collation for(@concat_res) real_collation; -- utf8mb4_general_ci
CALL merge_collation_func(_gbk'高斯DB' collate gbk_chinese_ci);
SELECT @concat_res,@collation_res,collation for(@concat_res) real_collation; -- utf8mb4_general_ci
DROP procedure merge_collation_func;
-- -- -- explicit collation && string
create or replace procedure merge_collation_func(p1 text)
as
DECLARE
    concat_res text;
    collation_res text;
    CURSOR c1 IS
        SELECT CONCAT(p1, fgbk_bin collate gbk_bin) result, collation for(result) FROM t_diff_charset_columns;
begin
    OPEN c1;
    LOOP
        FETCH c1 INTO concat_res, collation_res;
        EXIT;
    END LOOP;
    CLOSE c1;
    set @concat_res = concat_res;
    set @collation_res = collation_res;
end;
/


CALL merge_collation_func('高斯DB');
SELECT @concat_res,@collation_res,collation for(@concat_res) real_collation; -- gbk_bin
CALL merge_collation_func('高斯DB' collate utf8mb4_general_ci);
SELECT @concat_res,@collation_res,collation for(@concat_res) real_collation; -- gbk_bin
CALL merge_collation_func(_gbk'高斯DB');
SELECT @concat_res,@collation_res,collation for(@concat_res) real_collation; -- gbk_bin
CALL merge_collation_func(_gbk'高斯DB' collate gbk_chinese_ci);
SELECT @concat_res,@collation_res,collation for(@concat_res) real_collation; -- gbk_bin
DROP procedure merge_collation_func;
-- -- -- implicit collation && int
create or replace procedure merge_collation_func(p1 int)
as
DECLARE
    concat_res text;
    collation_res text;
    CURSOR c1 IS
        SELECT CONCAT(fgbk_bin, p1) result, collation for(result) FROM t_diff_charset_columns;
begin
    OPEN c1;
    LOOP
        FETCH c1 INTO concat_res, collation_res;
        EXIT;
    END LOOP;
    CLOSE c1;
    set @concat_res = concat_res;
    set @collation_res = collation_res;
end;
/

CALL merge_collation_func(100);
SELECT @concat_res,@collation_res,collation for(@concat_res) real_collation; -- gbk_bin
DROP procedure merge_collation_func;
-- -- -- explicit collation && string
create or replace procedure merge_collation_func(p1 text)
as
DECLARE
    concat_res text;
    collation_res text;
    CURSOR c1 IS
        SELECT CONCAT(futf8_uni, p1 COLLATE gbk_chinese_ci) result, collation for(result) FROM t_diff_charset_columns;
begin
    OPEN c1;
    LOOP
        FETCH c1 INTO concat_res, collation_res;
        EXIT;
    END LOOP;
    CLOSE c1;
    set @concat_res = concat_res;
    set @collation_res = collation_res;
end;
/

CALL merge_collation_func('高斯DB'); -- ERROR
DROP procedure merge_collation_func;

-- -- concat column and PROCEDURE parameter with select into
-- -- -- implicit collation
create or replace procedure merge_collation_func(p1 text)
as
begin
    SELECT CONCAT(futf8_uni, p1) result, collation for(result) FROM t_diff_charset_columns into @concat_res,@collation_res;
end;
/

CALL merge_collation_func('高斯DB');
SELECT @concat_res,@collation_res,collation for(@concat_res) real_collation; -- conflict
CALL merge_collation_func('高斯DB' collate utf8mb4_general_ci);
SELECT @concat_res,@collation_res,collation for(@concat_res) real_collation; -- conflict
CALL merge_collation_func(_gbk'高斯DB');
SELECT @concat_res,@collation_res,collation for(@concat_res) real_collation; -- utf8mb4_unicode_ci
CALL merge_collation_func(_gbk'高斯DB' collate gbk_chinese_ci);
SELECT @concat_res,@collation_res,collation for(@concat_res) real_collation; -- utf8mb4_unicode_ci
DROP procedure merge_collation_func;
-- -- -- implicit collation
create or replace procedure merge_collation_func(p1 text)
as
begin
    SELECT CONCAT(p1, fgbk_bin) result, collation for(result) FROM t_diff_charset_columns into @concat_res,@collation_res;
end;
/

CALL merge_collation_func('高斯DB');
SELECT @concat_res,@collation_res,collation for(@concat_res) real_collation; -- gbk_bin
CALL merge_collation_func('高斯DB' collate utf8mb4_general_ci);
SELECT @concat_res,@collation_res,collation for(@concat_res) real_collation; -- gbk_bin
CALL merge_collation_func(_gbk'高斯DB');
SELECT @concat_res,@collation_res,collation for(@concat_res) real_collation; -- gbk_bin
CALL merge_collation_func(_gbk'高斯DB' collate gbk_chinese_ci);
SELECT @concat_res,@collation_res,collation for(@concat_res) real_collation; -- gbk_bin
DROP procedure merge_collation_func;
-- -- -- explicit collation
create or replace procedure merge_collation_func(p1 text)
as
begin
    SELECT CONCAT(futf8_uni, p1 COLLATE gbk_chinese_ci) result, collation for(result) FROM t_diff_charset_columns into @concat_res,@collation_res;
end;
/

CALL merge_collation_func('高斯DB'); -- ERROR
DROP procedure merge_collation_func;

-- -- concat for DERIVATION
-- -- -- same charset & diff DERIVATION
SELECT CONCAT(CONCAT(futf8_gen, futf8_uni), futf8_bin) result, collation for(result) FROM t_diff_charset_columns; -- conflict in concat inside
SELECT CONCAT(futf8_bin, CONCAT(futf8_gen, futf8_uni)) result, collation for(result) FROM t_diff_charset_columns; -- conflict in concat inside
SELECT CONCAT(CONCAT(futf8_gen, futf8_uni), futf8_bin collate utf8mb4_unicode_ci) result, collation for(result) FROM t_diff_charset_columns; -- utf8mb4_unicode_ci
SELECT CONCAT(futf8_bin collate utf8mb4_unicode_ci, CONCAT(futf8_gen, futf8_uni)) result, collation for(result) FROM t_diff_charset_columns; -- utf8mb4_unicode_ci
SELECT CONCAT(CONCAT(futf8_gen), futf8_uni) result, collation for(result) FROM t_diff_charset_columns; -- conflict
SELECT CONCAT(futf8_uni, CONCAT(futf8_gen)) result, collation for(result) FROM t_diff_charset_columns; -- conflict
SELECT CONCAT(futf8_uni, opengauss_version()) result, collation for(result) FROM t_diff_charset_columns;
SELECT CONCAT(opengauss_version(), futf8_uni) result, collation for(result) FROM t_diff_charset_columns;
SELECT CONCAT(futf8_uni, '高斯DB') result, collation for(result) FROM t_diff_charset_columns;
SELECT CONCAT('高斯DB', futf8_uni) result, collation for(result) FROM t_diff_charset_columns;
SELECT CONCAT(futf8_uni, 123) result, collation for(result) FROM t_diff_charset_columns;
SELECT CONCAT(123, futf8_uni) result, collation for(result) FROM t_diff_charset_columns;
SELECT CONCAT(futf8_uni, DATE '2023-05-01') result, collation for(result) FROM t_diff_charset_columns;
SELECT CONCAT(DATE '2023-05-01', futf8_uni) result, collation for(result) FROM t_diff_charset_columns;
SELECT CONCAT(futf8_uni, NULL) result, collation for(result) FROM t_diff_charset_columns;
SELECT CONCAT(NULL, futf8_uni) result, collation for(result) FROM t_diff_charset_columns;
-- -- -- diff charset & diff DERIVATION
SELECT CONCAT(CONCAT(futf8_gen, futf8_uni), CONCAT(fgbk_chi, fgb18030_chi)) result, collation for(result) FROM t_diff_charset_columns; -- conflict in concat inside
SELECT CONCAT(CONCAT(fgbk_chi, fgb18030_chi), CONCAT(futf8_gen, futf8_uni)) result, collation for(result) FROM t_diff_charset_columns; -- conflict in concat inside
SELECT CONCAT(CONCAT(futf8_gen, futf8_uni), fgbk_chi) result, collation for(result) FROM t_diff_charset_columns; -- conflict in concat inside
SELECT CONCAT(fgbk_chi, CONCAT(futf8_gen, futf8_uni)) result, collation for(result) FROM t_diff_charset_columns; -- conflict in concat inside
SELECT CONCAT(CONCAT(futf8_gen, futf8_uni), fgbk_chi COLLATE gbk_chinese_ci) result, collation for(result) FROM t_diff_charset_columns; -- gbk_chinese_ci
SELECT CONCAT(fgbk_chi COLLATE gbk_chinese_ci, CONCAT(futf8_gen, futf8_uni)) result, collation for(result) FROM t_diff_charset_columns; -- gbk_chinese_ci
SELECT CONCAT(CONCAT(futf8_gen, fgbk_chi), futf8_uni) result, collation for(result) FROM t_diff_charset_columns; -- conflict
SELECT CONCAT(futf8_uni, CONCAT(futf8_gen, fgbk_chi)) result, collation for(result) FROM t_diff_charset_columns; -- conflict
SELECT CONCAT(fgbk_chi, CONCAT(fgb18030_chi)) result, collation for(result) FROM t_diff_charset_columns;
SELECT CONCAT(CONCAT(fgb18030_bin), fgbk_bin) result, collation for(result) FROM t_diff_charset_columns;
SELECT CONCAT(fgbk_chi, opengauss_version()) result, collation for(result) FROM t_diff_charset_columns;
SELECT CONCAT(opengauss_version(), fgbk_bin) result, collation for(result) FROM t_diff_charset_columns;
SELECT CONCAT(fgbk_chi, '高斯DB') result, collation for(result) FROM t_diff_charset_columns;
SELECT CONCAT('高斯DB', fgbk_bin) result, collation for(result) FROM t_diff_charset_columns;
SELECT CONCAT(fgbk_chi, 123) result, collation for(result) FROM t_diff_charset_columns;
SELECT CONCAT(123, fgbk_bin) result, collation for(result) FROM t_diff_charset_columns;
SELECT CONCAT(fgbk_chi, DATE '2023-05-01') result, collation for(result) FROM t_diff_charset_columns;
SELECT CONCAT(DATE '2023-05-01', fgbk_bin) result, collation for(result) FROM t_diff_charset_columns;
SELECT CONCAT(fgbk_chi, NULL) result, collation for(result) FROM t_diff_charset_columns;
SELECT CONCAT(NULL, fgbk_bin) result, collation for(result) FROM t_diff_charset_columns;

-- -- test explicit collate on concat
-- -- -- same charset & implicit collation
SELECT CONCAT(futf8_bin, futf8_uni) COLLATE utf8mb4_general_ci result, collation for(result) FROM t_diff_charset_columns;
SELECT CONCAT(futf8_bin, futf8_uni) COLLATE gbk_chinese_ci result, collation for(result) FROM t_diff_charset_columns; -- ERROR
-- -- -- diff charset & implicit collation
SELECT CONCAT(futf8_bin, fgbk_bin) COLLATE utf8mb4_general_ci result, collation for(result) FROM t_diff_charset_columns;
SELECT CONCAT(futf8_bin, fgbk_bin) COLLATE gbk_chinese_ci result, collation for(result) FROM t_diff_charset_columns; -- ERROR
-- -- -- with binary & implicit collation
SELECT CONCAT(fbytea, futf8_uni) COLLATE "binary" result, collation for(result) FROM t_diff_charset_columns; -- return datatype still text
SELECT CONCAT(fbytea, futf8_uni) COLLATE utf8mb4_general_ci result, collation for(result) FROM t_diff_charset_columns; -- return datatype still text

-- -- test explicit collate on blob result
SELECT CAST('DEADBEEF' AS blob) COLLATE utf8mb4_general_ci result; -- ERROR
SELECT CAST('DEADBEEF' AS blob) COLLATE "binary" result;

-- -- case when
-- -- -- condition same charset & result same charset & implicit collation
-- -- -- -- bool condition
SELECT CASE WHEN (fgbk_chi = fgbk_bin) THEN (futf8_bin) ELSE (futf8_uni) END result, collation for(result) FROM t_diff_charset_columns;
SELECT CASE WHEN (futf8_bin = futf8_uni) THEN (fgbk_chi) ELSE (fgbk_bin) END result, collation for(result) FROM t_diff_charset_columns;
SELECT CASE WHEN (fgbk_chi = fgbk_bin) THEN (futf8_gen) ELSE (futf8_uni) END result, collation for(result) FROM t_diff_charset_columns; -- null collation
SELECT CASE WHEN (futf8_gen = futf8_uni) THEN (fgbk_chi) ELSE (fgbk_bin) END result, collation for(result) FROM t_diff_charset_columns; -- ERROR
-- -- -- -- case condition
SELECT CASE fgbk_chi WHEN fgbk_bin THEN (futf8_bin) ELSE (futf8_uni) END result, collation for(result) FROM t_diff_charset_columns;
SELECT CASE futf8_bin WHEN futf8_uni THEN (fgbk_chi) ELSE (fgbk_bin) END result, collation for(result) FROM t_diff_charset_columns;
SELECT CASE fgbk_chi WHEN fgbk_bin THEN (futf8_gen) ELSE (futf8_uni) END result, collation for(result) FROM t_diff_charset_columns; -- null collation
SELECT CASE futf8_gen WHEN futf8_uni THEN (fgbk_chi) ELSE (fgbk_bin) END result, collation for(result) FROM t_diff_charset_columns; -- ERROR
-- -- -- condition same charset & result diff charset & implicit collation
-- -- -- -- bool condition
SELECT CASE WHEN (fgbk_chi = fgbk_bin) THEN (fgbk_bin) ELSE (futf8_uni) END result, collation for(result) FROM t_diff_charset_columns;
SELECT CASE WHEN (futf8_bin = futf8_uni) THEN (futf8_uni) ELSE (fgbk_bin) END result, collation for(result) FROM t_diff_charset_columns;
SELECT CASE WHEN (futf8_bin = futf8_uni) THEN (fgb18030_bin) ELSE (fgbk_bin) END result, collation for(result) FROM t_diff_charset_columns; -- ERROR
-- -- -- -- case condition
SELECT CASE fgbk_chi WHEN fgbk_bin THEN (futf8_bin) ELSE (futf8_uni) END result, collation for(result) FROM t_diff_charset_columns;
SELECT CASE futf8_bin WHEN futf8_uni THEN (futf8_uni) ELSE (fgbk_bin) END result, collation for(result) FROM t_diff_charset_columns;
-- -- -- condition diff charset & result same charset & implicit collation
-- -- -- -- bool condition
SELECT CASE WHEN (futf8_uni = fgbk_bin) THEN (futf8_bin) ELSE (futf8_uni) END result, collation for(result) FROM t_diff_charset_columns;
SELECT CASE WHEN (fgbk_bin = futf8_uni) THEN (fgbk_chi) ELSE (fgbk_bin) END result, collation for(result) FROM t_diff_charset_columns;
SELECT CASE WHEN (fgbk_bin = fgb18030_bin) THEN (fgbk_chi) ELSE (fgbk_bin) END result, collation for(result) FROM t_diff_charset_columns; -- ERROR
-- -- -- -- case condition
SELECT CASE futf8_uni WHEN fgbk_bin THEN (futf8_bin) ELSE (futf8_uni) END result, collation for(result) FROM t_diff_charset_columns;
SELECT CASE fgbk_bin WHEN futf8_uni THEN (fgbk_chi) ELSE (fgbk_bin) END result, collation for(result) FROM t_diff_charset_columns;
SELECT CASE fgbk_bin WHEN fgb18030_bin THEN (fgbk_chi) ELSE (fgbk_bin) END result, collation for(result) FROM t_diff_charset_columns; -- ERROR
-- -- -- condition diff charset & result diff charset & implicit collation
-- -- -- -- bool condition
SELECT CASE WHEN (futf8_uni = fgbk_bin) THEN (fgbk_bin) ELSE (futf8_bin) END result, collation for(result) FROM t_diff_charset_columns;
SELECT CASE WHEN (fgbk_bin = futf8_uni) THEN (futf8_bin) ELSE (fgbk_bin) END result, collation for(result) FROM t_diff_charset_columns;
SELECT CASE WHEN (fgbk_bin = fgb18030_bin) THEN (fgb18030_chi) ELSE (fgbk_chi) END result, collation for(result) FROM t_diff_charset_columns; -- ERROR
-- -- -- -- case condition
SELECT CASE futf8_uni WHEN fgbk_bin THEN (fgbk_bin) ELSE (futf8_bin) END result, collation for(result) FROM t_diff_charset_columns;
SELECT CASE fgbk_bin WHEN futf8_uni THEN (futf8_bin) ELSE (fgbk_bin) END result, collation for(result) FROM t_diff_charset_columns;
SELECT CASE fgb18030_chi WHEN fgbk_chi THEN (fgb18030_bin) ELSE (fgbk_bin) END result, collation for(result) FROM t_diff_charset_columns; -- ERROR
-- -- -- -- conflict
SELECT CASE _gb18030'高斯' WHEN fgbk_bin THEN (fgbk_bin) ELSE (futf8_bin) END result, collation for(result) FROM t_diff_charset_columns; -- ERROR
-- -- in
-- -- -- column utf8
EXPLAIN (costs off)
SELECT futf8_bin FROM t_diff_charset_columns WHERE futf8_bin in (_gb18030'高斯DB', _gbk'高斯DB'); -- ERROR
SELECT futf8_bin FROM t_diff_charset_columns WHERE futf8_bin in (_gb18030'高斯DB', _gbk'高斯DB'); -- ERROR
EXPLAIN (costs off)
SELECT futf8_bin FROM t_diff_charset_columns WHERE futf8_bin in (_gb18030'高斯DB' COLLATE gb18030_chinese_ci, '高斯DB');
SELECT futf8_bin FROM t_diff_charset_columns WHERE futf8_bin in (_gb18030'高斯DB' COLLATE gb18030_chinese_ci, '高斯DB');
EXPLAIN (costs off)
SELECT futf8_bin FROM t_diff_charset_columns WHERE futf8_bin in (_gb18030'高斯DB', concat('高斯DB'));
SELECT futf8_bin FROM t_diff_charset_columns WHERE futf8_bin in (_gb18030'高斯DB', concat('高斯DB'));
SELECT futf8_bin FROM t_diff_charset_columns t1 WHERE futf8_bin in (SELECT t2.futf8_gen FROM t_diff_charset_columns t2);
SELECT futf8_bin FROM t_diff_charset_columns t1 WHERE futf8_bin in (SELECT t2.fgbk_bin FROM t_diff_charset_columns t2);
SELECT futf8_bin FROM t_diff_charset_columns t1 WHERE futf8_gen in (SELECT t2.fgbk_bin FROM t_diff_charset_columns t2);
SELECT futf8_bin FROM t_diff_charset_columns t1 WHERE futf8_gen in (SELECT t2.futf8_uni FROM t_diff_charset_columns t2);
-- -- -- column gbk
EXPLAIN (costs off)
SELECT fgbk_bin FROM t_diff_charset_columns WHERE fgbk_bin in (_gb18030'高斯DB', '高斯DB');
EXPLAIN (costs off)
SELECT fgbk_bin FROM t_diff_charset_columns WHERE fgbk_bin in (_gb18030'高斯DB', '高斯DB' COLLATE utf8mb4_bin);
SELECT fgbk_bin FROM t_diff_charset_columns WHERE fgbk_bin in (_gb18030'高斯DB', '高斯DB' COLLATE utf8mb4_bin);
EXPLAIN (costs off)
SELECT fgbk_bin FROM t_diff_charset_columns WHERE fgbk_bin in (_utf8mb4'高斯DB', _gbk'高斯DB');
SELECT fgbk_bin FROM t_diff_charset_columns WHERE fgbk_bin in (_utf8mb4'高斯DB', _gbk'高斯DB');
EXPLAIN (costs off)
SELECT fgbk_bin FROM t_diff_charset_columns WHERE fgbk_bin in ('高斯DB', concat(_gbk'高斯DB' COLLATE gbk_chinese_ci));
SELECT fgbk_bin FROM t_diff_charset_columns WHERE fgbk_bin in ('高斯DB', concat(_gbk'高斯DB' COLLATE gbk_chinese_ci));
SELECT futf8_bin FROM t_diff_charset_columns t1 WHERE fgbk_bin in (SELECT t2.fgbk_chi FROM t_diff_charset_columns t2);
SELECT futf8_bin FROM t_diff_charset_columns t1 WHERE fgbk_bin in (SELECT t2.futf8_bin FROM t_diff_charset_columns t2);
SELECT futf8_bin FROM t_diff_charset_columns t1 WHERE fgbk_chi in (SELECT t2.fgbk_bin FROM t_diff_charset_columns t2);
SELECT futf8_bin FROM t_diff_charset_columns t1 WHERE fgbk_chi in (SELECT t2.fgb18030_bin FROM t_diff_charset_columns t2); -- ERROR

-- -- COALESCE
SELECT COALESCE(fgbk_chi, futf8_bin) result, collation for(result) FROM t_diff_charset_columns;
SELECT COALESCE(futf8_gen, futf8_bin) result, collation for (result) FROM t_diff_charset_columns;
SELECT COALESCE(futf8_uni, futf8_gen) result, collation for (result) FROM t_diff_charset_columns; -- conflict
SELECT COALESCE(fgbk_chi, fgb18030_chi) result, collation for (result) FROM t_diff_charset_columns; -- ERROR

-- -- GREATEST
SELECT GREATEST(fgbk_chi, futf8_bin) result, collation for (result) FROM t_diff_charset_columns;
SELECT GREATEST(futf8_gen, futf8_bin) result, collation for (result) FROM t_diff_charset_columns;
SELECT GREATEST(futf8_uni, futf8_gen) result, collation for (result) FROM t_diff_charset_columns; -- conflict
SELECT GREATEST(fgbk_chi, fgb18030_chi) result, collation for (result) FROM t_diff_charset_columns; -- ERROR

-- -- rowcompare
SELECT futf8_bin, futf8_uni, fgbk_bin, fgbk_chi, fgb18030_bin, fgb18030_chi FROM t_diff_charset_columns
    WHERE (futf8_bin, futf8_uni, fgbk_bin, fgbk_chi, fgb18030_bin, fgb18030_chi) = ('高斯DB', '高斯db', '高斯DB', '高斯db', '高斯DB', '高斯db');
SELECT futf8_bin, futf8_uni, fgbk_bin, fgbk_chi, fgb18030_bin, fgb18030_chi FROM t_diff_charset_columns
    WHERE (futf8_bin, futf8_uni, fgbk_bin, fgbk_chi, fgb18030_bin, fgb18030_chi) = ('高斯DB', '高斯DB', '高斯DB', '高斯DB', '高斯DB', '高斯DB');

-- -- aggregate
SELECT count(futf8_bin), futf8_bin FROM t_diff_charset_columns GROUP BY futf8_bin;
SELECT count(fgbk_bin), fgbk_bin FROM t_diff_charset_columns GROUP BY fgbk_bin;
SELECT count(fgb18030_bin), fgb18030_bin FROM t_diff_charset_columns GROUP BY fgb18030_bin;
SELECT group_concat(futf8_bin, fgbk_chi), fgb18030_bin FROM t_diff_charset_columns GROUP BY fgb18030_bin;
SELECT group_concat(fgbk_bin, fgbk_chi order by futf8_bin), fgb18030_bin FROM t_diff_charset_columns GROUP BY fgb18030_bin;
SELECT group_concat(DISTINCT fgbk_bin, futf8_bin order by fgbk_bin, futf8_bin), fgb18030_bin FROM t_diff_charset_columns GROUP BY fgb18030_bin;

-- -- UNION
-- -- -- const
select _utf8mb4'高斯' union select _gbk'高斯';
select _gb18030'高斯' union select _gbk'高斯'; -- ERROR
-- -- -- column
select futf8_bin FROM t_diff_charset_columns
    union
    select fgbk_bin FROM t_diff_charset_columns;
select fgb18030_bin FROM t_diff_charset_columns
    union
    select fgbk_bin FROM t_diff_charset_columns; -- ERROR
-- -- -- explicit
select fgbk_chi COLLATE gbk_chinese_ci FROM t_diff_charset_columns
    union
    select fgbk_bin FROM t_diff_charset_columns;
select fgbk_chi COLLATE gbk_chinese_ci FROM t_diff_charset_columns
    union all
    select fgbk_bin FROM t_diff_charset_columns;
select fgbk_chi COLLATE gbk_chinese_ci FROM t_diff_charset_columns
    INTERSECT
    select fgbk_bin FROM t_diff_charset_columns;
-- -- -- mixed
select _utf8mb4'高斯', futf8_bin FROM t_diff_charset_columns
    union
    select _gbk'高斯', fgbk_bin FROM t_diff_charset_columns;
select _utf8mb4'GS', futf8_bin FROM t_diff_charset_columns
    union
    select _gbk'GS', fgbk_bin FROM t_diff_charset_columns;
select _utf8mb4'GS', fgbk_bin FROM t_diff_charset_columns
    union
    select _gbk'GS', fgbk_chi FROM t_diff_charset_columns;
-- -- -- 3 select
select _utf8mb4'GS', fgbk_bin FROM t_diff_charset_columns
    union
    select _gbk'GS', futf8_bin FROM t_diff_charset_columns
    union
    select _gb18030'GS', fgb18030_bin FROM t_diff_charset_columns;
select _utf8mb4'GS', fgbk_bin FROM t_diff_charset_columns
    union
    select _gbk'GS', futf8_bin FROM t_diff_charset_columns
    union
    select _gb18030'GS', fgb18030_chi COLLATE gb18030_chinese_ci FROM t_diff_charset_columns;
-- -- -- complex select
select _utf8mb4'GS', GROUP_CONCAT(fgbk_bin, fgbk_chi), fgbk_bin FROM t_diff_charset_columns GROUP BY fgbk_bin
    union
    select _gbk'GS', GROUP_CONCAT(futf8_bin, futf8_uni), futf8_bin FROM t_diff_charset_columns GROUP BY futf8_bin
    union
    select _gb18030'GS', GROUP_CONCAT(fgb18030_bin, fgb18030_chi), fgb18030_bin FROM t_diff_charset_columns GROUP BY fgb18030_bin;
select _utf8mb4'GS', GROUP_CONCAT(fgbk_bin, fgbk_chi), fgbk_bin FROM t_diff_charset_columns GROUP BY fgbk_bin
    union
    select _gbk'GS', GROUP_CONCAT(futf8_bin, futf8_uni), futf8_bin FROM t_diff_charset_columns GROUP BY futf8_bin
    union
    select _gb18030'GS', GROUP_CONCAT(fgb18030_bin, fgb18030_chi  COLLATE gb18030_chinese_ci), fgb18030_chi COLLATE gb18030_chinese_ci FROM t_diff_charset_columns GROUP BY fgb18030_chi;

-- -- test COLLATE for function
-- -- -- for string function
SELECT CONCAT(_utf8mb4'高斯DB' COLLATE utf8mb4_unicode_ci , _gbk'高斯DB' COLLATE gbk_chinese_ci) COLLATE utf8mb4_general_ci result, collation for(result);
SELECT CONCAT(_utf8mb4'高斯DB' COLLATE utf8mb4_unicode_ci , _gbk'高斯DB' COLLATE gbk_chinese_ci) COLLATE gbk_chinese_ci result, collation for(result); -- ERROR
SELECT CONCAT(_utf8mb4'高斯DB' , _gbk'高斯DB') COLLATE utf8mb4_bin result, collation for(result);
SELECT CONCAT(_utf8mb4'高斯DB' , _gbk'高斯DB') COLLATE gbk_chinese_ci result, collation for(result); -- ERROR
SELECT CONCAT(_utf8mb4'高斯DB' , _gbk'高斯DB') COLLATE "binary" result, collation for(result); -- ERROR
SELECT CONCAT(futf8_uni , futf8_gen) COLLATE utf8mb4_bin result, collation for(result) FROM t_diff_charset_columns;
SELECT CONCAT(futf8_uni , futf8_gen) COLLATE gbk_chinese_ci result, collation for(result) FROM t_diff_charset_columns; -- ERROR
-- -- -- for binary argument string function
SELECT CONCAT(_utf8mb4'高斯DB', _binary'高斯DB') COLLATE "binary" result, collation for(result); -- ERROR
SELECT CONCAT(_utf8mb4'高斯DB', _binary'高斯DB') COLLATE utf8mb4_unicode_ci result, collation for(result);
SELECT CONCAT(futf8_uni, fbytea) COLLATE "binary" result, collation for(result) FROM t_diff_charset_columns; -- ERROR
SELECT CONCAT(futf8_uni, fbytea) COLLATE gbk_chinese_ci result, collation for(result) FROM t_diff_charset_columns; -- ERROR
-- -- -- for binary function
SELECT CONVERT(fbytea, 'UTF8', 'GBK') COLLATE "binary" result, collation for(result) FROM t_diff_charset_columns; -- pg_collation_for binary type ERROR
SELECT CONVERT(fbytea, 'UTF8', 'GBK') COLLATE "binary" result FROM t_diff_charset_columns;
SELECT CONCAT(CONVERT(fbytea, 'UTF8', 'GBK'), futf8_uni) result, collation for(result) FROM t_diff_charset_columns;
SELECT CONCAT(CONVERT(fbytea, 'UTF8', 'GBK'), futf8_uni) COLLATE "binary" result, collation for(result) FROM t_diff_charset_columns; -- ERROR
SELECT CONCAT(CONVERT(fbytea, 'UTF8', 'GBK'), futf8_uni) COLLATE utf8mb4_unicode_ci result, collation for(result) FROM t_diff_charset_columns;

DROP TABLE t_diff_charset_columns;

-- -- -- DML 中文 utf8mb4 COLUMNS
DROP TABLE IF EXISTS t_charset_utf8mb4;
CREATE TABLE t_charset_utf8mb4(
	id int auto_increment primary key,
	a text charset utf8mb4 collate 'utf8mb4_bin',
	b bytea
);

INSERT INTO t_charset_utf8mb4(a,b) VALUES('高斯', '高斯');
INSERT INTO t_charset_utf8mb4(a,b) VALUES('楂樻柉', '楂樻柉');
INSERT INTO t_charset_utf8mb4(a,b) VALUES('高斯DB', '高斯DB');
INSERT INTO t_charset_utf8mb4(a,b) VALUES(_gbk'高斯DB', (_gbk'高斯DB')::bytea);
INSERT INTO t_charset_utf8mb4(a,b) VALUES(_gbk'高斯DB' COLLATE 'gbk_chinese_ci',
    (_gbk'高斯DB' COLLATE 'gbk_chinese_ci')::bytea);
INSERT INTO t_charset_utf8mb4 SELECT 6,_gbk'高斯DB', (_gbk'高斯DB')::bytea;
-- -- -- -- test deparse for explain and select into
EXPLAIN (costs off)
SELECT id,a FROM t_charset_utf8mb4 WHERE a=_gbk'高斯DB' LIMIT 1;
SELECT id,a FROM t_charset_utf8mb4 WHERE a=_gbk'高斯DB' LIMIT 1 into @id_res,@a_res;
SELECT @id_res,@a_res;
EXPLAIN (costs off)
SELECT id,a FROM t_charset_utf8mb4 WHERE a=_gbk'高斯db' COLLATE gbk_chinese_ci LIMIT 1;
SELECT id,a FROM t_charset_utf8mb4 WHERE a=_gbk'高斯db' COLLATE gbk_chinese_ci LIMIT 1 into @id_res,@a_res;
SELECT @id_res,@a_res;
EXPLAIN (costs off)
SELECT id,a FROM t_charset_utf8mb4 WHERE a COLLATE utf8mb4_unicode_ci =_gbk'高斯db' LIMIT 1;
SELECT id,a FROM t_charset_utf8mb4 WHERE a COLLATE utf8mb4_unicode_ci =_gbk'高斯db' LIMIT 1 into @id_res,@a_res;
SELECT @id_res,@a_res;
EXPLAIN (costs off)
SELECT id,a FROM t_charset_utf8mb4 WHERE a=_utf8mb4'高斯DB' LIMIT 1;
SELECT id,a FROM t_charset_utf8mb4 WHERE a=_utf8mb4'高斯DB' LIMIT 1 into @id_res,@a_res;
SELECT @id_res,@a_res;
EXPLAIN (costs off)
SELECT id,a FROM t_charset_utf8mb4 WHERE a=_utf8mb4'高斯db' COLLATE utf8mb4_unicode_ci LIMIT 1;
SELECT id,a FROM t_charset_utf8mb4 WHERE a=_utf8mb4'高斯db' COLLATE utf8mb4_unicode_ci LIMIT 1 into @id_res,@a_res;
SELECT @id_res,@a_res;
-- -- -- -- test condition
SELECT * FROM t_charset_utf8mb4 WHERE a='楂樻柉' ORDER BY id;
SELECT * FROM t_charset_utf8mb4 WHERE a=_utf8mb4'高斯' ORDER BY id;
SELECT * FROM t_charset_utf8mb4 WHERE a=_utf8mb4'高斯db' COLLATE utf8mb4_unicode_ci ORDER BY id;
SELECT * FROM t_charset_utf8mb4 WHERE a=_binary'高斯DB' ORDER BY id;
SELECT * FROM t_charset_utf8mb4 WHERE a=_gbk'高斯DB' ORDER BY id;
SELECT * FROM t_charset_utf8mb4 WHERE a=_gbk'高斯db' COLLATE gbk_chinese_ci ORDER BY id;

UPDATE t_charset_utf8mb4 SET a='高斯DB', b='高斯DB'
    WHERE a=_gbk'高斯DB' COLLATE 'gbk_chinese_ci';
UPDATE t_charset_utf8mb4 SET a=_gbk'高斯DB', b=(_gbk'高斯DB')::bytea
	WHERE a='楂樻柉';
UPDATE t_charset_utf8mb4 SET a=_utf8mb4'高斯DB', b=(_utf8mb4'高斯DB')::bytea
	WHERE a=_gbk'高斯DB';
SELECT * FROM t_charset_utf8mb4 ORDER BY id;

DELETE FROM t_charset_utf8mb4 WHERE a=_utf8mb4'高斯';
DELETE FROM t_charset_utf8mb4 WHERE a::bytea=_binary'高斯DB';
SELECT * FROM t_charset_utf8mb4 ORDER BY id;
-- -- -- -- test assign expr
INSERT INTO t_charset_utf8mb4(a,b) VALUES(CONCAT('高斯','DB'), (CONCAT('高斯','DB'))::bytea);
INSERT INTO t_charset_utf8mb4 SELECT 0, CONCAT('高斯','DB'), (CONCAT('高斯','DB'))::bytea;
SELECT * FROM t_charset_utf8mb4 ORDER BY id;
INSERT INTO t_charset_utf8mb4(id,a,b) VALUES(7, CONCAT('高斯','DB'), (CONCAT('高斯','DB'))::bytea) ON DUPLICATE KEY UPDATE a=CONCAT('高斯','db');
SELECT * FROM t_charset_utf8mb4 ORDER BY id;
UPDATE t_charset_utf8mb4 SET a=CONCAT('DB','高斯'), b=(CONCAT('DB','高斯'))::bytea
	WHERE a=CONCAT('高斯','DB');
SELECT * FROM t_charset_utf8mb4 ORDER BY id;
DROP TABLE IF EXISTS t_charset_utf8mb4;

-- -- -- DML 中文 gbk COLUMNS
DROP TABLE IF EXISTS t_charset_gbk;
CREATE TABLE t_charset_gbk(
	id int auto_increment primary key,
	a text charset gbk collate "gbk_bin",
	b bytea
);

INSERT INTO t_charset_gbk(a,b) VALUES('高斯', '高斯');
INSERT INTO t_charset_gbk(a,b) VALUES('高斯DB', '高斯DB');
INSERT INTO t_charset_gbk(a,b) VALUES(_gbk'高斯DB', (_gbk'高斯DB')::bytea);
INSERT INTO t_charset_gbk(a,b) VALUES(_gbk'高斯DB' COLLATE 'gbk_chinese_ci',
    (_gbk'高斯DB' COLLATE 'gbk_chinese_ci')::bytea);
INSERT INTO t_charset_gbk(a,b) VALUES('楂樻柉', '楂樻柉');
INSERT INTO t_charset_gbk SELECT 6,_gbk'高斯DB', (_gbk'高斯DB')::bytea;
-- -- -- -- test deparse for explain and select into
EXPLAIN (costs off)
SELECT id,a FROM t_charset_gbk WHERE a=_gbk'高斯DB' LIMIT 1;
SELECT id,a FROM t_charset_gbk WHERE a=_gbk'高斯DB' LIMIT 1 into @id_res,@a_res;
SELECT @id_res,@a_res;
EXPLAIN (costs off)
SELECT id,a FROM t_charset_gbk WHERE a=_gbk'高斯db' COLLATE gbk_chinese_ci LIMIT 1;
SELECT id,a FROM t_charset_gbk WHERE a=_gbk'高斯db' COLLATE gbk_chinese_ci LIMIT 1 into @id_res,@a_res;
SELECT @id_res,@a_res;
EXPLAIN (costs off)
SELECT id,a FROM t_charset_gbk WHERE a=_utf8mb4'高斯DB' LIMIT 1;
SELECT id,a FROM t_charset_gbk WHERE a=_utf8mb4'高斯DB' LIMIT 1 into @id_res,@a_res;
SELECT @id_res,@a_res;
EXPLAIN (costs off)
SELECT id,a FROM t_charset_gbk WHERE a=_utf8mb4'高斯db' COLLATE utf8mb4_unicode_ci LIMIT 1;
SELECT id,a FROM t_charset_gbk WHERE a=_utf8mb4'高斯db' COLLATE utf8mb4_unicode_ci LIMIT 1 into @id_res,@a_res;
SELECT @id_res,@a_res;
EXPLAIN (costs off)
SELECT id,a FROM t_charset_gbk WHERE a COLLATE gbk_chinese_ci =_utf8mb4'高斯db' LIMIT 1;
SELECT id,a FROM t_charset_gbk WHERE a COLLATE gbk_chinese_ci =_utf8mb4'高斯db' LIMIT 1 into @id_res,@a_res;
SELECT @id_res,@a_res;
-- -- -- -- test condition
SELECT * FROM t_charset_gbk WHERE a=_utf8mb4'高斯' ORDER BY id;
SELECT * FROM t_charset_gbk WHERE a=_utf8mb4'高斯db' COLLATE utf8mb4_unicode_ci ORDER BY id;
SELECT * FROM t_charset_gbk WHERE a=_binary'高斯DB' ORDER BY id;
SELECT * FROM t_charset_gbk WHERE a=_gbk'高斯DB' ORDER BY id;
SELECT * FROM t_charset_gbk WHERE a=_gbk'高斯db' COLLATE gbk_chinese_ci ORDER BY id;
SELECT * FROM t_charset_gbk WHERE a=_gbk'高斯' ORDER BY id;
SELECT * FROM t_charset_gbk WHERE a='楂樻柉' ORDER BY id;

UPDATE t_charset_gbk SET a='高斯DB', b='高斯DB'
	WHERE a=_gbk'高斯DB' COLLATE 'gbk_chinese_ci';
UPDATE t_charset_gbk SET a=_gbk'高斯DB', b=(_gbk'高斯DB')::bytea
	WHERE a='楂樻柉';
UPDATE t_charset_gbk SET a=_utf8mb4'高斯DB', b=(_utf8mb4'高斯DB')::bytea
	WHERE a=_gbk'高斯DB';
SELECT * FROM t_charset_gbk ORDER BY id;

DELETE FROM t_charset_gbk WHERE a=_utf8mb4'高斯';
DELETE FROM t_charset_gbk WHERE a::bytea=_binary'高斯DB'; -- DELETE 0
DELETE FROM t_charset_gbk WHERE a::bytea= E'\\xB8DFCBB94442'::bytea;
SELECT * FROM t_charset_gbk ORDER BY id;
-- -- -- -- test assign expr
INSERT INTO t_charset_gbk(a,b) VALUES(CONCAT('高斯','DB'), (CONCAT('高斯','DB'))::bytea);
INSERT INTO t_charset_gbk SELECT 0, CONCAT('高斯','DB'), (CONCAT('高斯','DB'))::bytea;
SELECT * FROM t_charset_gbk ORDER BY id;
INSERT INTO t_charset_gbk(id,a,b) VALUES(7, CONCAT('高斯','DB'), (CONCAT('高斯','DB'))::bytea) ON DUPLICATE KEY UPDATE a=CONCAT('高斯','db');
SELECT * FROM t_charset_gbk ORDER BY id;
UPDATE t_charset_gbk SET a=CONCAT('DB','高斯'), b=(CONCAT('DB','高斯'))::bytea
	WHERE a=CONCAT('高斯','DB');
SELECT * FROM t_charset_gbk ORDER BY id;
DROP TABLE IF EXISTS t_charset_gbk;

-- -- test partkey
CREATE TABLE t_multi_charset_partkey (part varchar(32) collate utf8mb4_general_ci, a int)
    PARTITION BY RANGE(part) (
        partition p1 values less than('高斯DB'),
        partition p2 values less than('高斯db'),
        partition p3 values less than(MAXVALUE)
); -- ERROR
CREATE TABLE t_multi_charset_partkey (part varchar(32) collate utf8mb4_general_ci, a int)
    PARTITION BY RANGE(part) (
        partition p1 values less than('楂樻柉DB'),
        partition p2 values less than(_gbk'高斯db'),
        partition p3 values less than(MAXVALUE)
); -- ERROR
CREATE TABLE t_multi_charset_partkey (part varchar(32) collate utf8mb4_general_ci, a int)
    PARTITION BY LIST(part) (
        partition p1 values('高斯DB'),
        partition p2 values('高斯db')
); -- ERROR
CREATE TABLE t_multi_charset_partkey (part varchar(32) collate utf8mb4_general_ci, a int)
    PARTITION BY RANGE(part) (
        PARTITION pass START('高斯DB') END('高斯db'),
        PARTITION excellent START('高斯db') END(MAXVALUE)
); -- unsupported


-- -- -- utf8mb4
CREATE TABLE t_multi_charset_partkey (part text collate utf8mb4_bin, a int)
    PARTITION BY HASH(part) (
        partition p1,
        partition p2,
        partition p3,
        partition p4
);
-- -- -- insert
INSERT INTO t_multi_charset_partkey VALUES(_gbk'高斯DB', 1);
INSERT INTO t_multi_charset_partkey VALUES(_gbk'高斯db', 2);
INSERT INTO t_multi_charset_partkey VALUES(_utf8mb4'高斯DB1', 3);
INSERT INTO t_multi_charset_partkey VALUES(_utf8mb4'高斯db1', 4);
-- -- -- select
SELECT * FROM t_multi_charset_partkey PARTITION(p1) order by 1,2;
SELECT * FROM t_multi_charset_partkey PARTITION(p2) order by 1,2;
SELECT * FROM t_multi_charset_partkey PARTITION(p3) order by 1,2;
SELECT * FROM t_multi_charset_partkey PARTITION FOR(_gbk'高斯db') order by 1,2;
SELECT * FROM t_multi_charset_partkey PARTITION FOR(_utf8mb4'高斯DB1') order by 1,2;
-- -- -- partition pruning
EXPLAIN (costs off)
SELECT * FROM t_multi_charset_partkey WHERE part=_gbk'高斯DB' order by 1,2;
SELECT * FROM t_multi_charset_partkey WHERE part=_gbk'高斯DB' order by 1,2;
EXPLAIN (costs off)
SELECT * FROM t_multi_charset_partkey WHERE part=_utf8mb4'高斯db1' order by 1,2;
SELECT * FROM t_multi_charset_partkey WHERE part=_utf8mb4'高斯db1' order by 1,2;
EXPLAIN (costs off)
SELECT * FROM t_multi_charset_partkey WHERE part=_gbk'高斯DB' collate gbk_chinese_ci order by 1,2; -- ALL PARTS
SELECT * FROM t_multi_charset_partkey WHERE part=_gbk'高斯DB' collate gbk_chinese_ci order by 1,2;
-- -- -- partiton ddl
ALTER TABLE t_multi_charset_partkey SPLIT PARTITION p1 AT ( '高斯DB' ) INTO ( PARTITION p1, PARTITION p4); -- not support
ALTER TABLE t_multi_charset_partkey RENAME PARTITION FOR(_gbk'高斯db') TO newp1;
SELECT * FROM t_multi_charset_partkey PARTITION(newp1) order by 1,2;
DROP TABLE t_multi_charset_partkey;

-- -- -- utf8mb4
CREATE TABLE t_multi_charset_partkey (part text collate utf8mb4_bin, a int)
    PARTITION BY RANGE(part) (
        partition p1 values less than('楂樻柉DB'),
        partition p2 values less than('楂樻柉db'),
        partition p3 values less than('高斯DB'),
        partition p4 values less than('高斯db'),
        partition p5 values less than(MAXVALUE)
);
-- -- -- insert
INSERT INTO t_multi_charset_partkey VALUES(_gbk'高斯DB', 1);
INSERT INTO t_multi_charset_partkey VALUES(_gbk'高斯db', 2);
INSERT INTO t_multi_charset_partkey VALUES(_utf8mb4'高斯DB', 3);
INSERT INTO t_multi_charset_partkey VALUES(_utf8mb4'高斯db', 4);
-- -- -- select
SELECT * FROM t_multi_charset_partkey PARTITION(p1) order by 1,2;
SELECT * FROM t_multi_charset_partkey PARTITION(p2) order by 1,2;
SELECT * FROM t_multi_charset_partkey PARTITION(p3) order by 1,2;
SELECT * FROM t_multi_charset_partkey PARTITION(p4) order by 1,2;
SELECT * FROM t_multi_charset_partkey PARTITION(p5) order by 1,2;
SELECT * FROM t_multi_charset_partkey PARTITION FOR(_gbk'高斯db') order by 1,2;
SELECT * FROM t_multi_charset_partkey PARTITION FOR(_utf8mb4'高斯DB') order by 1,2;
-- -- -- partition pruning
EXPLAIN (costs off)
SELECT * FROM t_multi_charset_partkey WHERE part=_gbk'高斯DB' order by 1,2;
SELECT * FROM t_multi_charset_partkey WHERE part=_gbk'高斯DB' order by 1,2;
EXPLAIN (costs off)
SELECT * FROM t_multi_charset_partkey WHERE part=_utf8mb4'高斯db' order by 1,2;
SELECT * FROM t_multi_charset_partkey WHERE part=_utf8mb4'高斯db' order by 1,2;
EXPLAIN (costs off)
SELECT * FROM t_multi_charset_partkey WHERE part=_gbk'高斯DB' collate gbk_chinese_ci order by 1,2; -- ALL PARTS
SELECT * FROM t_multi_charset_partkey WHERE part=_gbk'高斯DB' collate gbk_chinese_ci order by 1,2;
-- -- -- partiton ddl
ALTER TABLE t_multi_charset_partkey SPLIT PARTITION FOR(_gbk'高斯DB') AT (_gbk'高斯DB1 ') INTO (PARTITION p2_1, PARTITION p2_2);
INSERT INTO t_multi_charset_partkey VALUES(_gbk'高斯DB1', 1);
SELECT * FROM t_multi_charset_partkey PARTITION(p2_1) order by 1,2;
SELECT * FROM t_multi_charset_partkey PARTITION(p2_2) order by 1,2;
ALTER TABLE t_multi_charset_partkey RENAME PARTITION FOR(_gbk'高斯db') TO p3_1;
SELECT * FROM t_multi_charset_partkey PARTITION(p3_1) order by 1,2;
DROP TABLE t_multi_charset_partkey;

-- -- -- utf8mb4
CREATE TABLE t_multi_charset_partkey (part varchar(32) collate utf8mb4_unicode_ci, part2 varchar(32) collate utf8mb4_general_ci, a int)
    PARTITION BY LIST COLUMNS(part, part2) (
        partition p1 values in(('楂樻柉DB', '楂樻柉db')),
        partition p2 values in(('高斯db', '高斯DB'))
);
-- -- -- insert
INSERT INTO t_multi_charset_partkey VALUES(_gbk'高斯DB', _gbk'高斯DB', 1);
INSERT INTO t_multi_charset_partkey VALUES(_gbk'高斯db', _gbk'高斯db', 2);
INSERT INTO t_multi_charset_partkey VALUES(_utf8mb4'高斯DB', _utf8mb4'高斯DB', 3);
INSERT INTO t_multi_charset_partkey VALUES(_utf8mb4'高斯db', _utf8mb4'高斯db', 4);
-- -- -- select
SELECT * FROM t_multi_charset_partkey PARTITION(p1) order by 1,2;
SELECT * FROM t_multi_charset_partkey PARTITION(p2) order by 1,2;
SELECT * FROM t_multi_charset_partkey PARTITION FOR(_gbk'高斯DB', _gbk'高斯db') order by 1,2;
SELECT * FROM t_multi_charset_partkey PARTITION FOR(_utf8mb4'高斯db', _utf8mb4'高斯db') order by 1,2;
-- -- -- partition pruning
EXPLAIN (costs off)
SELECT * FROM t_multi_charset_partkey WHERE part=_gbk'高斯DB' order by 1,2;
SELECT * FROM t_multi_charset_partkey WHERE part=_gbk'高斯DB' order by 1,2;
EXPLAIN (costs off)
SELECT * FROM t_multi_charset_partkey WHERE part=_utf8mb4'高斯db' order by 1,2;
SELECT * FROM t_multi_charset_partkey WHERE part=_utf8mb4'高斯db' order by 1,2;
EXPLAIN (costs off)
SELECT * FROM t_multi_charset_partkey WHERE part=_gbk'高斯DB' collate gbk_chinese_ci order by 1,2; -- ALL PARTS
SELECT * FROM t_multi_charset_partkey WHERE part=_gbk'高斯DB' collate gbk_chinese_ci order by 1,2;
-- -- -- partiton ddl
ALTER TABLE t_multi_charset_partkey RENAME PARTITION FOR(_gbk'高斯DB', _gbk'高斯db') TO p1_1;
SELECT * FROM t_multi_charset_partkey PARTITION(p1_1) order by 1,2;
DROP TABLE t_multi_charset_partkey;

-- -- -- gbk
CREATE TABLE t_multi_charset_partkey (part varchar(32) CHARACTER set gbk collate gbk_bin, a int)
    PARTITION BY RANGE(part) (
        partition p1 values less than('高斯DB'),
        partition p2 values less than(MAXVALUE)
); -- error

-- -- test column default value
CREATE TABLE t_default_charset(
    a varchar(32) character set gbk collate gbk_bin DEFAULT '高斯DB',
    b varchar(32) character set utf8mb4 collate utf8mb4_unicode_ci GENERATED ALWAYS AS (a) STORED,
    c varchar(32) character set utf8mb4 collate utf8mb4_unicode_ci GENERATED ALWAYS AS (a||_utf8mb4'高斯DB') STORED);
INSERT INTO t_default_charset VALUES(DEFAULT, DEFAULT, DEFAULT);
select * from t_default_charset;
DROP TABLE t_default_charset;
-- ------------------------------------------
-- SET NAMES GBK;
SET NAMES 'GBK';
SHOW client_encoding;
SHOW server_encoding;
SHOW character_set_connection;
SHOW collation_connection;

-- 中文
SELECT CAST('高斯' AS bytea);
SELECT CAST(_binary'高斯' AS bytea);
SELECT CAST(_utf8mb4'高斯' AS bytea);
SELECT CAST(_gbk'高斯' AS bytea);
SELECT _binary'高斯';
SELECT _utf8mb4'高斯';
SELECT _gbk'高斯';
SELECT _binary X'E9AB98E696AF';
SELECT _utf8mb4 X'E9AB98E696AF';
SELECT _gbk X'E9AB98E696AF';
SELECT '楂樻柉'; -- ERROR '楂樻柉'(\xe6a582e6a8bbe69f89) will be converted to utf8 first
SELECT CONVERT_TO(_utf8mb4'楂樻柉', 'gbk'); -- ERROR '楂樻柉'(\xe6a582e6a8bbe69f89) will be converted to utf8 first
SELECT CONVERT_TO(_utf8mb4'高斯', 'gbk');

-- -- -- DML 中文 utf8mb4 COLUMNS
DROP TABLE IF EXISTS t_charset_utf8mb4;
CREATE TABLE t_charset_utf8mb4(
	id int auto_increment primary key,
	a text charset utf8mb4 collate 'utf8mb4_bin',
	b bytea
);

INSERT INTO t_charset_utf8mb4(a,b) VALUES('高斯', '高斯');
INSERT INTO t_charset_utf8mb4(a,b) VALUES('楂樻柉', '楂樻柉'); -- ERROR
INSERT INTO t_charset_utf8mb4(a,b) VALUES('高斯DB', '高斯DB');
INSERT INTO t_charset_utf8mb4(a,b) VALUES(_utf8mb4'高斯DB', (_utf8mb4'高斯DB')::bytea);
INSERT INTO t_charset_utf8mb4(a,b) VALUES(_utf8mb4'高斯DB' COLLATE 'utf8mb4_unicode_ci',
    (_utf8mb4'高斯DB' COLLATE 'utf8mb4_unicode_ci')::bytea);
INSERT INTO t_charset_utf8mb4 SELECT 6,_utf8mb4'高斯DB', (_utf8mb4'高斯DB')::bytea;

-- -- -- -- test deparse for explain and select into
EXPLAIN (costs off)
SELECT id,a FROM t_charset_utf8mb4 WHERE a=_gbk'高斯DB';
SELECT id,a FROM t_charset_utf8mb4 WHERE a=_gbk'高斯DB' into @id_res,@a_res;
SELECT @id_res,@a_res;
EXPLAIN (costs off)
SELECT id,a FROM t_charset_utf8mb4 WHERE a=_gbk'高斯db' COLLATE gbk_chinese_ci;
SELECT id,a FROM t_charset_utf8mb4 WHERE a=_gbk'高斯db' COLLATE gbk_chinese_ci into @id_res,@a_res;
SELECT @id_res,@a_res;
EXPLAIN (costs off)
SELECT id,a FROM t_charset_utf8mb4 WHERE a COLLATE utf8mb4_unicode_ci =_gbk'高斯db';
SELECT id,a FROM t_charset_utf8mb4 WHERE a COLLATE utf8mb4_unicode_ci =_gbk'高斯db' into @id_res,@a_res;
SELECT @id_res,@a_res;
EXPLAIN (costs off)
SELECT id,a FROM t_charset_utf8mb4 WHERE a=_utf8mb4'高斯DB' LIMIT 1;
SELECT id,a FROM t_charset_utf8mb4 WHERE a=_utf8mb4'高斯DB' LIMIT 1 into @id_res,@a_res;
SELECT @id_res,@a_res;
EXPLAIN (costs off)
SELECT id,a FROM t_charset_utf8mb4 WHERE a=_utf8mb4'高斯db' COLLATE utf8mb4_unicode_ci LIMIT 1;
SELECT id,a FROM t_charset_utf8mb4 WHERE a=_utf8mb4'高斯db' COLLATE utf8mb4_unicode_ci LIMIT 1 into @id_res,@a_res;
SELECT @id_res,@a_res;
-- -- -- -- test condition
SELECT * FROM t_charset_utf8mb4 WHERE a='楂樻柉' ORDER BY id; -- ERROR
SELECT * FROM t_charset_utf8mb4 WHERE a=_gbk'高斯' ORDER BY id;
SELECT * FROM t_charset_utf8mb4 WHERE a=_gbk'高斯db' COLLATE gbk_chinese_ci ORDER BY id;
SELECT * FROM t_charset_utf8mb4 WHERE a=_binary'高斯DB' ORDER BY id; -- ERROR
SELECT * FROM t_charset_utf8mb4 WHERE a=_utf8mb4'高斯DB' ORDER BY id;
SELECT * FROM t_charset_utf8mb4 WHERE a=_utf8mb4'高斯db' COLLATE utf8mb4_unicode_ci ORDER BY id;

UPDATE t_charset_utf8mb4 SET a='高斯DB', b='高斯DB'
    WHERE a=_utf8mb4'高斯DB' COLLATE 'utf8mb4_unicode_ci';
UPDATE t_charset_utf8mb4 SET a=_utf8mb4'高斯DB', b=(_utf8mb4'高斯DB')::bytea
	WHERE a='楂樻柉'; -- ERROR
UPDATE t_charset_utf8mb4 SET a=_utf8mb4'高斯DB', b=(_utf8mb4'高斯DB')::bytea
	WHERE a=_utf8mb4'高斯DB';
SELECT * FROM t_charset_utf8mb4 ORDER BY id;

DELETE FROM t_charset_utf8mb4 WHERE a='高斯';
DELETE FROM t_charset_utf8mb4 WHERE a='高斯DB';
SELECT * FROM t_charset_utf8mb4 ORDER BY id;
-- -- -- -- test assign expr
INSERT INTO t_charset_utf8mb4(a,b) VALUES(CONCAT('高斯','DB'), (CONCAT('高斯','DB'))::bytea);
INSERT INTO t_charset_utf8mb4 SELECT 0, CONCAT('高斯','DB'), (CONCAT('高斯','DB'))::bytea;
SELECT * FROM t_charset_utf8mb4 ORDER BY id;
INSERT INTO t_charset_utf8mb4(id,a,b) VALUES(7, CONCAT('高斯','DB'), (CONCAT('高斯','DB'))::bytea) ON DUPLICATE KEY UPDATE a=CONCAT('高斯','db');
SELECT * FROM t_charset_utf8mb4 ORDER BY id;
UPDATE t_charset_utf8mb4 SET a=CONCAT('DB','高斯'), b=(CONCAT('DB','高斯'))::bytea
	WHERE a=CONCAT('高斯','DB');
SELECT * FROM t_charset_utf8mb4 ORDER BY id;
DROP TABLE IF EXISTS t_charset_utf8mb4;

-- -- -- DML 中文 gbk COLUMNS
DROP TABLE IF EXISTS t_charset_gbk;
CREATE TABLE t_charset_gbk(
	id int auto_increment primary key,
	a text charset gbk collate "gbk_bin",
	b bytea
);

INSERT INTO t_charset_gbk(a,b) VALUES('高斯', '高斯');
INSERT INTO t_charset_gbk(a,b) VALUES('高斯DB', '高斯DB');
INSERT INTO t_charset_gbk(a,b) VALUES(_utf8mb4'高斯DB', (_utf8mb4'高斯DB')::bytea);
INSERT INTO t_charset_gbk(a,b) VALUES(_utf8mb4'高斯DB' COLLATE 'utf8mb4_unicode_ci',
    (_utf8mb4'高斯DB' COLLATE 'utf8mb4_unicode_ci')::bytea);
INSERT INTO t_charset_gbk(a,b) VALUES('楂樻柉', '楂樻柉'); -- ERROR
INSERT INTO t_charset_gbk SELECT 6,_utf8mb4'高斯DB', (_utf8mb4'高斯DB')::bytea;
-- -- -- -- test deparse for explain and select into
EXPLAIN (costs off)
SELECT id,a FROM t_charset_gbk WHERE a=_gbk'高斯DB';
SELECT id,a FROM t_charset_gbk WHERE a=_gbk'高斯DB' into @id_res,@a_res;
SELECT @id_res,@a_res;
EXPLAIN (costs off)
SELECT id,a FROM t_charset_gbk WHERE a=_gbk'高斯db' COLLATE gbk_chinese_ci;
SELECT id,a FROM t_charset_gbk WHERE a=_gbk'高斯db' COLLATE gbk_chinese_ci into @id_res,@a_res;
SELECT @id_res,@a_res;
EXPLAIN (costs off)
SELECT id,a FROM t_charset_gbk WHERE a=_utf8mb4'高斯DB' LIMIT 1;
SELECT id,a FROM t_charset_gbk WHERE a=_utf8mb4'高斯DB' LIMIT 1 into @id_res,@a_res;
SELECT @id_res,@a_res;
EXPLAIN (costs off)
SELECT id,a FROM t_charset_gbk WHERE a=_utf8mb4'高斯db' COLLATE utf8mb4_unicode_ci LIMIT 1;
SELECT id,a FROM t_charset_gbk WHERE a=_utf8mb4'高斯db' COLLATE utf8mb4_unicode_ci LIMIT 1 into @id_res,@a_res;
SELECT @id_res,@a_res;
EXPLAIN (costs off)
SELECT id,a FROM t_charset_gbk WHERE a COLLATE gbk_chinese_ci =_utf8mb4'高斯db' LIMIT 1;
SELECT id,a FROM t_charset_gbk WHERE a COLLATE gbk_chinese_ci =_utf8mb4'高斯db' LIMIT 1 into @id_res,@a_res;
SELECT @id_res,@a_res;
-- -- -- -- test condition
SELECT * FROM t_charset_gbk WHERE a=_gbk'高斯' ORDER BY id;
SELECT * FROM t_charset_gbk WHERE a=_gbk'高斯db' COLLATE gbk_chinese_ci ORDER BY id;
SELECT * FROM t_charset_gbk WHERE a=_binary'高斯DB' ORDER BY id; -- ERROR
SELECT * FROM t_charset_gbk WHERE a=_utf8mb4'高斯DB' ORDER BY id;
SELECT * FROM t_charset_gbk WHERE a=_utf8mb4'高斯db' COLLATE utf8mb4_unicode_ci ORDER BY id;
SELECT * FROM t_charset_gbk WHERE a=_utf8mb4'高斯' ORDER BY id;
SELECT * FROM t_charset_gbk WHERE a='楂樻柉' ORDER BY id; -- ERROR

UPDATE t_charset_gbk SET a='高斯DB', b='高斯DB'
	WHERE a=_utf8mb4'高斯DB' COLLATE 'utf8mb4_unicode_ci';
UPDATE t_charset_gbk SET a=_utf8mb4'高斯DB', b=(_utf8mb4'高斯DB')::bytea
	WHERE a='楂樻柉'; -- ERROR
UPDATE t_charset_gbk SET a=_utf8mb4'高斯DB', b=(_utf8mb4'高斯DB')::bytea
	WHERE a=_utf8mb4'高斯DB';
SELECT * FROM t_charset_gbk ORDER BY id;

DELETE FROM t_charset_gbk WHERE a='高斯';
DELETE FROM t_charset_gbk WHERE a::bytea= _binary'高斯DB';
SELECT * FROM t_charset_gbk ORDER BY id;
-- -- -- -- test assign expr
INSERT INTO t_charset_gbk(a,b) VALUES(CONCAT('高斯','DB'), (CONCAT('高斯','DB'))::bytea);
INSERT INTO t_charset_gbk SELECT 0, CONCAT('高斯','DB'), (CONCAT('高斯','DB'))::bytea;
SELECT * FROM t_charset_gbk ORDER BY id;
INSERT INTO t_charset_gbk(id,a,b) VALUES(7, CONCAT('高斯','DB'), (CONCAT('高斯','DB'))::bytea) ON DUPLICATE KEY UPDATE a=CONCAT('高斯','db');
SELECT * FROM t_charset_gbk ORDER BY id;
UPDATE t_charset_gbk SET a=CONCAT('DB','高斯'), b=(CONCAT('DB','高斯'))::bytea
	WHERE a=CONCAT('高斯','DB');
SELECT * FROM t_charset_gbk ORDER BY id;
DROP TABLE IF EXISTS t_charset_gbk;

-- test multi charset
CREATE TABLE t_diff_charset_columns(
    futf8_bin varchar(16) CHARSET utf8mb4 COLLATE 'utf8mb4_bin',
    futf8_uni varchar(16) CHARSET utf8mb4 COLLATE 'utf8mb4_unicode_ci',
    futf8_gen varchar(16) CHARSET utf8mb4 COLLATE 'utf8mb4_general_ci',
    fgbk_bin varchar(16) CHARSET gbk COLLATE 'gbk_bin',
    fgbk_chi varchar(16) CHARSET gbk COLLATE 'gbk_chinese_ci',
    fgb18030_bin varchar(16) CHARSET gb18030 COLLATE 'gb18030_bin',
    fgb18030_chi varchar(16) CHARSET gb18030 COLLATE 'gb18030_chinese_ci',
    fbytea bytea,
    fblob blob,
    fbit bit varying(64)
);
INSERT INTO t_diff_charset_columns(futf8_bin, futf8_uni, futf8_gen, fgbk_bin, fgbk_chi, fgb18030_bin, fgb18030_chi, fbytea, fblob, fbit)
    VALUES('高斯DB', '高斯db', '高斯db', '高斯DB', '高斯db', '高斯DB', '高斯db', '高斯DB', 'E9AB98E696AF', B'111010011010101110011000111001101001011010101111');
-- -- test character length
SELECT futf8_bin,
       futf8_uni,
       fgbk_bin,
       fgbk_chi,
       fgb18030_bin,
       fgb18030_chi,
       fbytea FROM t_diff_charset_columns;
SELECT length(futf8_bin),
       length(futf8_uni),
       length(fgbk_bin),
       length(fgbk_chi),
       length(fgb18030_bin),
       length(fgb18030_chi),
       length(fbytea) FROM t_diff_charset_columns; -- 5
SELECT lengthb(futf8_bin),
       lengthb(futf8_uni),
       lengthb(fgbk_bin),
       lengthb(fgbk_chi),
       lengthb(fgb18030_bin),
       lengthb(fgb18030_chi),
       length(fbytea) FROM t_diff_charset_columns;

-- test prefixkey index
CREATE INDEX idx_prefixkey_futf8_bin on t_diff_charset_columns (futf8_bin(2) text_pattern_ops);
set enable_seqscan=off;
EXPLAIN (costs false)
SELECT /*+ indexscan(t_diff_charset_columns idx_prefixkey_futf8_bin) */ futf8_bin FROM t_diff_charset_columns WHERE futf8_bin LIKE '高斯%';
SELECT /*+ indexscan(t_diff_charset_columns idx_prefixkey_futf8_bin) */ futf8_bin FROM t_diff_charset_columns WHERE futf8_bin LIKE '高斯%';
EXPLAIN (costs false)
SELECT /*+ indexscan(t_diff_charset_columns idx_prefixkey_futf8_bin) */ futf8_bin FROM t_diff_charset_columns WHERE futf8_bin = '高斯DB';
SELECT /*+ indexscan(t_diff_charset_columns idx_prefixkey_futf8_bin) */ futf8_bin FROM t_diff_charset_columns WHERE futf8_bin = '高斯DB';
CREATE INDEX idx_prefixkey_fgbk_bin on t_diff_charset_columns (fgbk_bin(2) text_pattern_ops);
EXPLAIN (costs false)
SELECT /*+ indexscan(t_diff_charset_columns idx_prefixkey_fgbk_bin) */ fgbk_bin FROM t_diff_charset_columns WHERE fgbk_bin LIKE '高斯%';
SELECT /*+ indexscan(t_diff_charset_columns idx_prefixkey_fgbk_bin) */ fgbk_bin FROM t_diff_charset_columns WHERE fgbk_bin LIKE '高斯%';
EXPLAIN (costs false)
SELECT /*+ indexscan(t_diff_charset_columns idx_prefixkey_fgbk_bin) */ fgbk_bin FROM t_diff_charset_columns WHERE fgbk_bin = '高斯DB';
SELECT /*+ indexscan(t_diff_charset_columns idx_prefixkey_fgbk_bin) */ fgbk_bin FROM t_diff_charset_columns WHERE fgbk_bin = '高斯DB';
CREATE INDEX idx_prefixkey_fgb18030_bin on t_diff_charset_columns (fgb18030_bin(2) text_pattern_ops);
EXPLAIN (costs false)
SELECT /*+ indexscan(t_diff_charset_columns idx_prefixkey_fgb18030_bin) */ fgb18030_bin FROM t_diff_charset_columns WHERE fgb18030_bin LIKE '高斯%';
SELECT /*+ indexscan(t_diff_charset_columns idx_prefixkey_fgb18030_bin) */ fgb18030_bin FROM t_diff_charset_columns WHERE fgb18030_bin LIKE '高斯%';
EXPLAIN (costs false)
SELECT /*+ indexscan(t_diff_charset_columns idx_prefixkey_fgb18030_bin) */ fgb18030_bin FROM t_diff_charset_columns WHERE fgb18030_bin = '高斯DB';
SELECT /*+ indexscan(t_diff_charset_columns idx_prefixkey_fgb18030_bin) */ fgb18030_bin FROM t_diff_charset_columns WHERE fgb18030_bin = '高斯DB';
reset enable_seqscan;

DROP TABLE t_diff_charset_columns;
\c regression