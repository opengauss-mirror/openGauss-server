CREATE DATABASE c_gbk_b_db WITH ENCODING 'gbk' LC_COLLATE='C' LC_CTYPE='C' DBCOMPATIBILITY 'B';
\c c_gbk_b_db

SET b_format_behavior_compat_options = 'all';
SHOW b_format_behavior_compat_options;
-- ------------------------------------------
SET NAMES 'gbk';
SHOW client_encoding;
SHOW server_encoding;
SHOW character_set_connection;
SHOW collation_connection;
set enable_expr_fusion = ON;

-- -- CONST charset and collation
SELECT _utf8mb4'高斯' COLLATE gbk_chinese_ci; -- ERROR
SELECT _utf8mb4'高斯' COLLATE 'binary'; -- ERROR
SELECT _utf8mb4'高斯' COLLATE "zh_CN.utf8";
SELECT _utf8mb4'高斯' COLLATE "C"; -- ERROR
SELECT _gbk'高斯' COLLATE gbk_chinese_ci;
SELECT _gbk'高斯' COLLATE 'binary'; -- ERROR
SELECT _gbk'高斯' COLLATE "zh_CN.gbk"; -- support origin collation
SELECT _gbk'高斯' COLLATE "C"; -- support origin collation

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
SELECT '高斯' COLLATE "utf8mb4_unicode_ci"; -- ERROR
SELECT '高斯' COLLATE "gbk_chinese_ci";
SELECT '高斯' COLLATE "gb18030_chinese_ci"; -- ERROR
SELECT '高斯' COLLATE "binary"; -- ERROR

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
SELECT CONVERT_TO(_utf8mb4'楂樻柉', 'gbk'); -- ERROR
SELECT CONVERT_TO(_utf8mb4'高斯', 'gbk'); -- ERROR
SELECT COLLATION FOR(CAST('高斯' AS bytea)::text);

-- -- 中文 const compare
-- -- -- same charset & explicit collation
SELECT _utf8mb4'高斯DB' COLLATE utf8mb4_bin = _utf8mb4'高斯DB' COLLATE utf8mb4_unicode_ci; -- ERROR
SELECT _utf8mb4'高斯DB' COLLATE utf8mb4_general_ci = _utf8mb4'高斯DB' COLLATE utf8mb4_unicode_ci; -- ERROR
SELECT _utf8mb4'高斯DB' COLLATE utf8mb4_bin = _utf8mb4'高斯DB' COLLATE "C"; -- ERROR
SELECT _utf8mb4'高斯DB' COLLATE utf8mb4_bin = _utf8mb4'高斯DB' COLLATE "zh_CN.utf8"; -- ERROR
SELECT _utf8mb4'高斯DB' COLLATE utf8mb4_bin = _utf8mb4'高斯DB' COLLATE "DEFAULT"; -- ERROR
SELECT _gbk'高斯DB' COLLATE gbk_bin = '高斯DB' COLLATE utf8mb4_unicode_ci; -- ERROR
SELECT _gbk'高斯DB' COLLATE gbk_bin = '高斯DB' COLLATE utf8mb4_bin;
SELECT _gbk'高斯db' COLLATE gbk_chinese_ci = '高斯DB' COLLATE "C"; -- ERROR
SELECT _gbk'高斯db' COLLATE gbk_chinese_ci = '高斯DB' COLLATE "zh_CN.gbk"; -- ERROR
SELECT _gbk'高斯db' COLLATE gbk_bin = _gbk'高斯DB' COLLATE gbk_chinese_ci; -- ERROR
SELECT _gb18030'高斯db' COLLATE gb18030_bin = _gbk'高斯DB' COLLATE gbk_chinese_ci; -- ERROR
-- -- -- same charset & implicit collation
SELECT _gbk'高斯DB' = '高斯DB';
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
SELECT _utf8mb4'高斯DB' = '高斯DB';
SELECT _gbk'高斯DB' = _utf8mb4'高斯DB';
SELECT _gbk'高斯DB' = _binary'高斯DB'; -- not support yet
SELECT _gbk'高斯DB' = _gb18030'高斯DB'; -- ERROR
SELECT _gb18030'高斯DB' = _utf8mb4'高斯DB';
SELECT _gb18030'高斯DB' = _binary'高斯DB'; -- not support yet
SELECT '高斯DB' = _utf8mb4'高斯DB';
SELECT '高斯DB' = _gb18030'高斯DB'; -- ERROR
SELECT _binary'高斯DB' = _utf8mb4'高斯DB'; -- not support yet
SELECT _binary'高斯DB' = _gbk'高斯DB'; -- not support yet
SELECT _binary'高斯DB' = _gb18030'高斯DB'; -- not support yet
-- -- -- explicit & implicit
SELECT _utf8mb4'高斯DB' = '高斯DB' COLLATE gbk_chinese_ci;
SELECT _utf8mb4'高斯DB' = '高斯DB' COLLATE gbk_bin;
SELECT _utf8mb4'高斯DB' = '高斯DB' COLLATE "C";
SELECT _utf8mb4'高斯DB' = '高斯DB' COLLATE "zh_CN.gbk";
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
SELECT CONCAT(_gbk'高斯DB' COLLATE gbk_chinese_ci , '高斯DB' COLLATE "C"); -- ERROR
SELECT CONCAT(_gbk'高斯DB' COLLATE gbk_chinese_ci , '高斯DB' COLLATE "zh_CN.gbk"); -- ERROR
SELECT CONCAT(_gbk'高斯DB' COLLATE gbk_chinese_ci , '高斯DB' COLLATE gbk_chinese_ci);
SELECT CONCAT(_gbk'高斯DB' COLLATE gbk_chinese_ci , '高斯DB' COLLATE gbk_bin); -- ERROR
SELECT CONCAT(_gbk'高斯DB' COLLATE gbk_bin , '高斯DB' COLLATE gbk_chinese_ci); -- ERROR
SELECT CONCAT(_gbk'高斯DB' COLLATE gbk_bin , '高斯DB' COLLATE gbk_bin);
-- -- -- same charset & implicit collation
SELECT CONCAT(_gbk'高斯DB' , '高斯DB') result, collation for(result);
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
SELECT CONCAT(_utf8mb4'高斯DB' , '高斯DB') result, collation for(result);
SELECT CONCAT(_gbk'高斯DB' , _binary'高斯DB') result, collation for(result);
SELECT CONCAT(_gbk'高斯DB' , _gb18030'高斯DB') result, collation for(result); -- ERROR
SELECT CONCAT(_gb18030'高斯DB' , '高斯DB') result, collation for(result); -- ERROR
SELECT CONCAT(_gb18030'高斯DB' , _binary'高斯DB') result, collation for(result);
SELECT CONCAT( _binary'高斯DB', _utf8mb4'高斯DB') result, collation for(result);
SELECT CONCAT( _binary'高斯DB', '高斯DB') result, collation for(result);
-- -- -- explicit & implicit
SELECT CONCAT(_utf8mb4'高斯DB' , '高斯DB' COLLATE "C") result, collation for(result);
SELECT CONCAT(_utf8mb4'高斯DB' , '高斯DB' COLLATE "zh_CN.gbk") result, collation for(result);
SELECT CONCAT(_utf8mb4'高斯DB' , '高斯DB' COLLATE gbk_chinese_ci) result, collation for(result);
SELECT CONCAT(_utf8mb4'高斯DB' , '高斯DB' COLLATE gbk_bin) result, collation for(result);
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
SELECT CONCAT(_utf8mb4'高斯DB', opengauss_version()) result, collation for(result);
SELECT CONCAT(opengauss_version(), _utf8mb4'高斯DB') result, collation for(result);
SELECT CONCAT(_utf8mb4'高斯DB', 123) result, collation for(result);
SELECT CONCAT(123, _utf8mb4'高斯DB') result, collation for(result);
SELECT CONCAT(_utf8mb4'高斯DB', DATE '2023-05-01') result, collation for(result);
SELECT CONCAT(DATE '2023-05-01', _utf8mb4'高斯DB') result, collation for(result);
SELECT CONCAT(_utf8mb4'高斯DB', NULL) result, collation for(result);
SELECT CONCAT(NULL, _utf8mb4'高斯DB') result, collation for(result);
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
SELECT CONCAT(CONCAT(_utf8mb4'高斯DB'), opengauss_version()) result, collation for(result);
SELECT CONCAT(opengauss_version(), CONCAT(_utf8mb4'高斯DB')) result, collation for(result);
SELECT CONCAT(CONCAT(_utf8mb4'高斯DB'), 123) result, collation for(result);
SELECT CONCAT(123, CONCAT(_utf8mb4'高斯DB')) result, collation for(result);
SELECT CONCAT(CONCAT(_utf8mb4'高斯DB'), DATE '2023-05-01') result, collation for(result);
SELECT CONCAT(DATE '2023-05-01', CONCAT(_utf8mb4'高斯DB')) result, collation for(result);
SELECT CONCAT(CONCAT(_utf8mb4'高斯DB'), NULL) result, collation for(result);
SELECT CONCAT(NULL, CONCAT(_utf8mb4'高斯DB')) result, collation for(result);
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
SELECT futf8_bin COLLATE "zh_CN.utf8" FROM t_diff_charset_columns; -- ERROR
SELECT futf8_bin COLLATE "C" FROM t_diff_charset_columns; -- ERROR
SELECT fgbk_bin COLLATE gbk_chinese_ci FROM t_diff_charset_columns;
SELECT fgbk_bin COLLATE 'binary' FROM t_diff_charset_columns; -- ERROR
SELECT fgbk_bin COLLATE "zh_CN.gbk" FROM t_diff_charset_columns; -- support origin collation
SELECT fgbk_bin COLLATE "C" FROM t_diff_charset_columns; -- support origin collation
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
SELECT (futf8_uni COLLATE utf8mb4_bin) COLLATE "zh_CN.utf8" FROM t_diff_charset_columns; -- ERROR
SELECT (futf8_uni COLLATE utf8mb4_bin) COLLATE "C" FROM t_diff_charset_columns; -- ERROR
SELECT (futf8_uni COLLATE utf8mb4_bin) COLLATE utf8mb4_unicode_ci FROM t_diff_charset_columns;
SELECT (fgbk_chi COLLATE "C") COLLATE utf8mb4_bin FROM t_diff_charset_columns; -- ERROR
SELECT (fgbk_chi COLLATE "C") COLLATE gbk_chinese_ci FROM t_diff_charset_columns;
SELECT (fblob COLLATE "binary") COLLATE utf8mb4_bin FROM t_diff_charset_columns; -- ERROR
SELECT fbit COLLATE 'binary' FROM t_diff_charset_columns; -- ERROR

-- -- COLUMN compare COLUMN
-- -- -- same charset & implicit collation
SELECT futf8_bin = futf8_uni, futf8_uni = futf8_bin FROM t_diff_charset_columns;
SELECT fgbk_bin = fgbk_chi, fgbk_chi = fgbk_bin FROM t_diff_charset_columns;
SELECT fgb18030_bin = fgb18030_chi, fgb18030_chi = fgb18030_bin FROM t_diff_charset_columns;
SELECT futf8_gen = futf8_uni, futf8_uni = futf8_gen FROM t_diff_charset_columns; -- ERROR
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
set @var_utf8_gen = _utf8mb4'高斯DB' COLLATE utf8mb4_general_ci; -- should support
SELECT collation for(@var_utf8_gen);
SELECT CONCAT(futf8_uni, @var_utf8_gen) result, collation for(result) FROM t_diff_charset_columns; -- null collation
SELECT CONCAT(fgbk_bin, @var_utf8_gen) result, collation for(result) FROM t_diff_charset_columns; -- utf8mb4_general_ci
SELECT CONCAT(@var_utf8_gen, futf8_bin) result, collation for(result) FROM t_diff_charset_columns; -- _bin
-- -- -- string var gbk_chinese_ci
set @var_gbk_chi = '高斯DB' COLLATE gbk_chinese_ci; -- should support
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
SELECT CONCAT($1 collate gbk_chinese_ci, futf8_bin) result, collation for(result) FROM t_diff_charset_columns;
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
SELECT CONCAT($1 collate utf8mb4_unicode_ci, futf8_bin) result, collation for(result) FROM t_diff_charset_columns; -- $1 use collation_connection, ERROR
DEALLOCATE test_merge_collation;
-- -- -- -- test revalidate
SELECT fgbk_chi result FROM t_diff_charset_columns WHERE fgbk_chi=_gbk'高斯db'; -- 1 rows
PREPARE test_revalidate(text) AS
SELECT fgbk_chi result FROM t_diff_charset_columns WHERE fgbk_chi=$1;
EXECUTE test_revalidate(_gbk'高斯db'); -- fgbk_chi collation has priority, 1 rows
ALTER INDEX idx_prefixkey_futf8_bin UNUSABLE;
EXECUTE test_revalidate(_gbk'高斯db'); -- fgbk_chi collation has priority, 1 rows
SET NAMES utf8mb4 COLLATE utf8mb4_bin;
EXECUTE test_revalidate(_gbk'高斯db'); -- fgbk_chi collation has priority, 1 rows
ALTER INDEX idx_prefixkey_futf8_bin REBUILD;
EXECUTE test_revalidate(_gbk'高斯db'); -- fgbk_chi collation has priority, 1 rows
SET NAMES gbk COLLATE gbk_bin;
EXECUTE test_revalidate(_gbk'高斯db'); -- fgbk_chi collation has priority, 1 rows
ALTER INDEX idx_prefixkey_futf8_bin REBUILD;
EXECUTE test_revalidate(_gbk'高斯db'); -- fgbk_chi collation has priority, 1 rows
DEALLOCATE test_revalidate;
SET NAMES gbk;


-- -- concat for DERIVATION
-- -- -- same charset & diff DERIVATION
SELECT CONCAT(CONCAT(futf8_gen, futf8_uni), futf8_bin) result, collation for(result) FROM t_diff_charset_columns; -- conflict in concat inside
SELECT CONCAT(futf8_bin, CONCAT(futf8_gen, futf8_uni)) result, collation for(result) FROM t_diff_charset_columns; -- conflict in concat inside
SELECT CONCAT(CONCAT(futf8_gen, futf8_uni), futf8_bin collate utf8mb4_unicode_ci) result, collation for(result) FROM t_diff_charset_columns; -- conflict in concat inside
SELECT CONCAT(futf8_bin collate utf8mb4_unicode_ci, CONCAT(futf8_gen, futf8_uni)) result, collation for(result) FROM t_diff_charset_columns; -- conflict in concat inside
SELECT CONCAT(CONCAT(futf8_gen), futf8_uni) result, collation for(result) FROM t_diff_charset_columns; -- conflict
SELECT CONCAT(futf8_uni, CONCAT(futf8_gen)) result, collation for(result) FROM t_diff_charset_columns; -- conflict
SELECT CONCAT(futf8_uni, opengauss_version()) result, collation for(result) FROM t_diff_charset_columns;
SELECT CONCAT(opengauss_version(), futf8_uni) result, collation for(result) FROM t_diff_charset_columns;
SELECT CONCAT(fgbk_chi, '高斯DB') result, collation for(result) FROM t_diff_charset_columns;
SELECT CONCAT('高斯DB', fgbk_chi) result, collation for(result) FROM t_diff_charset_columns;
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
SELECT CONCAT(CONCAT(futf8_gen, futf8_uni), fgbk_chi COLLATE gbk_chinese_ci) result, collation for(result) FROM t_diff_charset_columns; -- conflict in concat inside
SELECT CONCAT(fgbk_chi COLLATE gbk_chinese_ci, CONCAT(futf8_gen, futf8_uni)) result, collation for(result) FROM t_diff_charset_columns; -- conflict in concat inside
SELECT CONCAT(fgbk_chi, CONCAT(fgb18030_chi)) result, collation for(result) FROM t_diff_charset_columns;
SELECT CONCAT(CONCAT(fgb18030_bin), fgbk_bin) result, collation for(result) FROM t_diff_charset_columns;
SELECT CONCAT(fgbk_chi, opengauss_version()) result, collation for(result) FROM t_diff_charset_columns;
SELECT CONCAT(opengauss_version(), fgbk_bin) result, collation for(result) FROM t_diff_charset_columns;
SELECT CONCAT(futf8_uni, '高斯DB') result, collation for(result) FROM t_diff_charset_columns;
SELECT CONCAT('高斯DB', futf8_uni) result, collation for(result) FROM t_diff_charset_columns;
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
SELECT fgbk_bin FROM t_diff_charset_columns WHERE fgbk_bin in (_gb18030'高斯DB', '高斯DB'); -- ERROR
EXPLAIN (costs off)
SELECT fgbk_bin FROM t_diff_charset_columns WHERE fgbk_bin in (_gb18030'高斯DB', '高斯DB' COLLATE gbk_bin);
SELECT fgbk_bin FROM t_diff_charset_columns WHERE fgbk_bin in (_gb18030'高斯DB', '高斯DB' COLLATE gbk_bin);
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

-- -- XMLEXPR
SELECT xmlelement(NAME a, fgbk_chi, futf8_bin) result FROM t_diff_charset_columns;
SELECT xmlelement(NAME a, futf8_gen, futf8_bin) result FROM t_diff_charset_columns;
SELECT xmlelement(NAME a, futf8_uni, futf8_gen) result FROM t_diff_charset_columns; -- conflict
SELECT xmlelement(NAME a, fgbk_chi, fgb18030_chi) result FROM t_diff_charset_columns; -- ERROR
SELECT xmlconcat(xmlelement(NAME a, fgbk_chi, futf8_bin), xmlelement(NAME b, fgb18030_chi)) result FROM t_diff_charset_columns;

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

DROP TABLE t_diff_charset_columns;

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
DROP TABLE IF EXISTS t_charset_gbk;

set enable_expr_fusion = OFF;
-- ------------------------------------------
-- SET NAMES utf8mb4;
SET NAMES utf8mb4;
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
SELECT '楂樻柉';
SELECT CONVERT_TO(_utf8mb4'楂樻柉', 'gbk'); -- ERROR
SELECT CONVERT_TO(_utf8mb4'高斯', 'gbk'); -- ERROR

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