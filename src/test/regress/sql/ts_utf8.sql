CREATE DATABASE db_utf8 TEMPLATE template0 encoding 'UTF8';
\c db_utf8

------------------------------ basic synax for text search configuration -------------------------------
--
--- Pound
--
-- CREATE TEXT SEARCH CONFIGURATION
create text search configuration pound_utf8(parser=pound) with (split_flag = '#');
ALTER TEXT SEARCH CONFIGURATION pound_utf8 ADD MAPPING FOR zh_words, en_word, numeric, alnum, grapsymbol, multisymbol WITH simple;

select cfgname, cfoptions from pg_ts_config where cfgname = 'pound_utf8';

SELECT to_tsvector('pound_utf8', '京东#淘宝#滴滴#爱奇艺#芒果TV');

-- SET THE SPLIT FLAG to '@'
alter text search configuration pound_utf8 set (split_flag = '@');
select cfgname, cfoptions from pg_ts_config where cfgname = 'pound_utf8';
SELECT to_tsvector('pound_utf8', '京东@淘宝@滴滴@爱奇艺@芒果TV');

-- SET THE SPLIT FLAG to '$'
alter text search configuration pound_utf8 set (split_flag = '$');
select cfgname, cfoptions from pg_ts_config where cfgname = 'pound_utf8';
SELECT to_tsvector('pound_utf8', '京东$淘宝$滴滴$爱奇艺$芒果TV');

-- SET THE SPLIT FLAG to '%'
alter text search configuration pound_utf8 set (split_flag = '%');
select cfgname, cfoptions from pg_ts_config where cfgname = 'pound_utf8';
SELECT to_tsvector('pound_utf8', '京东%淘宝%滴滴%爱奇艺%芒果TV');

-- SET THE SPLIT FLAG to '/'
alter text search configuration pound_utf8 set (split_flag = '/');
select cfgname, cfoptions from pg_ts_config where cfgname = 'pound_utf8';
SELECT to_tsvector('pound_utf8', '京东/淘宝/滴滴/爱奇艺/芒果TV');

-- FOR UNSUPPORTED SPLPT_FLAG, REPORT ERROR
alter text search configuration pound_utf8 set (split_flag = ',');

-- WHEN SPLIT FLAG IS NOT SINGLE CHARACTER, REPORT ERROR
alter text search configuration pound_utf8 set (split_flag = '#@');

-- WHEN SPLIT FLAG IS NULL CHARACTER, REPORT ERROR
alter text search configuration pound_utf8 set (split_flag = '');

-- WHEN SINGLE TOKEN EXCEED 256 CHARACTERS, SPLIT DIRECTLY EVEN NO SPLIT FLAG IS FOUND
select to_tsvector('pound_utf8','1981年8月26日，美国宇宙飞船“旅行者2号”飞过土星，取得了一系列探测成果，其中包括发现了土星的第17颗卫星——土卫17。所以，今天我们就来说一说“旅行者2号”上的“地球之音”唱片。1977年8月和9月，人类成功发射了“旅行者1号”和“旅行者2号”探测器，再次向外星人作了更详细的自我介绍。这次，它们各自携带了一张称为“地球之音”的唱片，上面录制了丰富的地球信息。这两张唱片都是镀金铜质的，直径为30.5厘米。唱片上录有115幅照片和图表，35种各类声音，近60种语言的问候语和27首世界著名乐曲等。115幅照片中包括我国八达岭长城');
select to_tsvector('pound_utf8','1981年8月26日，美国宇宙飞船“旅行者2号”飞过土星，取得了一系列探测成果，其中包括发现了土星的第17颗卫星——土卫17。所以，今天我们就来说一说“旅行者2号”上的“地球之音”唱片。1977年8月和9月，人类成功发射了“旅行者1号”和“旅行者2号”探测器，再次向外星人作了更详细的自我介绍。这次，它们各自携带了一张称为“地球之音”的唱片，上面录制了丰富的地球信息。这两张唱片都是镀金铜质的，直径为30.5厘米。唱片上录有115幅照片和图表，35种各类声音，近60种语言的问候语和27首世界著名乐曲等。115幅///照片中包括我国八达岭长城');

drop text search configuration pound_utf8;
create text search configuration pound_utf8(parser=pound) with (split_flag = '#');
ALTER TEXT SEARCH CONFIGURATION pound_utf8 ADD MAPPING FOR zh_words, en_word, numeric, alnum, grapsymbol, multisymbol WITH simple;

select cfgname, cfoptions from pg_ts_config where cfgname = 'pound_utf8';

SELECT to_tsvector('pound_utf8', '京东#淘宝#滴滴#爱奇艺#芒果TV');

--
--- N-gram
--
-- CREATE TEXT SEARCH CONFIGURATION
create text search configuration ngram1(parser=ngram) with (punctuation_ignore = on, gram_size = 2, grapsymbol_ignore = on);
alter text search configuration ngram1 ADD MAPPING FOR zh_words, en_word, numeric, alnum, grapsymbol, multisymbol with simple;

select cfgname, cfoptions from pg_ts_config where cfgname = 'ngram1';

SELECT to_tsvector('ngram1', '画,龙&点睛');

--punctuation_ignore
alter text search configuration ngram1 set (punctuation_ignore = off);
SELECT to_tsvector('ngram1', '画,龙&点睛');

--punctuation_ignore
alter text search configuration ngram1 set (grapsymbol_ignore = off);
SELECT to_tsvector('ngram1', '画,龙&点睛');

--punctuation_ignore
alter text search configuration ngram1 set (gram_size = 3);
SELECT to_tsvector('ngram1', '画,龙&点睛');

drop text search configuration ngram1;
create text search configuration ngram1(parser=ngram);
alter text search configuration ngram1 ADD MAPPING FOR  zh_words, en_word, numeric, alnum, grapsymbol, multisymbol with simple;
select cfgname, cfoptions from pg_ts_config where cfgname = 'ngram1';
SELECT to_tsvector('ngram1', '画,龙&点睛');
--------------------------- basic operation for text search parser ----------------------------------
--
---- N-gram parser
--
set ngram_gram_size = 2;
set ngram_grapsymbol_ignore = off;
set ngram_punctuation_ignore = off;

select ts_parse('ngram', 'Uber提供了电脑版、手机版&平板电脑等版本');

-- configuration parameter: ngram.punctuation_ignore
set ngram_punctuation_ignore = on;
select ts_parse('ngram', 'Uber提供了电脑版、手机版&平板电脑等版本');

-- configuration parameter: ngram.grapsymbol_ignore
set ngram_grapsymbol_ignore = on;
select ts_parse('ngram', 'Uber提供了电脑版、手机版&平板电脑等版本');

-- configuration parameter: ngram.grapsymbol_ignore
set ngram_gram_size = 4;
select ts_parse('ngram', 'Uber提供了电脑版、手机版&平板电脑等版本');

select ts_token_type('ngram');

------------------------------- basic operation for text search datatype -----------------------
--
--- pound
--
select to_tsvector('pound_utf8', 'Uber#滴滴#爱奇艺#芒果TV');
select to_tsquery('pound_utf8','芒果TV');

--
---- N-gram
--
select to_tsvector('ngram1','辽GQQ360'); 
select to_tsquery('ngram1','360');

------------------------------- basic operation for text search operator -----------------------
--
--- pound
--
select to_tsvector('pound_utf8','Uber#滴滴#爱奇艺#芒果TV') @@ to_tsquery('pound_utf8','芒果TV'); -- true
select to_tsvector('pound_utf8','Uber#滴滴#爱奇艺#芒果TV') @@@ to_tsquery('pound_utf8','芒果TV'); -- true

select to_tsvector('pound_utf8','芒果TV') @@ to_tsquery('pound_utf8','QQ'); -- false
select (to_tsvector('pound_utf8','滴滴') || to_tsvector('pound_utf8','爱奇艺')) @@ to_tsquery('pound_utf8','滴滴'); -- true

select to_tsquery('pound_utf8','淘宝') && to_tsquery('pound_utf8','爱奇艺');
select to_tsquery('pound_utf8','淘宝') || to_tsquery('pound_utf8','爱奇艺');

select to_tsvector('pound_utf8','Uber#滴滴#爱奇艺#芒果TV') @@ (to_tsquery('pound_utf8','淘宝')); -- false
select to_tsvector('pound_utf8','Uber#滴滴#爱奇艺#芒果TV') @@ (to_tsquery('pound_utf8','爱奇艺')); -- true
select to_tsvector('pound_utf8','Uber#滴滴#爱奇艺#芒果TV') @@ (to_tsquery('pound_utf8','淘宝') || to_tsquery('pound_utf8','爱奇艺')); -- true
select to_tsvector('pound_utf8','Uber#滴滴#爱奇艺#芒果TV') @@ (to_tsquery('pound_utf8','淘宝') && to_tsquery('pound_utf8','爱奇艺')); --false

select !!to_tsquery('pound_utf8','爱奇艺');
select to_tsvector('pound_utf8','Uber#滴滴#爱奇艺#芒果TV') @@ !!to_tsquery('pound_utf8','爱奇艺'); -- false
select to_tsvector('pound_utf8','Uber#滴滴#爱奇艺#芒果TV') @@ !!to_tsquery('pound_utf8','淘宝'); --true

select to_tsquery('pound_utf8','爱奇艺') @> to_tsquery('pound_utf8','Uber#滴滴#爱奇艺#芒果TV'); -- false
select to_tsquery('pound_utf8','Uber#滴滴#爱奇艺#芒果TV') @> to_tsquery('pound_utf8','爱奇艺'); -- true

select to_tsquery('pound_utf8','爱奇艺') <@ to_tsquery('pound_utf8','Uber#滴滴#爱奇艺#芒果TV'); -- true
select to_tsquery('pound_utf8','Uber#滴滴#爱奇艺#芒果TV') <@ to_tsquery('pound_utf8','爱奇艺'); -- false

--
---- N-gram
--
select to_tsvector('ngram1','辽GQQ360') @@ to_tsquery('ngram1','360'); -- true
select to_tsvector('ngram1','辽GQQ360') @@@ to_tsquery('ngram1','360'); -- true

select to_tsvector('ngram1','360') @@ to_tsquery('ngram1','QQ'); -- false
select (to_tsvector('ngram1','辽GQQ') || to_tsvector('ngram1','360')) @@ to_tsquery('ngram1','QQ'); -- true

select to_tsquery('ngram1','480') && to_tsquery('ngram1','辽GQQ');
select to_tsquery('ngram1','480') || to_tsquery('ngram1','辽GQQ');

select to_tsvector('ngram1','辽GQQ360') @@ (to_tsquery('ngram1','480')); -- false
select to_tsvector('ngram1','辽GQQ360') @@ (to_tsquery('ngram1','辽GQQ')); -- true
select to_tsvector('ngram1','辽GQQ360') @@ (to_tsquery('ngram1','480') || to_tsquery('ngram1','辽GQQ')); -- true
select to_tsvector('ngram1','辽GQQ360') @@ (to_tsquery('ngram1','480') && to_tsquery('ngram1','辽GQQ')); -- false

select !!to_tsquery('ngram1','360') ;
select to_tsvector('ngram1','辽GQQ360') @@ !!to_tsquery('ngram1','360'); -- false
select to_tsvector('ngram1','辽GQQ360') @@ !!to_tsquery('ngram1','480'); -- true

select to_tsquery('ngram1','360') @> to_tsquery('ngram1','辽GQQ360'); -- false
select to_tsquery('ngram1','辽GQQ360') @> to_tsquery('ngram1','360'); -- true

select to_tsquery('ngram1','360') <@ to_tsquery('ngram1','辽GQQ360'); -- true
select to_tsquery('ngram1','辽GQQ360') <@ to_tsquery('ngram1','360'); -- false

------------------------------- basic operation for text search function -----------------------
-- pound
select get_current_ts_config(); -- english
select length(to_tsvector('pound_utf8','Uber#滴滴#爱奇艺#芒果TV')); -- 4
select length(to_tsvector('pound_utf8','Uber#滴滴#爱奇艺#芒果TV#QQ')); -- 5

select numnode(to_tsquery('pound_utf8','Uber#滴滴#爱奇艺#芒果TV')); 
select numnode(to_tsquery('pound_utf8','芒果TV') || to_tsquery('pound_utf8','滴滴'));

-- select plainto_tsquery('pound_utf8', 'Uber##滴滴# 爱奇艺##芒果TV');
-- select plainto_tsquery('pound_utf8', 'Uber#滴滴#爱奇艺#芒果TV');

select querytree(to_tsquery('pound_utf8','Uber#滴滴#爱奇艺#芒果TV'));
select querytree(to_tsquery('pound_utf8','滴滴') && to_tsquery('pound_utf8','爱奇艺'));
select querytree(to_tsquery('pound_utf8','滴滴') || to_tsquery('pound_utf8','爱奇艺'));

select strip(to_tsvector('pound_utf8','Uber#滴滴#爱奇艺#芒果TV'));
select ts_headline('pound_utf8', 'Uber#滴滴#爱奇艺#芒果TV', to_tsquery('pound_utf8','Uber'));

select ts_rank(to_tsvector('pound_utf8', 'Uber#滴滴#爱奇艺#芒果TV'), to_tsquery('pound_utf8','Uber#滴滴#爱奇艺#芒果TV'));
select ts_rank(to_tsvector('pound_utf8', 'Uber#滴滴#爱奇艺#芒果TV'), to_tsquery('pound_utf8','Uber'));
select ts_rank(to_tsvector('pound_utf8', 'Uber#滴滴#爱奇艺#芒果TV'), to_tsquery('pound_utf8','滴滴'));

select ts_rank_cd(to_tsvector('pound_utf8', 'Uber#滴滴#爱奇艺#芒果TV'), to_tsquery('pound_utf8','Uber#滴滴#爱奇艺#芒果TV'));
select ts_rank_cd(to_tsvector('pound_utf8', 'Uber#滴滴#爱奇艺#芒果TV'), to_tsquery('pound_utf8','Uber'));
select ts_rank_cd(to_tsvector('pound_utf8', 'Uber#滴滴#爱奇艺#芒果TV'), to_tsquery('pound_utf8','滴滴'));

select ts_rewrite(to_tsquery('pound_utf8','Uber#滴滴#爱奇艺#芒果TV'), to_tsquery('pound_utf8','Uber'), to_tsquery('pound_utf8','爱奇艺'));

--N-gram
select get_current_ts_config(); -- english
select length(to_tsvector('ngram1','辽GQQ360')); -- 6
select length(to_tsvector('ngram1','辽GQQ888')); -- 5

select numnode(to_tsquery('ngram1','辽GQQ360')); -- 11
select numnode(to_tsquery('ngram1','480') || to_tsquery('ngram1','辽GQQ'));  -- 9

alter text search configuration ngram1 set (grapsymbol_ignore = off);
alter text search configuration ngram1 set (punctuation_ignore = on);
alter text search configuration ngram1 set (gram_size = 2);

select cfgname, cfoptions from pg_ts_config where cfgname = 'ngram1';

select plainto_tsquery('ngram1', '车，牌，号');
select plainto_tsquery('ngram1', '车,牌,号');

select querytree(to_tsquery('ngram1','中国人民银行'));
select querytree(to_tsquery('ngram1','中国人') && to_tsquery('ngram1','民银行'));
select querytree(to_tsquery('ngram1','中国人') || to_tsquery('ngram1','民银行'));

select strip(to_tsvector('ngram1','中国人民银行'));
select ts_headline('ngram1', '中国人民银行', to_tsquery('ngram1','中国人'));

select ts_rank(to_tsvector('ngram1', '中国人民银行'), to_tsquery('ngram1','中国人民银行'));
select ts_rank(to_tsvector('ngram1', '中国人民银行'), to_tsquery('ngram1','中国人'));
select ts_rank(to_tsvector('ngram1', '中国人民银行'), to_tsquery('ngram1','中人'));

select ts_rank_cd(to_tsvector('ngram1', '中国人民银行'), to_tsquery('ngram1','中国人民银行'));
select ts_rank_cd(to_tsvector('ngram1', '中国人民银行'), to_tsquery('ngram1','中国人'));
select ts_rank_cd(to_tsvector('ngram1', '中国人民银行'), to_tsquery('ngram1','中人'));

select ts_rewrite(to_tsquery('ngram1','中国人民银行'), to_tsquery('ngram1','银行'), to_tsquery('ngram1','政府'));

create table t1(a int);
drop table t1 cascade;

drop text search configuration ngram1;
drop text search configuration pound_utf8;
CREATE TEXT SEARCH CONFIGURATION ntestf(parser=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA);
CREATE TEXT SEARCH CONFIGURATION ntestf(parser=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA.B);
CREATE TEXT SEARCH CONFIGURATION ntestf(parser=B.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA);
CREATE TEXT SEARCH CONFIGURATION ntestf(parser=A.B.C);
CREATE TEXT SEARCH CONFIGURATION ntestf(parser='A.B.C');

\c postgres
DROP DATABASE db_utf8;
