CREATE TABLE heap_copytuple_s (rf_a SERIAL PRIMARY KEY,
	b INT);

CREATE TABLE heap_copytuple (a SERIAL PRIMARY KEY,
	b INT,
	c TEXT,
	d TEXT
	);

CREATE INDEX heap_copytuple_b ON heap_copytuple (b);
CREATE INDEX heap_copytuple_c ON heap_copytuple (c);
CREATE INDEX heap_copytuple_c_b ON heap_copytuple (c,b);
CREATE INDEX heap_copytuple_b_c ON heap_copytuple (b,c);

INSERT INTO heap_copytuple_s (b) VALUES (0);
INSERT INTO heap_copytuple_s (b) SELECT b FROM heap_copytuple_s;
INSERT INTO heap_copytuple_s (b) SELECT b FROM heap_copytuple_s;
INSERT INTO heap_copytuple_s (b) SELECT b FROM heap_copytuple_s;
INSERT INTO heap_copytuple_s (b) SELECT b FROM heap_copytuple_s;
INSERT INTO heap_copytuple_s (b) SELECT b FROM heap_copytuple_s;
drop table heap_copytuple_s cascade;
INSERT INTO heap_copytuple (b, c) VALUES (11, 'once');
INSERT INTO heap_copytuple (b, c) VALUES (10, 'diez');
INSERT INTO heap_copytuple (b, c) VALUES (31, 'treinta y uno');
INSERT INTO heap_copytuple (b, c) VALUES (22, 'veintidos');
INSERT INTO heap_copytuple (b, c) VALUES (3, 'tres');
INSERT INTO heap_copytuple (b, c) VALUES (20, 'veinte');
INSERT INTO heap_copytuple (b, c) VALUES (23, 'veintitres');
INSERT INTO heap_copytuple (b, c) VALUES (21, 'veintiuno');
INSERT INTO heap_copytuple (b, c) VALUES (4, 'cuatro');
INSERT INTO heap_copytuple (b, c) VALUES (14, 'catorce');
INSERT INTO heap_copytuple (b, c) VALUES (2, 'dos');
INSERT INTO heap_copytuple (b, c) VALUES (18, 'dieciocho');
INSERT INTO heap_copytuple (b, c) VALUES (27, 'veintisiete');
INSERT INTO heap_copytuple (b, c) VALUES (25, 'veinticinco');
INSERT INTO heap_copytuple (b, c) VALUES (13, 'trece');
INSERT INTO heap_copytuple (b, c) VALUES (28, 'veintiocho');
INSERT INTO heap_copytuple (b, c) VALUES (32, 'treinta y dos');
INSERT INTO heap_copytuple (b, c) VALUES (5, 'cinco');
INSERT INTO heap_copytuple (b, c) VALUES (29, 'veintinueve');
INSERT INTO heap_copytuple (b, c) VALUES (1, 'uno');
INSERT INTO heap_copytuple (b, c) VALUES (24, 'veinticuatro');
INSERT INTO heap_copytuple (b, c) VALUES (30, 'treinta');
INSERT INTO heap_copytuple (b, c) VALUES (12, 'doce');
INSERT INTO heap_copytuple (b, c) VALUES (17, 'diecisiete');
INSERT INTO heap_copytuple (b, c) VALUES (9, 'nueve');
INSERT INTO heap_copytuple (b, c) VALUES (19, 'diecinueve');
INSERT INTO heap_copytuple (b, c) VALUES (26, 'veintiseis');
INSERT INTO heap_copytuple (b, c) VALUES (15, 'quince');
INSERT INTO heap_copytuple (b, c) VALUES (7, 'siete');
INSERT INTO heap_copytuple (b, c) VALUES (16, 'dieciseis');
INSERT INTO heap_copytuple (b, c) VALUES (8, 'ocho');
-- This entry is needed to test that TOASTED values are copied correctly.
INSERT INTO heap_copytuple (b, c, d) VALUES (6, 'seis', repeat('xyzzy', 100000));

CLUSTER heap_copytuple_c ON heap_copytuple;
INSERT INTO heap_copytuple (b, c) VALUES (1111, 'this should fail');
ALTER TABLE heap_copytuple CLUSTER ON heap_copytuple_b_c;

-- Try turning off all clustering
ALTER TABLE heap_copytuple SET WITHOUT CLUSTER;
drop table heap_copytuple cascade;





create table tGin122 (
        name varchar(50) not null, 
        age int, 
        birth date, 
        ID varchar(50) , 
        phone varchar(15),
        carNum varchar(50),
        email varchar(50), 
        info text, 
        config varchar(50) default 'english',
        tv tsvector,
        i varchar(50)[],
        ts tsquery);
insert into tGin122 values('Linda', 20, '1996-06-01', '140110199606012076', '13454333333', '京A QL666', 'linda20@sohu.com', 'When he was busy with teaching men the art of living, Prometheus had left a bigcask in the care of Epimetheus. He had warned his brother not to open the lid. Pandora was a curious woman. She had been feeling very disappointed that her husband did not allow her to take a look at the contents of the cask. One day, when Epimetheus was out, she lifted the lid and out it came unrest and war, Plague and sickness, theft and violence, grief, sorrow, and all the other evils. The human world was hence to experience these evils. Only hope stayed within the mouth of the jar and never flew out. So men always have hope within their hearts.
偷窃天火之后，宙斯对人类的敌意与日俱增。一天，他令儿子赫菲斯托斯用泥塑一美女像，并请众神赠予她不同的礼物。世上的第一个女人是位迷人女郎，因为她从每位神灵那里得到了一样对男人有害的礼物，因此宙斯称她为潘多拉。
', 'ngram', '', '{''brother'',''与日俱增'',''赫菲斯托斯''}',NULL);
insert into tGin122 values('张三', 20,  '1996-07-01', '140110199607012076', '13514333333', '鲁K QL662', 'zhangsan@163.com', '希腊北部国王阿塔玛斯有两个孩子，法瑞克斯和赫勒。当国王离
开第一个妻子和一个名叫伊诺的坏女人结婚后，两个孩子受到后母残忍虐待，整个王国也受到毁灭性瘟疫的侵袭。伊诺在爱轻信的丈夫耳边进谗言，终于使国王相信：他的儿子法瑞克斯是这次灾害的罪魁祸首，并要将他献给宙斯以结束
瘟疫。可怜的孩子被推上了祭坛，将要被处死。正在此时，上帝派了一只浑身上下长着金色羊毛的公羊来将两个孩子驮在背上带走了。当他们飞过隔开欧洲和亚洲的海峡时，赫勒由于看到浩瀚的海洋而头晕目眩，最终掉进大海淹死了。
这片海洋古时候的名称叫赫勒之海，赫勒拉旁海峡便由此而来。金色公羊驮着法瑞克斯继续向前飞去，来到了黑海东岸的科尔契斯。在那里，法瑞克斯将公羊献给了宙斯；而将金羊毛送给了埃厄忒斯国王。国王将羊毛钉在一棵圣树上，
并派了一条不睡觉的龙负责看护。', 'ngram', '',  '{''法瑞克斯和赫勒'',''王国'',''埃厄忒斯国王''}',NULL); 
insert into tGin122 values('Sara', 20,  '1996-07-02', '140110199607022076', '13754333333', '冀A QL661', 'sara20@sohu.com', '英语语言结构重形合（hypotaxis），汉语重义合（parataxis）>，也就是说，英语的句子组织通常通过连接词（connectives）和词尾的曲折变化（inflection）来实现，汉语则较少使用连接词和受语法规则约束。英语句子通过表示各种关系如因果、条件、逻辑、预设等形合手段组织，环环相扣，>可以形成像树枝一样包孕许多修饰成分和分句的长句和复杂句，而汉语则多用短句和简单句。此外，英语注重使用各种短语作为句子的构成单位，在修饰位置上可前可后、十分灵活，常习惯于后置语序。这些差异就形成了王力先生所谓
的英语“化零为整”而汉语则“化整为零”特点。此外，英语多用被动语态，这在科技英语中尤为如此。了解英语和汉语这些造句差异，就可在英语长句和复杂句的理解和翻译中有意识地将英语句子按照汉语造句特点进行转化处理，短从句结构变单独句子或相反，后置变前置，被动变主动。以下结合本人在教学中遇到的例子，说说如何对生物类专业英语长句和复杂句翻译进行翻译处理。', 'english', '',  '{''parataxis'',''后置变前置'',''差异''}',NULL);
insert into tGin122 values('Mira', 20,  '1996-08-01', '140110199608012076', '13654333333', '津A QL660', 'mm20@sohu.com', '[解析]第一个分句宜将被动语态译为主动语态，第二个分句如将定>语分句处理为汉语前置，“利用能在培养组织中迅速降解而无需提供第二种生根培养基的IAA则是克服这个问题的一种有用方法。”则会因修饰语太长，不易理解，也不符合汉语习惯，宜作为分句处理。[翻译]根发端所需的生长素水平抑制根的伸长，而利用IAA则是克服这个问题的一种有用方法，因为IAA能在培养组织中迅速降解而无需提供第二种生根培养基。', 'english', '',  '{''汉语前置'',''分句处理'',''生长素水平''}',NULL);
insert into tGin122 values('Amy', 20,  ' 1996-09-01', '140110199609012076', '13854333333', '吉A QL663', 'amy2008@163.com', '[解析]该句的理解的关键是要抓住主句的结构“Current concern focus on ……, and on……”，同时不要将第二个“on”的搭配（intrusionon）与主句中第一个和第三个“on”的搭配（focuson）混淆。翻译时，为了避免宾语的修补词过长，可用“目前公众对转基因植物的关注集中在这两点”来用“一方面……；另一方面……”来分述，这样处理更符合汉语习惯。', 'ngram', '',  '{''intrusionon'',''13854333333'',''140110199609012076''}',NULL);
insert into tGin122 values('汪玲沁 ', 20,  ' 1996-09-01', '44088319921103106X', '13854333333', '沈YWZJW0', 'si2008@163.com', '晨的美好就如青草般芳香，如河溪般清澈，如玻璃般透明，如>甘露般香甜。[解析]该句的主句结构为“This led to a whole new field of academic research”，后面有一个现在分词结构“including the milestone paper by Paterson and co-workers in 1988”之后为“the milestone pape长定语从句。在翻译时，宜将该定语从句分译成句，但要将表示方法手段的现在分词结构“using an approach that could be applied to dissect the genetic make-up of any physiological, morphological and behavioural trat in plants and animals”前置译出，这样更符合汉语的表达习惯。', 'ngram', '',  '{''44088319921103106X'',''分词结构'',''透明''}',NULL);
create index tgin122_idx1 on tgin122 (substr(email,2,5));
create index tgin122_idx2 on tgin122 (upper(info));
set default_statistics_target=-2;
analyze tGin122 ((tv, ts));
select * from pg_ext_stats where schemaname='distribute_stat_2' and tablename='tgin122' order by attname;
alter table tGin122 delete statistics ((tv, ts));
update tGin122 set tv=to_tsvector(config::regconfig, coalesce(name,'') || ' ' || coalesce(ID,'') || ' ' || coalesce(carNum,'') || ' ' || coalesce(phone,'') || ' ' || coalesce(email,'') || ' ' || coalesce(info,''));
update tGin122 set ts=to_tsquery('ngram', coalesce(phone,'')); 
analyze tGin122 ((tv, ts));
select * from pg_ext_stats where schemaname='distribute_stat_2' and tablename='tgin122' order by attname;
alter table tGin122 delete statistics ((tv, ts));
select * from pg_ext_stats where schemaname='distribute_stat_2' and tablename='tgin122' order by attname;
alter table tGin122 add statistics ((tv, ts));
analyze tGin122;
select * from pg_ext_stats where schemaname='distribute_stat_2' and tablename='tgin122' order by attname;
select * from pg_stats where tablename='tgin122' and attname = 'tv';
select attname,avg_width,n_distinct,histogram_bounds from pg_stats where tablename='tgin122_idx1';
drop table tgin122 cascade;








CREATE SCHEMA regress_rls_schema;
GRANT CREATE ON SCHEMA regress_rls_schema to public;
GRANT USAGE ON SCHEMA regress_rls_schema to public;
-- reconnect
\c
SET search_path = regress_rls_schema;
	
CREATE TABLE regress_rls_schema.document_row(
    did     int primary key,
    cid     int,
    dlevel  int not null,
    dauthor name,
    dtitle  text
);
GRANT ALL ON regress_rls_schema.document_row TO public;
INSERT INTO regress_rls_schema.document_row VALUES
    ( 1, 11, 1, 'regress_rls_bob', 'my first novel'),
    ( 2, 11, 5, 'regress_rls_bob', 'my second novel'),
    ( 3, 22, 7, 'regress_rls_bob', 'my science fiction'),
    ( 4, 44, 9, 'regress_rls_bob', 'my first manga'),
    ( 5, 44, 3, 'regress_rls_bob', 'my second manga'),
    ( 6, 22, 2, 'regress_rls_peter', 'great science fiction'),
    ( 7, 33, 6, 'regress_rls_peter', 'great technology book'),
    ( 8, 44, 4, 'regress_rls_peter', 'great manga'),
    ( 9, 22, 5, 'regress_rls_david', 'awesome science fiction'),
    (10, 33, 4, 'regress_rls_david', 'awesome technology book'),
    (11, 55, 8, 'regress_rls_alice', 'great biography'),
    (12, 33, 10, 'regress_rls_admin', 'physical technology'),
    (13, 55, 5, 'regress_rls_single_user', 'Beethoven biography');
ANALYZE regress_rls_schema.document_row;
UPDATE document_row SET dlevel = dlevel + 1 - 1 WHERE did > 1;
INSERT INTO document_row VALUES (100, 49, 1, 'regress_rls_david', 'testing sorting of policies');
DELETE FROM document_row WHERE did = 100;
INSERT INTO document_row VALUES (100, 49, 1, 'regress_rls_david', 'testing sorting of policies');
DELETE FROM document_row WHERE did = 100 RETURNING dauthor, did;

CREATE TABLE regress_rls_schema.account_row(
    aid   int,
    aname varchar(100)
) WITH (ORIENTATION=row);
CREATE ROW LEVEL SECURITY POLICY p01 ON document_row AS PERMISSIVE
    USING (dlevel <= (SELECT aid FROM account_row WHERE aname = current_user));

ALTER POLICY p01 ON document_row USING (dauthor = current_user);
ALTER POLICY p01 ON document_row RENAME TO p12;
ALTER POLICY p12 ON document_row RENAME TO p13;
ALTER POLICY p13 ON document_row RENAME TO p01;
SELECT * FROM pg_rlspolicies ORDER BY tablename, policyname;
drop schema regress_rls_schema cascade;
reset search_path;



















CREATE TABLE y (a INTEGER PRIMARY KEY INITIALLY DEFERRED)  ;
INSERT INTO y SELECT generate_series(1, 10);
TRUNCATE TABLE y;
INSERT INTO y SELECT generate_series(1, 10);

CREATE FUNCTION y_trigger() RETURNS trigger AS $$
begin
  raise notice 'y_trigger: a = %', new.a;
  return new;
end;
$$ LANGUAGE plpgsql;

CREATE TRIGGER y_trig BEFORE INSERT ON y FOR EACH ROW
    EXECUTE PROCEDURE y_trigger();

WITH t AS (
    INSERT INTO y
    VALUES
        (21),
        (22),
        (23)
    RETURNING *
)
SELECT * FROM t;

drop table y cascade;
drop function y_trigger;

