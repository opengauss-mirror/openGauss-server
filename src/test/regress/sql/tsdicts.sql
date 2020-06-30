--Test text search dictionaries and configurations

-- Test ISpell dictionary with ispell affix file
CREATE TEXT SEARCH DICTIONARY ispell (
                        Template=ispell,
                        DictFile=ispell_sample,
                        AffFile=ispell_sample
);

SELECT ts_lexize('ispell', 'skies');
SELECT ts_lexize('ispell', 'bookings');
SELECT ts_lexize('ispell', 'booking');
SELECT ts_lexize('ispell', 'foot');
SELECT ts_lexize('ispell', 'foots');
SELECT ts_lexize('ispell', 'rebookings');
SELECT ts_lexize('ispell', 'rebooking');
SELECT ts_lexize('ispell', 'rebook');
SELECT ts_lexize('ispell', 'unbookings');
SELECT ts_lexize('ispell', 'unbooking');
SELECT ts_lexize('ispell', 'unbook');

SELECT ts_lexize('ispell', 'footklubber');
SELECT ts_lexize('ispell', 'footballklubber');
SELECT ts_lexize('ispell', 'ballyklubber');
SELECT ts_lexize('ispell', 'footballyklubber');

-- Test ISpell dictionary with hunspell affix file
CREATE TEXT SEARCH DICTIONARY hunspell (
                        Template=ispell,
                        DictFile=ispell_sample,
                        AffFile=hunspell_sample
);

SELECT ts_lexize('hunspell', 'skies');
SELECT ts_lexize('hunspell', 'bookings');
SELECT ts_lexize('hunspell', 'booking');
SELECT ts_lexize('hunspell', 'foot');
SELECT ts_lexize('hunspell', 'foots');
SELECT ts_lexize('hunspell', 'rebookings');
SELECT ts_lexize('hunspell', 'rebooking');
SELECT ts_lexize('hunspell', 'rebook');
SELECT ts_lexize('hunspell', 'unbookings');
SELECT ts_lexize('hunspell', 'unbooking');
SELECT ts_lexize('hunspell', 'unbook');

SELECT ts_lexize('hunspell', 'footklubber');
SELECT ts_lexize('hunspell', 'footballklubber');
SELECT ts_lexize('hunspell', 'ballyklubber');
SELECT ts_lexize('hunspell', 'footballyklubber');

-- Test ISpell dictionary with hunspell affix file with FLAG long parameter
CREATE TEXT SEARCH DICTIONARY hunspell_long (
                        Template=ispell,
                        DictFile=hunspell_sample_long,
                        AffFile=hunspell_sample_long
);

SELECT ts_lexize('hunspell_long', 'skies');
SELECT ts_lexize('hunspell_long', 'bookings');
SELECT ts_lexize('hunspell_long', 'booking');
SELECT ts_lexize('hunspell_long', 'foot');
SELECT ts_lexize('hunspell_long', 'foots');
SELECT ts_lexize('hunspell_long', 'rebookings');
SELECT ts_lexize('hunspell_long', 'rebooking');
SELECT ts_lexize('hunspell_long', 'rebook');
SELECT ts_lexize('hunspell_long', 'unbookings');
SELECT ts_lexize('hunspell_long', 'unbooking');
SELECT ts_lexize('hunspell_long', 'unbook');
SELECT ts_lexize('hunspell_long', 'booked');

SELECT ts_lexize('hunspell_long', 'footklubber');
SELECT ts_lexize('hunspell_long', 'footballklubber');
SELECT ts_lexize('hunspell_long', 'ballyklubber');
SELECT ts_lexize('hunspell_long', 'ballsklubber');
SELECT ts_lexize('hunspell_long', 'footballyklubber');
SELECT ts_lexize('hunspell_long', 'ex-machina');

-- Test ISpell dictionary with hunspell affix file with FLAG num parameter
CREATE TEXT SEARCH DICTIONARY hunspell_num (
                        Template=ispell,
                        DictFile=hunspell_sample_num,
                        AffFile=hunspell_sample_num
);

SELECT ts_lexize('hunspell_num', 'skies');
SELECT ts_lexize('hunspell_num', 'sk');
SELECT ts_lexize('hunspell_num', 'bookings');
SELECT ts_lexize('hunspell_num', 'booking');
SELECT ts_lexize('hunspell_num', 'foot');
SELECT ts_lexize('hunspell_num', 'foots');
SELECT ts_lexize('hunspell_num', 'rebookings');
SELECT ts_lexize('hunspell_num', 'rebooking');
SELECT ts_lexize('hunspell_num', 'rebook');
SELECT ts_lexize('hunspell_num', 'unbookings');
SELECT ts_lexize('hunspell_num', 'unbooking');
SELECT ts_lexize('hunspell_num', 'unbook');
SELECT ts_lexize('hunspell_num', 'booked');

SELECT ts_lexize('hunspell_num', 'footklubber');
SELECT ts_lexize('hunspell_num', 'footballklubber');
SELECT ts_lexize('hunspell_num', 'ballyklubber');
SELECT ts_lexize('hunspell_num', 'footballyklubber');

-- Synonim dictionary
CREATE TEXT SEARCH DICTIONARY synonym (
						Template=synonym,
						Synonyms=synonym_sample
);

SELECT ts_lexize('synonym', 'PoStGrEs');
SELECT ts_lexize('synonym', 'Gogle');
SELECT ts_lexize('synonym', 'indices');

-- Create and simple test thesaurus dictionary
-- More tests in configuration checks because ts_lexize()
-- cannot pass more than one word to thesaurus.
CREATE TEXT SEARCH DICTIONARY thesaurus (
                        Template=thesaurus,
						DictFile=thesaurus_sample,
						Dictionary=english_stem
);

SELECT ts_lexize('thesaurus', 'one');

-- Test ispell dictionary in configuration
CREATE TEXT SEARCH CONFIGURATION ispell_tst (
						COPY=english
);

ALTER TEXT SEARCH CONFIGURATION ispell_tst ALTER MAPPING FOR
	word, numword, asciiword, hword, numhword, asciihword, hword_part, hword_numpart, hword_asciipart
	WITH ispell, english_stem;

SELECT to_tsvector('ispell_tst', 'Booking the skies after rebookings for footballklubber from a foot');
SELECT to_tsquery('ispell_tst', 'footballklubber');
SELECT to_tsquery('ispell_tst', 'footballyklubber:b & rebookings:A & sky');

-- Test ispell dictionary with hunspell affix in configuration
CREATE TEXT SEARCH CONFIGURATION hunspell_tst (
						COPY=ispell_tst
);

ALTER TEXT SEARCH CONFIGURATION hunspell_tst ALTER MAPPING
	REPLACE ispell WITH hunspell;

SELECT to_tsvector('hunspell_tst', 'Booking the skies after rebookings for footballklubber from a foot');
SELECT to_tsquery('hunspell_tst', 'footballklubber');
SELECT to_tsquery('hunspell_tst', 'footballyklubber:b & rebookings:A & sky');

-- Test ispell dictionary with hunspell affix with FLAG long in configuration
ALTER TEXT SEARCH CONFIGURATION hunspell_tst ALTER MAPPING
	REPLACE hunspell WITH hunspell_long;

SELECT to_tsvector('hunspell_tst', 'Booking the skies after rebookings for footballklubber from a foot');
SELECT to_tsquery('hunspell_tst', 'footballklubber');
SELECT to_tsquery('hunspell_tst', 'footballyklubber:b & rebookings:A & sky');

-- Test ispell dictionary with hunspell affix with FLAG num in configuration
ALTER TEXT SEARCH CONFIGURATION hunspell_tst ALTER MAPPING
	REPLACE hunspell_long WITH hunspell_num;

SELECT to_tsvector('hunspell_tst', 'Booking the skies after rebookings for footballklubber from a foot');
SELECT to_tsquery('hunspell_tst', 'footballklubber');
SELECT to_tsquery('hunspell_tst', 'footballyklubber:b & rebookings:A & sky');

-- Test synonym dictionary in configuration
CREATE TEXT SEARCH CONFIGURATION synonym_tst (
						COPY=english
);

ALTER TEXT SEARCH CONFIGURATION synonym_tst ALTER MAPPING FOR
	asciiword, hword_asciipart, asciihword
	WITH synonym, english_stem;

SELECT to_tsvector('synonym_tst', 'Postgresql is often called as postgres or pgsql and pronounced as postgre');
SELECT to_tsvector('synonym_tst', 'Most common mistake is to write Gogle instead of Google');
SELECT to_tsvector('synonym_tst', 'Indexes or indices - Which is right plural form of index?');
SELECT to_tsquery('synonym_tst', 'Index & indices');

-- test thesaurus in configuration
-- see thesaurus_sample.ths to understand 'odd' resulting tsvector
CREATE TEXT SEARCH CONFIGURATION thesaurus_tst (
						COPY=synonym_tst
);

ALTER TEXT SEARCH CONFIGURATION thesaurus_tst ALTER MAPPING FOR
	asciiword, hword_asciipart, asciihword
	WITH synonym, thesaurus, english_stem;

SELECT to_tsvector('thesaurus_tst', 'one postgres one two one two three one');
SELECT to_tsvector('thesaurus_tst', 'Supernovae star is very new star and usually called supernovae (abbrevation SN)');
SELECT to_tsvector('thesaurus_tst', 'Booking tickets is looking like a booking a tickets');

-- test owner change
create user dict_u1 password 'Bigdata123@';
create user dict_u2 password 'Bigdata123@';
CREATE TEXT SEARCH DICTIONARY dict_pri (
    SYNONYMS = synonym_sample,
    template = SYNONYM 
);

alter text search dictionary dict_pri owner to dict_u1;
alter text search dictionary public.dict_pri (dummy);
select dictname from pg_ts_dict a, pg_user c where c.usename='dict_u1' and a.dictowner=c.usesysid;
select objfile is not null from pg_ts_dict a, pg_shdepend b, pg_user c where dictname='dict_pri' and a.oid=b.objid and b.refobjid=c.usesysid and c.usename='dict_u1';

alter text search dictionary dict_pri owner to dict_u2;
alter text search dictionary public.dict_pri (dummy);
select dictname from pg_ts_dict a, pg_user c where c.usename='dict_u1' and a.dictowner=c.usesysid;
select dictname from pg_ts_dict a, pg_user c where c.usename='dict_u2' and a.dictowner=c.usesysid;
select objfile is not null from pg_ts_dict a, pg_shdepend b, pg_user c where dictname='dict_pri' and a.oid=b.objid and b.refobjid=c.usesysid and c.usename='dict_u1';
select objfile is not null from pg_ts_dict a, pg_shdepend b, pg_user c where dictname='dict_pri' and a.oid=b.objid and b.refobjid=c.usesysid and c.usename='dict_u2';

REASSIGN OWNED BY dict_u2 to dict_u1;
select dictname from pg_ts_dict a, pg_user c where c.usename='dict_u1' and a.dictowner=c.usesysid;
select dictname from pg_ts_dict a, pg_user c where c.usename='dict_u2' and a.dictowner=c.usesysid;
select objfile is not null from pg_ts_dict a, pg_shdepend b, pg_user c where dictname='dict_pri' and a.oid=b.objid and b.refobjid=c.usesysid and c.usename='dict_u1';
select objfile is not null from pg_ts_dict a, pg_shdepend b, pg_user c where dictname='dict_pri' and a.oid=b.objid and b.refobjid=c.usesysid and c.usename='dict_u2';

drop user dict_u2;
select dictname from pg_ts_dict where dictname='dict_pri';
select objfile is not null from pg_ts_dict a, pg_shdepend b where dictname='dict_pri' and a.oid=b.objid;
drop user dict_u1;
drop user dict_u1 cascade;
select dictname from pg_ts_dict where dictname='dict_pri';
select objfile is not null from pg_ts_dict a, pg_shdepend b where dictname='dict_pri' and a.oid=b.objid;

-- test transaction block
drop table if exists dict_tb cascade;
create table dict_tb(c1 serial, c2 tsvector);
CREATE or replace FUNCTION create_dict_conf() RETURNS int
LANGUAGE plpgsql AS $$
BEGIN
  CREATE TEXT SEARCH DICTIONARY dict_trans (
  TEMPLATE = simple,
  STOPWORDS = english);
  create text search configuration conf_trans(copy=english);
  ALTER TEXT SEARCH CONFIGURATION conf_trans alter mapping for asciiword,word,numword,asciihword with dict_trans;
  Alter TEXT SEARCH DICTIONARY dict_trans (dummy);
  insert into dict_tb(c2) values(to_tsvector('conf_trans','i am a student'));
  insert into dict_tb(c2) values(to_tsvector('conf_trans','i am a student'));
  RETURN 1;
END $$;

BEGIN;
DECLARE ctt1 CURSOR FOR SELECT create_dict_conf();
DECLARE ctt2 CURSOR FOR SELECT create_dict_conf();
SAVEPOINT s1;
FETCH ctt1; 
ROLLBACK TO s1;
savepoint s2;
FETCH ctt2; 
ABORT;
select * from dict_tb order by 1,2;
SELECT ts_lexize('dict_trans', 'the');
select dictname from pg_ts_dict where dictname='dict_trans';
select objfile is not null from pg_ts_dict a, pg_shdepend b where dictname='dict_trans' and a.oid=b.objid;

BEGIN;
DECLARE ctt1 CURSOR FOR SELECT create_dict_conf();
DECLARE ctt2 CURSOR FOR SELECT create_dict_conf();
SAVEPOINT s1;
FETCH ctt1; 
ROLLBACK TO s1;
savepoint s2;
FETCH ctt2; 
COMMIT;
select * from dict_tb order by 1,2;
SELECT ts_lexize('dict_trans', 'the');
select dictname from pg_ts_dict where dictname='dict_trans';
select objfile is not null from pg_ts_dict a, pg_shdepend b where dictname='dict_trans' and a.oid=b.objid;

drop function create_dict_conf() cascade;
drop text search dictionary dict_trans cascade;
drop table dict_tb;