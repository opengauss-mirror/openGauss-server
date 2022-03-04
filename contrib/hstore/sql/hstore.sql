CREATE EXTENSION hstore;

set escape_string_warning=off;

--hstore;

select ''::hstore;
select 'a=>b'::hstore;
select ' a=>b'::hstore;
select 'a =>b'::hstore;
select 'a=>b '::hstore;
select 'a=> b'::hstore;
select '"a"=>"b"'::hstore;
select ' "a"=>"b"'::hstore;
select '"a" =>"b"'::hstore;
select '"a"=>"b" '::hstore;
select '"a"=> "b"'::hstore;
select 'aa=>bb'::hstore;
select ' aa=>bb'::hstore;
select 'aa =>bb'::hstore;
select 'aa=>bb '::hstore;
select 'aa=> bb'::hstore;
select '"aa"=>"bb"'::hstore;
select ' "aa"=>"bb"'::hstore;
select '"aa" =>"bb"'::hstore;
select '"aa"=>"bb" '::hstore;
select '"aa"=> "bb"'::hstore;

select 'aa=>bb, cc=>dd'::hstore;
select 'aa=>bb , cc=>dd'::hstore;
select 'aa=>bb ,cc=>dd'::hstore;
select 'aa=>bb, "cc"=>dd'::hstore;
select 'aa=>bb , "cc"=>dd'::hstore;
select 'aa=>bb ,"cc"=>dd'::hstore;
select 'aa=>"bb", cc=>dd'::hstore;
select 'aa=>"bb" , cc=>dd'::hstore;
select 'aa=>"bb" ,cc=>dd'::hstore;

select 'aa=>null'::hstore;
select 'aa=>NuLl'::hstore;
select 'aa=>"NuLl"'::hstore;

select e'\\=a=>q=w'::hstore;
select e'"=a"=>q\\=w'::hstore;
select e'"\\"a"=>q>w'::hstore;
select e'\\"a=>q"w'::hstore;

select ''::hstore;
select '	'::hstore;

-- -> operator

select pg_catalog.fetchval('aa=>b, c=>d , b=>16'::hstore, 'c');
select pg_catalog.fetchval('aa=>b, c=>d , b=>16'::hstore, 'b');
select pg_catalog.fetchval('aa=>b, c=>d , b=>16'::hstore, 'aa');
select (pg_catalog.fetchval('aa=>b, c=>d , b=>16'::hstore, 'gg')) is null;
select (pg_catalog.fetchval('aa=>NULL, c=>d , b=>16'::hstore, 'aa')) is null;
select (pg_catalog.fetchval('aa=>"NULL", c=>d , b=>16'::hstore, 'aa')) is null;

-- -> array operator

select pg_catalog.slice_array('aa=>"NULL", c=>d , b=>16'::hstore, ARRAY['aa','c']);
select pg_catalog.slice_array('aa=>"NULL", c=>d , b=>16'::hstore, ARRAY['c','aa']);
select pg_catalog.slice_array('aa=>NULL, c=>d , b=>16'::hstore, ARRAY['aa','c',null]);
select pg_catalog.slice_array('aa=>1, c=>3, b=>2, d=>4'::hstore, ARRAY[['b','d'],['aa','c']]);

-- exists/defined

select pg_catalog.exist('a=>NULL, b=>qq', 'a');
select pg_catalog.exist('a=>NULL, b=>qq', 'b');
select pg_catalog.exist('a=>NULL, b=>qq', 'c');
select pg_catalog.exist('a=>"NULL", b=>qq', 'a');
select pg_catalog.defined('a=>NULL, b=>qq', 'a');
select pg_catalog.defined('a=>NULL, b=>qq', 'b');
select pg_catalog.defined('a=>NULL, b=>qq', 'c');
select pg_catalog.defined('a=>"NULL", b=>qq', 'a');

-- delete

select pg_catalog.delete('a=>1 , b=>2, c=>3'::hstore, 'a');
select pg_catalog.delete('a=>null , b=>2, c=>3'::hstore, 'a');
select pg_catalog.delete('a=>1 , b=>2, c=>3'::hstore, 'b');
select pg_catalog.delete('a=>1 , b=>2, c=>3'::hstore, 'c');
select pg_catalog.delete('a=>1 , b=>2, c=>3'::hstore, 'd');
select pg_catalog.pg_column_size(pg_catalog.delete('a=>1 , b=>2, c=>3'::hstore, 'b'::text))
         = pg_catalog.pg_column_size('a=>1, b=>2'::hstore);

-- delete (array)

select pg_catalog.delete('a=>1 , b=>2, c=>3'::hstore, ARRAY['d','e']);
select pg_catalog.delete('a=>1 , b=>2, c=>3'::hstore, ARRAY['d','b']);
select pg_catalog.delete('a=>1 , b=>2, c=>3'::hstore, ARRAY['a','c']);
select pg_catalog.delete('a=>1 , b=>2, c=>3'::hstore, ARRAY[['b'],['c'],['a']]);
select pg_catalog.delete('a=>1 , b=>2, c=>3'::hstore, '{}'::text[]);
select pg_catalog.pg_column_size(pg_catalog.delete('a=>1 , b=>2, c=>3'::hstore, ARRAY['a','c']))
         = pg_catalog.pg_column_size('b=>2'::hstore);
select pg_catalog.pg_column_size(pg_catalog.delete('a=>1 , b=>2, c=>3'::hstore, '{}'::text[]))
         = pg_catalog.pg_column_size('a=>1, b=>2, c=>3'::hstore);

-- delete (hstore)

select pg_catalog.delete('aa=>1 , b=>2, c=>3'::hstore, 'aa=>4, b=>2'::hstore);
select pg_catalog.delete('aa=>1 , b=>2, c=>3'::hstore, 'aa=>NULL, c=>3'::hstore);
select pg_catalog.delete('aa=>1 , b=>2, c=>3'::hstore, 'aa=>1, b=>2, c=>3'::hstore);
select pg_catalog.delete('aa=>1 , b=>2, c=>3'::hstore, 'b=>2'::hstore);
select pg_catalog.delete('aa=>1 , b=>2, c=>3'::hstore, ''::hstore);
select pg_catalog.pg_column_size(pg_catalog.delete('a=>1 , b=>2, c=>3'::hstore, 'b=>2'::hstore))
         = pg_catalog.pg_column_size('a=>1, c=>3'::hstore);
select pg_catalog.pg_column_size(pg_catalog.delete('a=>1 , b=>2, c=>3'::hstore, ''::hstore))
         = pg_catalog.pg_column_size('a=>1, b=>2, c=>3'::hstore);

-- hs_concat
select pg_catalog.hs_concat('aa=>1 , b=>2, cq=>3'::hstore, 'cq=>l, b=>g, fg=>f');
select pg_catalog.hs_concat('aa=>1 , b=>2, cq=>3'::hstore, 'aq=>l');
select pg_catalog.hs_concat('aa=>1 , b=>2, cq=>3'::hstore, 'aa=>l');
select pg_catalog.hs_concat('aa=>1 , b=>2, cq=>3'::hstore, '');
select pg_catalog.hs_concat(''::hstore, 'cq=>l, b=>g, fg=>f');
select pg_catalog.pg_column_size(pg_catalog.hs_concat(''::hstore, ''::hstore)) = pg_catalog.pg_column_size(''::hstore);
select pg_catalog.pg_column_size(pg_catalog.hs_concat('aa=>1'::hstore, 'b=>2'::hstore))
         = pg_catalog.pg_column_size('aa=>1, b=>2'::hstore);
select pg_catalog.pg_column_size(pg_catalog.hs_concat('aa=>1, b=>2'::hstore, ''::hstore))
         = pg_catalog.pg_column_size('aa=>1, b=>2'::hstore);
select pg_catalog.pg_column_size(pg_catalog.hs_concat(''::hstore, 'aa=>1, b=>2'::hstore))
         = pg_catalog.pg_column_size('aa=>1, b=>2'::hstore);

-- hstore(text,text)
select pg_catalog.hs_concat('a=>g, b=>c'::hstore, pg_catalog.hstore('asd', 'gf'));
select pg_catalog.hs_concat('a=>g, b=>c'::hstore, pg_catalog.hstore('b', 'gf'));
select pg_catalog.hs_concat('a=>g, b=>c'::hstore, pg_catalog.hstore('b', 'NULL'));
select pg_catalog.hs_concat('a=>g, b=>c'::hstore, pg_catalog.hstore('b', NULL));
select (pg_catalog.hs_concat('a=>g, b=>c'::hstore, pg_catalog.hstore(NULL, 'b'))) is null;
select pg_catalog.pg_column_size(pg_catalog.hstore('b', 'gf'))
         = pg_catalog.pg_column_size('b=>gf'::hstore);
select pg_catalog.pg_column_size(pg_catalog.hs_concat('a=>g, b=>c'::hstore, pg_catalog.hstore('b', 'gf')))
         = pg_catalog.pg_column_size('a=>g, b=>gf'::hstore);

-- slice()
select pg_catalog.slice(hstore 'aa=>1, b=>2, c=>3', ARRAY['g','h','i']);
select pg_catalog.slice(hstore 'aa=>1, b=>2, c=>3', ARRAY['c','b']);
select pg_catalog.slice(hstore 'aa=>1, b=>2, c=>3', ARRAY['aa','b']);
select pg_catalog.slice(hstore 'aa=>1, b=>2, c=>3', ARRAY['c','b','aa']);
select pg_catalog.pg_column_size(pg_catalog.slice(hstore 'aa=>1, b=>2, c=>3', ARRAY['c','b']))
         = pg_catalog.pg_column_size('b=>2, c=>3'::hstore);
select pg_catalog.pg_column_size(pg_catalog.slice(hstore 'aa=>1, b=>2, c=>3', ARRAY['c','b','aa']))
         = pg_catalog.pg_column_size('aa=>1, b=>2, c=>3'::hstore);

-- array input
select '{}'::text[]::hstore;
select ARRAY['a','g','b','h','asd']::hstore;
select ARRAY['a','g','b','h','asd','i']::hstore;
select ARRAY[['a','g'],['b','h'],['asd','i']]::hstore;
select ARRAY[['a','g','b'],['h','asd','i']]::hstore;
select ARRAY[[['a','g'],['b','h'],['asd','i']]]::hstore;
select pg_catalog.hstore('{}'::text[]);
select pg_catalog.hstore(ARRAY['a','g','b','h','asd']);
select pg_catalog.hstore(ARRAY['a','g','b','h','asd','i']);
select pg_catalog.hstore(ARRAY[['a','g'],['b','h'],['asd','i']]);
select pg_catalog.hstore(ARRAY[['a','g','b'],['h','asd','i']]);
select pg_catalog.hstore(ARRAY[[['a','g'],['b','h'],['asd','i']]]);
select pg_catalog.hstore('[0:5]={a,g,b,h,asd,i}'::text[]);
select pg_catalog.hstore('[0:2][1:2]={{a,g},{b,h},{asd,i}}'::text[]);

-- pairs of arrays
select pg_catalog.hstore(ARRAY['a','b','asd'], ARRAY['g','h','i']);
select pg_catalog.hstore(ARRAY['a','b','asd'], ARRAY['g','h',NULL]);
select pg_catalog.hstore(ARRAY['z','y','x'], ARRAY['1','2','3']);
select pg_catalog.hstore(ARRAY['aaa','bb','c','d'], ARRAY[null::text,null,null,null]);
select pg_catalog.hstore(ARRAY['aaa','bb','c','d'], null);
select pg_catalog.quote_literal(pg_catalog.hstore('{}'::text[], '{}'::text[]));
select pg_catalog.quote_literal(pg_catalog.hstore('{}'::text[], null));
select pg_catalog.hstore(ARRAY['a'], '{}'::text[]);  -- error
select pg_catalog.hstore('{}'::text[], ARRAY['a']);  -- error
select pg_catalog.pg_column_size(pg_catalog.hstore(ARRAY['a','b','asd'], ARRAY['g','h','i']))
         = pg_catalog.pg_column_size('a=>g, b=>h, asd=>i'::hstore);

-- keys/values
select pg_catalog.akeys(pg_catalog.hs_concat('aa=>1 , b=>2, cq=>3'::hstore, 'cq=>l, b=>g, fg=>f'));
select pg_catalog.akeys('""=>1');
select pg_catalog.akeys('');
select pg_catalog.avals(pg_catalog.hs_concat('aa=>1 , b=>2, cq=>3'::hstore, 'cq=>l, b=>g, fg=>f'));
select pg_catalog.avals(pg_catalog.hs_concat('aa=>1 , b=>2, cq=>3'::hstore, 'cq=>l, b=>g, fg=>NULL'));
select pg_catalog.avals('""=>1');
select pg_catalog.avals('');

select pg_catalog.hstore_to_array('aa=>1, cq=>l, b=>g, fg=>NULL'::hstore);

select pg_catalog.hstore_to_matrix('aa=>1, cq=>l, b=>g, fg=>NULL'::hstore);

select * from pg_catalog.skeys(pg_catalog.hs_concat('aa=>1 , b=>2, cq=>3'::hstore, 'cq=>l, b=>g, fg=>f'));
select * from pg_catalog.skeys('""=>1');
select * from pg_catalog.skeys('');
select * from pg_catalog.svals(pg_catalog.hs_concat('aa=>1 , b=>2, cq=>3'::hstore, 'cq=>l, b=>g, fg=>f'));
select *, svals is null from pg_catalog.svals(pg_catalog.hs_concat('aa=>1 , b=>2, cq=>3'::hstore, 'cq=>l, b=>g, fg=>NULL'));
select * from pg_catalog.svals('""=>1');
select * from pg_catalog.svals('');

select * from pg_catalog.each('aaa=>bq, b=>NULL, ""=>1 ');

-- hs_contains
select pg_catalog.hs_contains('a=>b, b=>1, c=>NULL'::hstore, 'a=>b'::hstore);
select pg_catalog.hs_contains('a=>b, b=>1, c=>NULL'::hstore, 'a=>b, c=>NULL'::hstore);
select pg_catalog.hs_contains('a=>b, b=>1, c=>NULL'::hstore, 'a=>b, g=>NULL'::hstore);
select pg_catalog.hs_contains('a=>b, b=>1, c=>NULL'::hstore, 'g=>NULL'::hstore);
select pg_catalog.hs_contains('a=>b, b=>1, c=>NULL'::hstore, 'a=>c'::hstore);
select pg_catalog.hs_contains('a=>b, b=>1, c=>NULL'::hstore, 'a=>b'::hstore);
select pg_catalog.hs_contains('a=>b, b=>1, c=>NULL'::hstore, 'a=>b, c=>q'::hstore);

CREATE TABLE testhstore (h hstore);
\copy testhstore from 'data/hstore.data'

select pg_catalog.count(*) from testhstore where pg_catalog.hs_contains(h, 'wait=>NULL'::hstore);
select pg_catalog.count(*) from testhstore where pg_catalog.hs_contains(h, 'wait=>CC'::hstore);
select pg_catalog.count(*) from testhstore where pg_catalog.hs_contains(h, 'wait=>CC, public=>t'::hstore);
select pg_catalog.count(*) from testhstore where pg_catalog.exist(h, 'public');
select pg_catalog.count(*) from testhstore where pg_catalog.exists_any(h, ARRAY['public','disabled']);
select pg_catalog.count(*) from testhstore where pg_catalog.exists_all(h, ARRAY['public','disabled']);
