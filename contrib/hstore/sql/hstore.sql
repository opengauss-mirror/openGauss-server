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

select fetchval('aa=>b, c=>d , b=>16'::hstore, 'c');
select fetchval('aa=>b, c=>d , b=>16'::hstore, 'b');
select fetchval('aa=>b, c=>d , b=>16'::hstore, 'aa');
select (fetchval('aa=>b, c=>d , b=>16'::hstore, 'gg')) is null;
select (fetchval('aa=>NULL, c=>d , b=>16'::hstore, 'aa')) is null;
select (fetchval('aa=>"NULL", c=>d , b=>16'::hstore, 'aa')) is null;

-- -> array operator

select slice_array('aa=>"NULL", c=>d , b=>16'::hstore, ARRAY['aa','c']);
select slice_array('aa=>"NULL", c=>d , b=>16'::hstore, ARRAY['c','aa']);
select slice_array('aa=>NULL, c=>d , b=>16'::hstore, ARRAY['aa','c',null]);
select slice_array('aa=>1, c=>3, b=>2, d=>4'::hstore, ARRAY[['b','d'],['aa','c']]);

-- exists/defined

select exist('a=>NULL, b=>qq', 'a');
select exist('a=>NULL, b=>qq', 'b');
select exist('a=>NULL, b=>qq', 'c');
select exist('a=>"NULL", b=>qq', 'a');
select defined('a=>NULL, b=>qq', 'a');
select defined('a=>NULL, b=>qq', 'b');
select defined('a=>NULL, b=>qq', 'c');
select defined('a=>"NULL", b=>qq', 'a');

-- delete

select delete('a=>1 , b=>2, c=>3'::hstore, 'a');
select delete('a=>null , b=>2, c=>3'::hstore, 'a');
select delete('a=>1 , b=>2, c=>3'::hstore, 'b');
select delete('a=>1 , b=>2, c=>3'::hstore, 'c');
select delete('a=>1 , b=>2, c=>3'::hstore, 'd');
select pg_column_size(delete('a=>1 , b=>2, c=>3'::hstore, 'b'::text))
         = pg_column_size('a=>1, b=>2'::hstore);

-- delete (array)

select delete('a=>1 , b=>2, c=>3'::hstore, ARRAY['d','e']);
select delete('a=>1 , b=>2, c=>3'::hstore, ARRAY['d','b']);
select delete('a=>1 , b=>2, c=>3'::hstore, ARRAY['a','c']);
select delete('a=>1 , b=>2, c=>3'::hstore, ARRAY[['b'],['c'],['a']]);
select delete('a=>1 , b=>2, c=>3'::hstore, '{}'::text[]);
select pg_column_size(delete('a=>1 , b=>2, c=>3'::hstore, ARRAY['a','c']))
         = pg_column_size('b=>2'::hstore);
select pg_column_size(delete('a=>1 , b=>2, c=>3'::hstore, '{}'::text[]))
         = pg_column_size('a=>1, b=>2, c=>3'::hstore);

-- delete (hstore)

select delete('aa=>1 , b=>2, c=>3'::hstore, 'aa=>4, b=>2'::hstore);
select delete('aa=>1 , b=>2, c=>3'::hstore, 'aa=>NULL, c=>3'::hstore);
select delete('aa=>1 , b=>2, c=>3'::hstore, 'aa=>1, b=>2, c=>3'::hstore);
select delete('aa=>1 , b=>2, c=>3'::hstore, 'b=>2'::hstore);
select delete('aa=>1 , b=>2, c=>3'::hstore, ''::hstore);
select pg_column_size(delete('a=>1 , b=>2, c=>3'::hstore, 'b=>2'::hstore))
         = pg_column_size('a=>1, c=>3'::hstore);
select pg_column_size(delete('a=>1 , b=>2, c=>3'::hstore, ''::hstore))
         = pg_column_size('a=>1, b=>2, c=>3'::hstore);

-- hs_concat
select hs_concat('aa=>1 , b=>2, cq=>3'::hstore, 'cq=>l, b=>g, fg=>f');
select hs_concat('aa=>1 , b=>2, cq=>3'::hstore, 'aq=>l');
select hs_concat('aa=>1 , b=>2, cq=>3'::hstore, 'aa=>l');
select hs_concat('aa=>1 , b=>2, cq=>3'::hstore, '');
select hs_concat(''::hstore, 'cq=>l, b=>g, fg=>f');
select pg_column_size(hs_concat(''::hstore, ''::hstore)) = pg_column_size(''::hstore);
select pg_column_size(hs_concat('aa=>1'::hstore, 'b=>2'::hstore))
         = pg_column_size('aa=>1, b=>2'::hstore);
select pg_column_size(hs_concat('aa=>1, b=>2'::hstore, ''::hstore))
         = pg_column_size('aa=>1, b=>2'::hstore);
select pg_column_size(hs_concat(''::hstore, 'aa=>1, b=>2'::hstore))
         = pg_column_size('aa=>1, b=>2'::hstore);

-- hstore(text,text)
select hs_concat('a=>g, b=>c'::hstore, hstore('asd', 'gf'));
select hs_concat('a=>g, b=>c'::hstore, hstore('b', 'gf'));
select hs_concat('a=>g, b=>c'::hstore, hstore('b', 'NULL'));
select hs_concat('a=>g, b=>c'::hstore, hstore('b', NULL));
select (hs_concat('a=>g, b=>c'::hstore, hstore(NULL, 'b'))) is null;
select pg_column_size(hstore('b', 'gf'))
         = pg_column_size('b=>gf'::hstore);
select pg_column_size(hs_concat('a=>g, b=>c'::hstore, hstore('b', 'gf')))
         = pg_column_size('a=>g, b=>gf'::hstore);

-- slice()
select slice(hstore 'aa=>1, b=>2, c=>3', ARRAY['g','h','i']);
select slice(hstore 'aa=>1, b=>2, c=>3', ARRAY['c','b']);
select slice(hstore 'aa=>1, b=>2, c=>3', ARRAY['aa','b']);
select slice(hstore 'aa=>1, b=>2, c=>3', ARRAY['c','b','aa']);
select pg_column_size(slice(hstore 'aa=>1, b=>2, c=>3', ARRAY['c','b']))
         = pg_column_size('b=>2, c=>3'::hstore);
select pg_column_size(slice(hstore 'aa=>1, b=>2, c=>3', ARRAY['c','b','aa']))
         = pg_column_size('aa=>1, b=>2, c=>3'::hstore);

-- array input
select '{}'::text[]::hstore;
select ARRAY['a','g','b','h','asd']::hstore;
select ARRAY['a','g','b','h','asd','i']::hstore;
select ARRAY[['a','g'],['b','h'],['asd','i']]::hstore;
select ARRAY[['a','g','b'],['h','asd','i']]::hstore;
select ARRAY[[['a','g'],['b','h'],['asd','i']]]::hstore;
select hstore('{}'::text[]);
select hstore(ARRAY['a','g','b','h','asd']);
select hstore(ARRAY['a','g','b','h','asd','i']);
select hstore(ARRAY[['a','g'],['b','h'],['asd','i']]);
select hstore(ARRAY[['a','g','b'],['h','asd','i']]);
select hstore(ARRAY[[['a','g'],['b','h'],['asd','i']]]);
select hstore('[0:5]={a,g,b,h,asd,i}'::text[]);
select hstore('[0:2][1:2]={{a,g},{b,h},{asd,i}}'::text[]);

-- pairs of arrays
select hstore(ARRAY['a','b','asd'], ARRAY['g','h','i']);
select hstore(ARRAY['a','b','asd'], ARRAY['g','h',NULL]);
select hstore(ARRAY['z','y','x'], ARRAY['1','2','3']);
select hstore(ARRAY['aaa','bb','c','d'], ARRAY[null::text,null,null,null]);
select hstore(ARRAY['aaa','bb','c','d'], null);
select quote_literal(hstore('{}'::text[], '{}'::text[]));
select quote_literal(hstore('{}'::text[], null));
select hstore(ARRAY['a'], '{}'::text[]);  -- error
select hstore('{}'::text[], ARRAY['a']);  -- error
select pg_column_size(hstore(ARRAY['a','b','asd'], ARRAY['g','h','i']))
         = pg_column_size('a=>g, b=>h, asd=>i'::hstore);

-- keys/values
select akeys(hs_concat('aa=>1 , b=>2, cq=>3'::hstore, 'cq=>l, b=>g, fg=>f'));
select akeys('""=>1');
select akeys('');
select avals(hs_concat('aa=>1 , b=>2, cq=>3'::hstore, 'cq=>l, b=>g, fg=>f'));
select avals(hs_concat('aa=>1 , b=>2, cq=>3'::hstore, 'cq=>l, b=>g, fg=>NULL'));
select avals('""=>1');
select avals('');

select hstore_to_array('aa=>1, cq=>l, b=>g, fg=>NULL'::hstore);

select hstore_to_matrix('aa=>1, cq=>l, b=>g, fg=>NULL'::hstore);

select * from skeys(hs_concat('aa=>1 , b=>2, cq=>3'::hstore, 'cq=>l, b=>g, fg=>f'));
select * from skeys('""=>1');
select * from skeys('');
select * from svals(hs_concat('aa=>1 , b=>2, cq=>3'::hstore, 'cq=>l, b=>g, fg=>f'));
select *, svals is null from svals(hs_concat('aa=>1 , b=>2, cq=>3'::hstore, 'cq=>l, b=>g, fg=>NULL'));
select * from svals('""=>1');
select * from svals('');

select * from each('aaa=>bq, b=>NULL, ""=>1 ');

-- hs_contains
select hs_contains('a=>b, b=>1, c=>NULL'::hstore, 'a=>b'::hstore);
select hs_contains('a=>b, b=>1, c=>NULL'::hstore, 'a=>b, c=>NULL'::hstore);
select hs_contains('a=>b, b=>1, c=>NULL'::hstore, 'a=>b, g=>NULL'::hstore);
select hs_contains('a=>b, b=>1, c=>NULL'::hstore, 'g=>NULL'::hstore);
select hs_contains('a=>b, b=>1, c=>NULL'::hstore, 'a=>c'::hstore);
select hs_contains('a=>b, b=>1, c=>NULL'::hstore, 'a=>b'::hstore);
select hs_contains('a=>b, b=>1, c=>NULL'::hstore, 'a=>b, c=>q'::hstore);

CREATE TABLE testhstore (h hstore);
\copy testhstore from 'data/hstore.data'

select count(*) from testhstore where hs_contains(h, 'wait=>NULL'::hstore);
select count(*) from testhstore where hs_contains(h, 'wait=>CC'::hstore);
select count(*) from testhstore where hs_contains(h, 'wait=>CC, public=>t'::hstore);
select count(*) from testhstore where exist(h, 'public');
select count(*) from testhstore where exists_any(h, ARRAY['public','disabled']);
select count(*) from testhstore where exists_all(h, ARRAY['public','disabled']);
