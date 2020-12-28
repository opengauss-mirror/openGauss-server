-- Test regexp_matches
select regexp_matches('abb', '(?<=a)b*');
select regexp_matches('a', 'a(?<=a)b*');
select regexp_matches('abc', 'a(?<=a)b*(?<=b)c*');
select regexp_matches('ab', 'a(?<=a)b*(?<=b)c*');
select regexp_matches('ab', 'a*(?<!a)b*');
select regexp_matches('ab', 'a*(?<!a)b+');
select regexp_matches('b', 'a*(?<!a)b+');
select regexp_matches('a', 'a(?<!a)b*');
select regexp_matches('b', '(?<=b)b');
select regexp_matches('foobar', '(?<=f)b+');
select regexp_matches('foobar', '(?<=foo)b+');
select regexp_matches('foobar', '(?<=oo)b+');
select regexp_matches('Programmer', '(\w)(.*?\1)', 'g');
SELECT regexp_matches('foo' || chr(10) || 'bar' || chr(10) || 'bequq' || chr(10) || 'baz', '^', 'mg');
SELECT regexp_matches('foo' || chr(10) || 'bar' || chr(10) || 'bequq' || chr(10) || 'baz', '$', 'mg');
SELECT regexp_matches('1' || chr(10) || '2' || chr(10) || '3' || chr(10) || '4' || chr(10), '^.?', 'mg');
create database tpcds dbcompatibility 'C';
\c tpcds
select regexp_matches('foo/bar/baz',
                      '^([^/]+?)(?:/([^/]+?))(?:/([^/]+?))?$', '');
\c regression
drop database tpcds;
