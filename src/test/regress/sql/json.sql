-- Strings.
SELECT '""'::json;				-- OK.
SELECT $$''$$::json;			-- ERROR, single quotes are not allowed
SELECT '"abc"'::json;			-- OK
SELECT '"abc'::json;			-- ERROR, quotes not closed
SELECT '"abc
def"'::json;					-- ERROR, unescaped newline in string constant
SELECT '"\n\"\\"'::json;		-- OK, legal escapes
SELECT '"\v"'::json;			-- ERROR, not a valid JSON escape
SELECT '"\u"'::json;			-- ERROR, incomplete escape
SELECT '"\u00"'::json;			-- ERROR, incomplete escape
SELECT '"\u000g"'::json;		-- ERROR, g is not a hex digit
SELECT '"\u0000"'::json;		-- OK, legal escape
SELECT '"\uaBcD"'::json;		-- OK, uppercase and lower case both OK

-- Numbers.
SELECT '1'::json;				-- OK
SELECT '0'::json;				-- OK
SELECT '01'::json;				-- ERROR, not valid according to JSON spec
SELECT '0.1'::json;				-- OK
SELECT '9223372036854775808'::json;	-- OK, even though it's too large for int8
SELECT '1e100'::json;			-- OK
SELECT '1.3e100'::json;			-- OK
SELECT '1f2'::json;				-- ERROR
SELECT '0.x1'::json;			-- ERROR
SELECT '1.3ex100'::json;		-- ERROR

-- Arrays.
SELECT '[]'::json;				-- OK
SELECT '[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]'::json;  -- OK
SELECT '[1,2]'::json;			-- OK
SELECT '[1,2,]'::json;			-- ERROR, trailing comma
SELECT '[1,2'::json;			-- ERROR, no closing bracket
SELECT '[1,[2]'::json;			-- ERROR, no closing bracket

-- Objects.
SELECT '{}'::json;				-- OK
SELECT '{"abc"}'::json;			-- ERROR, no value
SELECT '{"abc":1}'::json;		-- OK
SELECT '{1:"abc"}'::json;		-- ERROR, keys must be strings
SELECT '{"abc",1}'::json;		-- ERROR, wrong separator
SELECT '{"abc"=1}'::json;		-- ERROR, totally wrong separator
SELECT '{"abc"::1}'::json;		-- ERROR, another wrong separator
SELECT '{"abc":1,"def":2,"ghi":[3,4],"hij":{"klm":5,"nop":[6]}}'::json; -- OK
SELECT '{"abc":1:2}'::json;		-- ERROR, colon in wrong spot
SELECT '{"abc":1,3}'::json;		-- ERROR, no value

-- Miscellaneous stuff.
SELECT 'true'::json;			-- OK
SELECT 'false'::json;			-- OK
SELECT 'null'::json;			-- OK
SELECT ' true '::json;			-- OK, even with extra whitespace
SELECT 'true false'::json;		-- ERROR, too many values
SELECT 'true, false'::json;		-- ERROR, too many values
SELECT 'truf'::json;			-- ERROR, not a keyword
SELECT 'trues'::json;			-- ERROR, not a keyword
SELECT ''::json;				-- ERROR, no value
SELECT '    '::json;			-- ERROR, no value

--constructors
-- array_to_json

SELECT array_to_json(array(select 1 as a));
SELECT array_to_json(array_agg(q),false) from (select x as b, x * 2 as c from generate_series(1,3) x) q;
SELECT array_to_json(array_agg(q),true) from (select x as b, x * 2 as c from generate_series(1,3) x) q;
SELECT array_to_json(array_agg(q),false)
  FROM ( SELECT $$a$$ || x AS b, y AS c,
               ARRAY[ROW(x.*,ARRAY[1,2,3]),
               ROW(y.*,ARRAY[4,5,6])] AS z
         FROM generate_series(1,2) x,
              generate_series(4,5) y) q;
SELECT array_to_json(array_agg(x),false) from generate_series(5,10) x;
SELECT array_to_json('{{1,5},{99,100}}'::int[]);

-- row_to_json
SELECT row_to_json(row(1,'foo'));

SELECT row_to_json(q)
FROM (SELECT $$a$$ || x AS b,
         y AS c,
         ARRAY[ROW(x.*,ARRAY[1,2,3]),
               ROW(y.*,ARRAY[4,5,6])] AS z
      FROM generate_series(1,2) x,
           generate_series(4,5) y) q;

SELECT row_to_json(q,true)
FROM (SELECT $$a$$ || x AS b,
         y AS c,
         ARRAY[ROW(x.*,ARRAY[1,2,3]),
               ROW(y.*,ARRAY[4,5,6])] AS z
      FROM generate_series(1,2) x,
           generate_series(4,5) y) q;

-- Enforce use of COMMIT instead of 2PC for temporary objects
CREATE TEMP TABLE rows AS
SELECT x, 'txt' || x as y
FROM generate_series(1,3) AS x;

SELECT row_to_json(q,true)
FROM rows q order by x;

SELECT row_to_json(row((select array_agg(x) as d from generate_series(5,10) x)),false);

--json_agg
SELECT json_agg(q)
  FROM ( SELECT $$a$$ || x AS b, y AS c,
               ARRAY[ROW(x.*,ARRAY[1,2,3]),
               ROW(y.*,ARRAY[4,5,6])] AS z
         FROM generate_series(1,2) x,
              generate_series(4,5) y) q;

SELECT json_agg(q)
  FROM rows q;

-- non-numeric output
SELECT row_to_json(q)
FROM (SELECT 'NaN'::float8 AS "float8field") q;

SELECT row_to_json(q)
FROM (SELECT 'Infinity'::float8 AS "float8field") q;

SELECT row_to_json(q)
FROM (SELECT '-Infinity'::float8 AS "float8field") q;

-- json input
SELECT row_to_json(q)
FROM (SELECT '{"a":1,"b": [2,3,4,"d","e","f"],"c":{"p":1,"q":2}}'::json AS "jsonfield") q;

-- json extraction functions
CREATE TEMP TABLE test_json (
       json_type text,
       test_json json
);

INSERT INTO test_json VALUES
('scalar','"a scalar"'),
('array','["zero", "one","two",null,"four","five", [1,2,3],{"f1":9}]'),
('object','{"field1":"val1","field2":"val2","field3":null, "field4": 4, "field5": [1,2,3], "field6": {"f1":9}}');

SELECT test_json -> 'x'
FROM test_json
WHERE json_type = 'scalar';

SELECT test_json -> 'x'
FROM test_json
WHERE json_type = 'array';

SELECT test_json -> 'x'
FROM test_json
WHERE json_type = 'object';

SELECT test_json->'field2'
FROM test_json
WHERE json_type = 'object';

SELECT test_json->>'field2'
FROM test_json
WHERE json_type = 'object';

SELECT test_json -> 2
FROM test_json
WHERE json_type = 'scalar';

SELECT test_json -> 2
FROM test_json
WHERE json_type = 'array';

SELECT test_json -> 2
FROM test_json
WHERE json_type = 'object';

SELECT test_json->>2
FROM test_json
WHERE json_type = 'array';

SELECT test_json ->> 6 FROM test_json WHERE json_type = 'array';
SELECT test_json ->> 7 FROM test_json WHERE json_type = 'array';

SELECT test_json ->> 'field4' FROM test_json WHERE json_type = 'object';
SELECT test_json ->> 'field5' FROM test_json WHERE json_type = 'object';
SELECT test_json ->> 'field6' FROM test_json WHERE json_type = 'object';

SELECT json_object_keys(test_json)
FROM test_json
WHERE json_type = 'scalar';

SELECT json_object_keys(test_json)
FROM test_json
WHERE json_type = 'array';

SELECT json_object_keys(test_json)
FROM test_json
WHERE json_type = 'object';

-- test extending object_keys resultset - initial resultset size is 256

select count(*) from
    (select json_object_keys(json_object(array_agg(g)))
     from (select unnest(array['f'||n,n::text])as g
           from generate_series(1,300) as n) x ) y;

-- nulls

select (test_json->'field3') is null as expect_false
from test_json
where json_type = 'object';

select (test_json->>'field3') is null as expect_true
from test_json
where json_type = 'object';

select (test_json->3) is null as expect_false
from test_json
where json_type = 'array';

select (test_json->>3) is null as expect_true
from test_json
where json_type = 'array';


-- array length

SELECT json_array_length('[1,2,3,{"f1":1,"f2":[5,6]},4]');

SELECT json_array_length('[]');

SELECT json_array_length('{"f1":1,"f2":[5,6]}');

SELECT json_array_length('4');

-- each

select json_each('{"f1":[1,2,3],"f2":{"f3":1},"f4":null}');
select * from json_each('{"f1":[1,2,3],"f2":{"f3":1},"f4":null,"f5":99,"f6":"stringy"}') q;

select json_each_text('{"f1":[1,2,3],"f2":{"f3":1},"f4":null,"f5":"null"}');
select * from json_each_text('{"f1":[1,2,3],"f2":{"f3":1},"f4":null,"f5":99,"f6":"stringy"}') q;

-- extract_path, extract_path_as_text

select json_extract_path('{"f2":{"f3":1},"f4":{"f5":99,"f6":"stringy"}}','f4','f6');
select json_extract_path('{"f2":{"f3":1},"f4":{"f5":99,"f6":"stringy"}}','f2');
select json_extract_path('{"f2":["f3",1],"f4":{"f5":99,"f6":"stringy"}}','f2',0::text);
select json_extract_path('{"f2":["f3",1],"f4":{"f5":99,"f6":"stringy"}}','f2',1::text);
select json_extract_path_text('{"f2":{"f3":1},"f4":{"f5":99,"f6":"stringy"}}','f4','f6');
select json_extract_path_text('{"f2":{"f3":1},"f4":{"f5":99,"f6":"stringy"}}','f2');
select json_extract_path_text('{"f2":["f3",1],"f4":{"f5":99,"f6":"stringy"}}','f2',0::text);
select json_extract_path_text('{"f2":["f3",1],"f4":{"f5":99,"f6":"stringy"}}','f2',1::text);

-- extract_path nulls

select json_extract_path('{"f2":{"f3":1},"f4":{"f5":null,"f6":"stringy"}}','f4','f5') is null as expect_false;
select json_extract_path_text('{"f2":{"f3":1},"f4":{"f5":null,"f6":"stringy"}}','f4','f5') is null as expect_true;
select json_extract_path('{"f2":{"f3":1},"f4":[0,1,2,null]}','f4','3') is null as expect_false;
select json_extract_path_text('{"f2":{"f3":1},"f4":[0,1,2,null]}','f4','3') is null as expect_true;

-- extract_path operators

select '{"f2":{"f3":1},"f4":{"f5":99,"f6":"stringy"}}'::json #>array['f4','f6'];
select '{"f2":{"f3":1},"f4":{"f5":99,"f6":"stringy"}}'::json #>array['f2'];
select '{"f2":["f3",1],"f4":{"f5":99,"f6":"stringy"}}'::json #>array['f2','0'];
select '{"f2":["f3",1],"f4":{"f5":99,"f6":"stringy"}}'::json #>array['f2','1'];
select '{"f2":{"f3":1},"f4":{"f5":99,"f6":"stringy"}}'::json #>>array['f4','f6'];
select '{"f2":{"f3":1},"f4":{"f5":99,"f6":"stringy"}}'::json #>>array['f2'];
select '{"f2":["f3",1],"f4":{"f5":99,"f6":"stringy"}}'::json #>>array['f2','0'];
select '{"f2":["f3",1],"f4":{"f5":99,"f6":"stringy"}}'::json #>>array['f2','1'];

-- same using array literals
select '{"f2":{"f3":1},"f4":{"f5":99,"f6":"stringy"}}'::json #>'{f4,f6}';
select '{"f2":{"f3":1},"f4":{"f5":99,"f6":"stringy"}}'::json #>'{f2}';
select '{"f2":["f3",1],"f4":{"f5":99,"f6":"stringy"}}'::json #>'{f2,0}';
select '{"f2":["f3",1],"f4":{"f5":99,"f6":"stringy"}}'::json #>'{f2,1}';
select '{"f2":{"f3":1},"f4":{"f5":99,"f6":"stringy"}}'::json #>>'{f4,f6}';
select '{"f2":{"f3":1},"f4":{"f5":99,"f6":"stringy"}}'::json #>>'{f2}';
select '{"f2":["f3",1],"f4":{"f5":99,"f6":"stringy"}}'::json #>>'{f2,0}';
select '{"f2":["f3",1],"f4":{"f5":99,"f6":"stringy"}}'::json #>>'{f2,1}';

-- array_elements

select json_array_elements('[1,true,[1,[2,3]],null,{"f1":1,"f2":[7,8,9]},false,"stringy"]');
select * from json_array_elements('[1,true,[1,[2,3]],null,{"f1":1,"f2":[7,8,9]},false,"stringy"]') q;
select json_array_elements_text('[1,true,[1,[2,3]],null,{"f1":1,"f2":[7,8,9]},false,"stringy"]');
select * from json_array_elements_text('[1,true,[1,[2,3]],null,{"f1":1,"f2":[7,8,9]},false,"stringy"]') q;

-- populate_record
create type jpop as (a text, b int, c timestamp);

select * from json_populate_record(null::jpop,'{"a":"blurfl","x":43.2}') q;
select * from json_populate_record(row('x',3,'2012-12-31 15:30:56')::jpop,'{"a":"blurfl","x":43.2}') q;

select * from json_populate_record(null::jpop,'{"a":"blurfl","x":43.2}', true) q;
select * from json_populate_record(row('x',3,'2012-12-31 15:30:56')::jpop,'{"a":"blurfl","x":43.2}', true) q;

select * from json_populate_record(null::jpop,'{"a":[100,200,false],"x":43.2}', true) q;
select * from json_populate_record(row('x',3,'2012-12-31 15:30:56')::jpop,'{"a":[100,200,false],"x":43.2}', true) q;
select * from json_populate_record(row('x',3,'2012-12-31 15:30:56')::jpop,'{"c":[100,200,false],"x":43.2}', true) q;

-- populate_recordset

select * from json_populate_recordset(null::jpop,'[{"a":"blurfl","x":43.2},{"b":3,"c":"2012-01-20 10:42:53"}]',false) q;
select * from json_populate_recordset(row('def',99,null)::jpop,'[{"a":"blurfl","x":43.2},{"b":3,"c":"2012-01-20 10:42:53"}]',false) q;
select * from json_populate_recordset(null::jpop,'[{"a":"blurfl","x":43.2},{"b":3,"c":"2012-01-20 10:42:53"}]',true) q;
select * from json_populate_recordset(row('def',99,null)::jpop,'[{"a":"blurfl","x":43.2},{"b":3,"c":"2012-01-20 10:42:53"}]',true) q;
select * from json_populate_recordset(row('def',99,null)::jpop,'[{"a":[100,200,300],"x":43.2},{"a":{"z":true},"b":3,"c":"2012-01-20 10:42:53"}]',true) q;
select * from json_populate_recordset(row('def',99,null)::jpop,'[{"c":[100,200,300],"x":43.2},{"a":{"z":true},"b":3,"c":"2012-01-20 10:42:53"}]',true) q;

-- using the default use_json_as_text argument

select * from json_populate_recordset(null::jpop,'[{"a":"blurfl","x":43.2},{"b":3,"c":"2012-01-20 10:42:53"}]') q;
select * from json_populate_recordset(row('def',99,null)::jpop,'[{"a":"blurfl","x":43.2},{"b":3,"c":"2012-01-20 10:42:53"}]') q;
select * from json_populate_recordset(row('def',99,null)::jpop,'[{"a":[100,200,300],"x":43.2},{"a":{"z":true},"b":3,"c":"2012-01-20 10:42:53"}]') q;
select * from json_populate_recordset(row('def',99,null)::jpop,'[{"c":[100,200,300],"x":43.2},{"a":{"z":true},"b":3,"c":"2012-01-20 10:42:53"}]') q;

-- handling of unicode surrogate pairs

select json '{ "a":  "\ud83d\ude04\ud83d\udc36" }' -> 'a' as correct_in_utf8;
select json '{ "a":  "\ud83d\ud83d" }' -> 'a'; -- 2 high surrogates in a row
select json '{ "a":  "\ude04\ud83d" }' -> 'a'; -- surrogates in wrong order
select json '{ "a":  "\ud83dX" }' -> 'a'; -- orphan high surrogate
select json '{ "a":  "\ude04X" }' -> 'a'; -- orphan low surrogate

--handling of simple unicode escapes

select json '{ "a":  "the Copyright \u00a9 sign" }' ->> 'a' as correct_in_utf8;
select json '{ "a":  "dollar \u0024 character" }' ->> 'a' as correct_everywhere;
select json '{ "a":  "null \u0000 escape" }' ->> 'a' as not_unescaped;

--json_typeof() function
select value, json_typeof(value)
  from (values (json '123.4'),
               (json '-1'),
               (json '"foo"'),
               (json 'true'),
               (json 'false'),
               (json 'null'),
               (json '[1, 2, 3]'),
               (json '[]'),
               (json '{"x":"foo", "y":123}'),
               (json '{}'),
               (NULL::json))
      as data(value);

-- json_build_array, json_build_object, json_object_agg

SELECT json_build_array('a',1,'b',1.2,'c',true,'d',null,'e',json '{"x": 3, "y": [1,2,3]}');

SELECT json_build_object('a',1,'b',1.2,'c',true,'d',null,'e',json '{"x": 3, "y": [1,2,3]}');

SELECT json_build_object(
       'a', json_build_object('b',false,'c',99),
       'd', json_build_object('e',array[9,8,7]::int[],
           'f', (select row_to_json(r) from ( select relkind, oid::regclass as name from pg_class where relname = 'pg_class') r)));


-- empty objects/arrays
SELECT json_build_array();

SELECT json_build_object();

-- make sure keys are quoted
SELECT json_build_object(1,2);

-- keys must be scalar and not null
SELECT json_build_object(null,2);

SELECT json_build_object(r,2) FROM (SELECT 1 AS a, 2 AS b) r;

SELECT json_build_object(json '{"a":1,"b":2}', 3);

SELECT json_build_object('{1,2,3}'::int[], 3);

CREATE TEMP TABLE foo (serial_num int, name text, type text);
INSERT INTO foo VALUES (847001,'t15','GE1043');
INSERT INTO foo VALUES (847002,'t16','GE1043');
INSERT INTO foo VALUES (847003,'sub-alpha','GESS90');

SELECT json_build_object('turbines',json_object_agg(serial_num,json_build_object('name',name,'type',type)))
FROM foo;

-- json_object

-- one dimension
SELECT json_object('{a,1,b,2,3,NULL,"d e f","a b c"}');

-- same but with two dimensions
SELECT json_object('{{a,1},{b,2},{3,NULL},{"d e f","a b c"}}');

-- odd number error
SELECT json_object('{a,b,c}');

-- one column error
SELECT json_object('{{a},{b}}');

-- too many columns error
SELECT json_object('{{a,b,c},{b,c,d}}');

-- too many dimensions error
SELECT json_object('{{{a,b},{c,d}},{{b,c},{d,e}}}');

--two argument form of json_object

select json_object('{a,b,c,"d e f"}','{1,2,3,"a b c"}');

-- too many dimensions
SELECT json_object('{{a,1},{b,2},{3,NULL},{"d e f","a b c"}}', '{{a,1},{b,2},{3,NULL},{"d e f","a b c"}}');

-- mismatched dimensions

select json_object('{a,b,c,"d e f",g}','{1,2,3,"a b c"}');

select json_object('{a,b,c,"d e f"}','{1,2,3,"a b c",g}');

-- null key error

select json_object('{a,b,NULL,"d e f"}','{1,2,3,"a b c"}');

-- empty key error

select json_object('{a,b,"","d e f"}','{1,2,3,"a b c"}');

-- execced max stack depath
select json_in(REPEAT('{"a":[', 100000)::cstring);

-- json_to_record and json_to_recordset

select * from json_to_record('{"a":1,"b":"foo","c":"bar"}',true)
    as x(a int, b text, d text);

select * from json_to_recordset('[{"a":1,"b":"foo","d":false},{"a":2,"b":"bar","c":true}]',false)
    as x(a int, b text, c boolean);

create schema json_agg_schema;
set current_schema to json_agg_schema;

CREATE TABLE jsonaggvalue_object_cfg_t (
    c1 bigint NOT NULL,
    c2 bigint,
    c3 bigint,
    c4 varchar(50) NOT NULL,
    c5 varchar(500),
    c6 numeric(38,10),
    c7 varchar(240),
    c8 bigint,
    c9 bigint,
    c10 varchar(30),
    c11 varchar(30),
    c12 numeric(38,10),
    c13 varchar(500),
    c14 varchar(1) NOT NULL,
    c15 varchar(1) NOT NULL,
    c16 bigint NOT NULL,
    c17 timestamp without time zone NOT NULL,
    c18 bigint NOT NULL,
    c19 timestamp without time zone NOT NULL,
    c20 varchar(100)
)
;

CREATE TABLE jsonaggvalue_object_quality_t (
    c21 bigint NOT NULL,
    c1 bigint NOT NULL,
    c22 varchar(30),
    c23 varchar(50),
    c24 varchar(30),
    c14 varchar(1) NOT NULL,
    c15 varchar(1) NOT NULL,
    c16 bigint NOT NULL,
    c17 timestamp without time zone NOT NULL,
    c18 bigint NOT NULL,
    c19 timestamp without time zone NOT NULL
)
;

CREATE TABLE jsonaggvalue_object_hierarchy_t (
    c3 bigint NOT NULL,
    c25 bigint,
    c26 varchar(30),
    c27 varchar(500),
    c28 numeric(38,10),
    c7 varchar(240),
    c29 bigint,
    c30 bigint,
    c13 varchar(500),
    c14 varchar(1) NOT NULL,
    c15 varchar(1) NOT NULL,
    c16 bigint NOT NULL,
    c17 timestamp without time zone NOT NULL,
    c18 bigint NOT NULL,
    c19 timestamp without time zone NOT NULL
)
;

CREATE TABLE jsonaggvalue_tree_t (
    c31 bigint NOT NULL,
    c32 varchar(30),
    c33 varchar(200),
    c34 varchar(30),
    c35 varchar(240),
    c36 varchar(1),
    c14 varchar(1) NOT NULL,
    c15 varchar(1) NOT NULL,
    c16 bigint NOT NULL,
    c17 timestamp without time zone NOT NULL,
    c18 bigint NOT NULL,
    c19 timestamp without time zone NOT NULL,
    c37 varchar(10),
    c38 integer,
    c39 varchar(30),
    c40 varchar(30),
    c41 numeric(2,0),
    c42 varchar(15),
    c43 numeric(38,18)
)
;

CREATE TABLE jsonaggvalue_tree_object_t (
    c29 bigint NOT NULL,
    c31 bigint,
    c44 bigint,
    c45 numeric(38,10),
    c15 varchar(1) NOT NULL,
    c14 varchar(1) NOT NULL,
    c16 bigint NOT NULL,
    c17 timestamp without time zone NOT NULL,
    c18 bigint NOT NULL,
    c19 timestamp without time zone NOT NULL,
    c7 varchar(240)
)
;

insert into jsonaggvalue_object_cfg_t values 
(15296,15295,16336,3,'cc',null,null,null,null,null,null,null,null,'Y','N',123456789123456,'2022-12-14 14:41:06.628',123456789123456,'2022-12-14 14:41:06.628'),
(15299,15298,16336,3,'价格',null,null,null,null,null,null,null,null,'Y','N',123456789123456,'2022-12-14 14:41:06.628',123456789123456,'2022-12-14 14:41:06.628'),
(15308,15307,16336,3,'啊',null,null,null,null,null,null,null,null,'Y','N',123456789123456,'2022-12-14 15:19:01.517',123456789123456,'2022-12-14 15:19:01.517');

insert into jsonaggvalue_object_quality_t values 
(10108,15296,1,'重要性评分',1,'Y','N',123456789123456,'2022-12-14 14:52:26.397',123456789123456,'2022-12-14 15:19:01.517'),
(10109,15296,2,'测试',2,'Y','N',123456789123456,      '2022-12-14 14:52:26.397',123456789123456,'2022-12-14 15:19:01.517'),
(10110,15296,2,'fdsa',3,'Y','N',123456789123456,      '2022-12-14 14:52:26.397',123456789123456,'2022-12-14 15:19:01.517'),
(10114,15299,2,'fdsa',2,'Y','N',123456789123456,      '2022-12-14 14:52:26.397',123456789123456,'2022-12-14 15:19:01.517'),
(10115,15299,2,'gfds',1,'Y','N',123456789123456,      '2022-12-14 14:52:26.397',123456789123456,'2022-12-14 15:19:01.517'),
(10116,15308,1,'重要性评分',2,'Y','N',123456789123456,'2022-12-14 15:19:01.517',123456789123456,'2022-12-14 15:19:01.517'),
(10117,15308,2,'测试',3,'Y','N',123456789123456,      '2022-12-14 15:19:01.517',123456789123456,'2022-12-14 15:19:01.517'),
(10118,15308,2,'fdsa',4,'Y','N',123456789123456,      '2022-12-14 15:19:01.517',123456789123456,'2022-12-14 15:19:01.517'),
(10119,15308,2,'gfds',4,'Y','N',123456789123456,      '2022-12-14 15:19:01.517',123456789123456,'2022-12-14 15:19:01.517'),
(10111,15296,2,'gfds',4,'Y','N',123456789123456,      '2022-12-14 14:52:26.397',123456789123456,'2022-12-14 15:19:01.517'),
(10112,15299,1,'重要性评分',4,'Y','N',123456789123456,'2022-12-14 14:52:26.397',123456789123456,'2022-12-14 15:19:01.517'),
(10113,15299,2,'测试',3,'Y','N',123456789123456,      '2022-12-14 14:52:26.397',123456789123456,'2022-12-14 15:19:01.517');

insert into jsonaggvalue_tree_t values (101179,'ZAQ123456789123','test000112','Q','test00022',0,'Y','N',123456789123456,'2022-12-14 14:38:36.488',131269511,'2024-01-12 17:56:45.249');

insert into jsonaggvalue_tree_object_t values (101344,101179,106789,null,'N','Y',123456789123456,'2022-12-14 14:38:36.488',123456789123456,'2022-12-14 14:41:06.628');

insert into jsonaggvalue_object_hierarchy_t values (16336,null,-1,'associator',null,null,101344,null,null,'Y','N',123456789123456,'2022-12-14 14:38:36.488',123456789123456,'2022-12-14 14:38:36.488');

WITH RECURSIVE
    value_object_cfg AS (
        SELECT voc.c1 AS h_configure_id,
             voc.c3 AS h_node_id,
             voc.*
         FROM jsonaggvalue_object_cfg_t voc
         WHERE voc.c4 = '3'
           AND voc.c14 = 'Y'
           AND voc.c15 = 'N'
         UNION
         SELECT voct.h_configure_id,
             voct.h_node_id,
             voc.*
         FROM jsonaggvalue_object_cfg_t voc
         INNER JOIN value_object_cfg voct
         ON voct.c2 = voc.c1
         WHERE voc.c14 = 'Y'
           AND voc.c15 = 'N'),
  
    value_object_quality AS (
        SELECT voqt.c1,
             JSON_OBJECT_AGG(CONCAT('analysis_', tmp.c21, '_Id'), voqt.c21)::json AS aidjson,
             JSON_OBJECT_AGG(CONCAT('analysis_', tmp.c21, '_Name'), voqt.c23)::json AS anamejson,
             JSON_OBJECT_AGG(CONCAT('analysis_', tmp.c21, '_Result'), voqt.c24)::json AS aresultjson
         FROM jsonaggvalue_object_quality_t voqt
         INNER JOIN jsonaggvalue_object_cfg_t voct
         ON voct.c1 = voqt.c1
         AND voct.c4 = '3'
         AND voct.c14 = 'Y'
         AND voct.c15 = 'N'
         LEFT JOIN
         (SELECT voc.c3,
              voq.c21,
              voq.c23,
              voq.c22
          FROM jsonaggvalue_object_quality_t voq
          INNER JOIN jsonaggvalue_object_cfg_t voc
          ON voc.c1 = voq.c1
          AND voc.c4 = '-1'
          AND voc.c14 = 'Y'
          AND voc.c15 = 'N'
          WHERE voq.c14 = 'Y'
            AND voq.c15 = 'N') tmp
         ON tmp.c3 = voct.c3
         AND voqt.c22 = tmp.c22
         AND voqt.c23 = tmp.c23
         WHERE voqt.c14 = 'Y'
           AND voqt.c15 = 'N'
         GROUP BY voqt.c1
),
   tmp AS (
      SELECT vt.c31,
          voct.h_configure_id
      FROM jsonaggvalue_tree_t vt
      INNER JOIN jsonaggvalue_tree_object_t vto
      ON vto.c31 = vt.c31
          AND vto.c14 = 'Y'
          AND vto.c15 = 'N'
      INNER JOIN jsonaggvalue_object_hierarchy_t voh
      ON voh.c29 = vto.c29
          AND voh.c26 = '-1'
          AND voh.c14 = 'Y'
          AND voh.c15 = 'N'
      INNER JOIN value_object_cfg voct
      ON voct.h_node_id = voh.c3
          AND voct.c4 != '-1'
          AND voct.c14 = 'Y'
          AND voct.c15 = 'N'
      WHERE vt.c34 = 'Q'
        AND vt.c14 = 'Y'
        AND vt.c15 = 'N'
        AND vt.c31 in (101179)
      GROUP BY vt.c31,
          voct.h_configure_id)
select tmp.*, voqt.* FROM  tmp , value_object_quality voqt where voqt.c1 = tmp.h_configure_id order by c31,h_configure_id;
drop schema json_agg_schema cascade;