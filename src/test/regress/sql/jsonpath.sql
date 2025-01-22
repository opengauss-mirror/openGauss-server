DROP SCHEMA IF EXISTS test_jsonpath CASCADE;
CREATE SCHEMA test_jsonpath;
SET CURRENT_SCHEMA TO test_jsonpath ;

CREATE TABLE t (name VARCHAR2(100));
INSERT INTO t VALUES ('[{"first":"John"}, {"middle":"Mark"}, {"last":"Smith"}, 1]');
INSERT INTO t VALUES ('[{"first":"John"}, {"middle":"Mark"}, {"last":"Smith"}, [1, 2, 3]]');
INSERT INTO t VALUES ('[{"first":"Mary"}, {"last":"Jones"}]');
INSERT INTO t VALUES ('[{"first":"Jeff"}, {"last":"Williams"}]');
INSERT INTO t VALUES ('[{"first":"Jean"}, {"middle":"Anne"}, {"last":"Brown"}]');
INSERT INTO t VALUES ('[{"first":"Jean"}, {"middle":"Anne"}, {"middle":"Alice"}, {"last":"Brown"}]');
INSERT INTO t VALUES ('[{"first":1}, {"middle":2}, {"last":3}]');
INSERT INTO t VALUES (NULL);
INSERT INTO t VALUES ('This is not well-formed JSON data');


CREATE TABLE families (family_doc CLOB);
INSERT INTO families
VALUES ('{"family" : {"id":10, "ages":[40,38,12], "address" : {"street" : "10 Main Street"}}}');
INSERT INTO families
VALUES ('{"family" : {"id":11, "ages":[42,40,10,5], "address" : {"street" : "200 East Street", "apt" : 20}}}');
INSERT INTO families
VALUES ('{"family" : {"id":12, "ages":[25,23], "address" : {"street" : "300 Oak Street", "apt" : 10}}}');

INSERT INTO families VALUES ('This is not well-formed JSON data');

-- JsonPath grammar
SELECT name FROM t WHERE JSON_EXISTS(name, '$');
SELECT name FROM t WHERE JSON_EXISTS(name, '$[0]');
SELECT name FROM t WHERE JSON_EXISTS(name, '$[*]');
SELECT name FROM t WHERE JSON_EXISTS(name, '$[99]');
SELECT name FROM t WHERE JSON_EXISTS(name, '$[0,2]');
SELECT name FROM t WHERE JSON_EXISTS(name, '$[2,0,1]'); 
SELECT name FROM t WHERE JSON_EXISTS(name, '$[0 to 2]');
SELECT name FROM t WHERE JSON_EXISTS(name, '$[3 to 3]');
SELECT name FROM t WHERE JSON_EXISTS(name, '$[2 to 0]');
SELECT name FROM t WHERE JSON_EXISTS(name, '$[2].*.*');
SELECT family_doc FROM families WHERE JSON_EXISTS(family_doc, '$.family');
SELECT family_doc FROM families WHERE JSON_EXISTS(family_doc, '$.*');
SELECT name FROM t WHERE JSON_EXISTS(name, '$[3][1]');
SELECT name FROM t WHERE JSON_EXISTS(name, '$[1].last');
SELECT family_doc FROM families WHERE JSON_EXISTS(family_doc, '$.family.address.apt');
SELECT family_doc FROM families WHERE JSON_EXISTS(family_doc, '$.family.ages[2]');

-- syntax error in jsonpath
SELECT name FROM t WHERE JSON_EXISTS(name, '$[-1]');
SELECT name FROM t WHERE JSON_EXISTS(name, '$[0b10]');
SELECT name FROM t WHERE JSON_EXISTS(name, '$[1');
SELECT name FROM t WHERE JSON_EXISTS(name, '$[1+2]');
SELECT name FROM t WHERE JSON_EXISTS(name, '$[0.1]');
SELECT name FROM t WHERE JSON_EXISTS(name, 'NULL');
SELECT name FROM t WHERE JSON_EXISTS(name, NULL);

-- json_exists
SELECT name FROM t WHERE JSON_EXISTS(name, '$[0].first');
SELECT name FROM t WHERE JSON_EXISTS(name, '$[1,2].last');
SELECT name FROM t WHERE JSON_EXISTS(name, '$[1 to 2].last');

SELECT name FROM t WHERE JSON_EXISTS(name, '$[1].first');
SELECT name FROM t WHERE JSON_EXISTS(name, '$[0].first[1]');

SELECT name FROM t WHERE JSON_EXISTS(NULL, '$');
SELECT JSON_EXISTS('[{"first":"John"}, {"middle":"Mark"}, {"last":"Smith"}]', '$[0].first');
SELECT JSON_EXISTS('[{"first":"John"}, {"middle":"Mark"}, {"last":"Smith"}]', '$[2 to 1].*');
SELECT JSON_EXISTS('[{"first":"John"}, {"middle":"Mark"}, {"last":"Smith"}]', '$[*].last');
SELECT JSON_EXISTS('This is not well-formed JSON data', '$[0].first');

-- json_exists with on error
SELECT name FROM t WHERE JSON_EXISTS(name, '$' FALSE ON ERROR);
SELECT name FROM t WHERE JSON_EXISTS(name, '$' TRUE ON ERROR);
SELECT name FROM t WHERE JSON_EXISTS(name, '$' ERROR ON ERROR);

SELECT JSON_EXISTS('This is not well-formed JSON data', '$[0].first' FALSE ON ERROR);
SELECT JSON_EXISTS('This is not well-formed JSON data', '$[0].first' TRUE ON ERROR);
SELECT JSON_EXISTS('This is not well-formed JSON data', '$[0].first' ERROR ON ERROR);

PREPARE stmt1 AS SELECT JSON_EXISTS($1,$2);
EXECUTE stmt1('[{"first":"John"}, {"middle":"Mark"}, {"last":"Smith"}]','$[0].first');
EXECUTE stmt1('[{"first":"John"}, {"middle":"Mark"}, {"last":"Smith"}]','$[0].last');
EXECUTE stmt1('This is not well-formed JSON data','$[0].last');
DEALLOCATE PREPARE stmt1;
PREPARE stmt1 AS SELECT JSON_EXISTS($1,$2 TRUE ON ERROR);
EXECUTE stmt1('This is not well-formed JSON data','$[0].last');
DEALLOCATE PREPARE stmt1;
PREPARE stmt1 AS SELECT JSON_EXISTS($1,$2 FALSE ON ERROR);
EXECUTE stmt1('This is not well-formed JSON data','$[0].last');
DEALLOCATE PREPARE stmt1;
PREPARE stmt1 AS SELECT JSON_EXISTS($1,$2 ERROR ON ERROR);
EXECUTE stmt1('This is not well-formed JSON data','$[0].last');
DEALLOCATE PREPARE stmt1;

-- json_textcontains
SELECT family_doc FROM families WHERE JSON_TEXTCONTAINS(family_doc, '$.family', '10');
SELECT family_doc FROM families WHERE JSON_TEXTCONTAINS(family_doc, '$.family', '25, 5');
SELECT family_doc FROM families WHERE JSON_TEXTCONTAINS(family_doc, '$.family', 'West');
SELECT family_doc FROM families WHERE JSON_TEXTCONTAINS(family_doc, '$.family', 'Oak Street');
SELECT family_doc FROM families WHERE JSON_TEXTCONTAINS(family_doc, '$.family', 'oak street');
SELECT family_doc FROM families WHERE JSON_TEXTCONTAINS(family_doc, '$.family', '25 23, Oak Street');
SELECT family_doc FROM families WHERE JSON_TEXTCONTAINS(family_doc, '$.family', 'Oak Street 10');
SELECT family_doc FROM families WHERE JSON_TEXTCONTAINS(family_doc, '$.family', '12 25 23 300 Oak Street 10');
SELECT family_doc FROM families WHERE JSON_TEXTCONTAINS(family_doc, '$.family.id', 'Oak Street');
SELECT family_doc FROM families WHERE JSON_TEXTCONTAINS(family_doc, '$.family', 'ak street');

PREPARE stmt2 AS SELECT JSON_TEXTCONTAINS($1, $2, $3);
EXECUTE stmt2(NULL, '$.family', 'data');
EXECUTE stmt2('{"family" : {"id":12, "ages":[25,23], "address" : {"street" : "300 Oak Street", "apt" : 10}}}'
             , '$.family', '12');
EXECUTE stmt2('{"family" : {"id":12, "ages":[25,23], "address" : {"street" : "300 Oak Street", "apt" : 10}}}'
             , '$.family', 'K STREET');
EXECUTE stmt2('{"family" : {"id":12, "ages":[25,23], "address" : {"street" : "300 Oak Street", "apt" : 10}}}'
            , NULL, 'data');

SELECT JSON_TEXTCONTAINS('This is not well-formed JSON data', '$.family', 'data');
SELECT JSON_TEXTCONTAINS(NULL, '$.family', 'data');
SELECT JSON_TEXTCONTAINS('{"family" : {"id":12, "ages":[25,23], "address" : {"street" : "300 Oak Street", "apt" : 10}}}'
                        , '$.family', '12');
SELECT JSON_TEXTCONTAINS('{"family" : {"id":12, "ages":[25,23], "address" : {"street" : "300 Oak Street", "apt" : 10}}}'
                        , '$.family', '12, OAK STREET');
SELECT JSON_TEXTCONTAINS('{"family" : {"id":12, "ages":[25,23], "address" : {"street" : "300 Oak Street", "apt" : 10}}}'
                        , '$.family', 'K STREET');
SELECT JSON_TEXTCONTAINS('{"family" : {"id":12, "ages":[25,23], "address" : {"street" : "300 Oak Street", "apt" : 10}}}'
                        , NULL, 'data');

select json_textcontains('{ "zebra" : { "name" : "Marty",   
                       "stripes" : ["Black","White"],  
                       "handler" : "Bob" }}','$','Marty');
select json_textcontains('{ "zebra" : { "name" : "Marty",   
                       "stripes" : ["Black","White"],  
                       "handler" : "Bob" }}','$.zebra.name','Marty');
select json_textcontains('{"family" : {"id":12, "ages":[25,23], "address" : {"street" : "300 Oak Street", "apt" : 10}}}',
                         '$.family.address.street','300');

DROP SCHEMA test_jsonpath CASCADE;