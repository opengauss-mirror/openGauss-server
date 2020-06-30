--cleanup
DROP FOREIGN TABLE bmsql_warehouse;
DROP FOREIGN TABLE bmsql_district;

SET default_tablespace = '';
SET default_with_oids = false;
CREATE FOREIGN TABLE bmsql_warehouse (
  w_id        integer   not null,
  w_ytd       decimal(12,2),
  w_tax       decimal(4,4),
  w_name      varchar(10),
  w_street_1  varchar(20),
  w_street_2  varchar(20),
  w_city      varchar(20),
  w_state     char(2),
  w_zip       char(9),
  primary key (w_id)
) SERVER mot_server;

CREATE FOREIGN TABLE bmsql_district (
  d_w_id       integer       not null,
  d_id         integer       not null,
  d_ytd        decimal(12,2),
  d_tax        decimal(4,4),
  d_next_o_id  integer,
  d_name       varchar(10),
  d_street_1   varchar(20),
  d_street_2   varchar(20),
  d_city       varchar(20),
  d_state      char(2),
  d_zip        char(9),
  primary key (d_w_id, d_id)
) SERVER mot_server;
CREATE index  d_idx1 on  bmsql_district(d_w_id);
INSERT INTO bmsql_district VALUES (1,1, 103.22, .1934, 100, 'AAA', 'FFF', 'DDD', 'XXX', 'IL', '84965');
INSERT INTO bmsql_district VALUES (1,2, 133.22, .1234, 100, 'CCC', 'FFF', 'DDD', 'XXX', 'IL', '84965');
INSERT INTO bmsql_district VALUES (1,3, 133.22, .1234, 100, 'AAA', 'FFF', 'DDD', 'XXX', 'IL', '84965');
INSERT INTO bmsql_district VALUES (1,4, 123.22, .0234, 100, 'BBB', 'FFF', 'DDD', 'XXX', 'IL', '84965');
INSERT INTO bmsql_district VALUES (1,5, 723.22, .1234, 100, 'CCC', 'FFF', 'DDD', 'XXX', 'IL', '84965');
INSERT INTO bmsql_district VALUES (1,6, 123.22, .2234, 100, 'AAA', 'FFF', 'DDD', 'XXX', 'IL', '84965');
INSERT INTO bmsql_district VALUES (1,7, 23.22, .1234, 100, 'VVV', 'FFF', 'DDD', 'XXX', 'IL', '84965');
INSERT INTO bmsql_district VALUES (1,8, 423.22, .05, 100, 'VVV', 'FFF', 'DDD', 'XXX', 'IL', '84965');
INSERT INTO bmsql_district VALUES (1,9, 129.22, .1234, 100, 'BBB', 'FFF', 'DDD', 'XXX', 'IL', '84965');
INSERT INTO bmsql_district VALUES (1,10, 163.22, .2234, 100, 'VVV', 'FFF', 'DDD', 'XXX', 'IL', '84965');

INSERT INTO bmsql_district VALUES (2,1, 123.22, .1234, 100, 'VVV', 'FFF', 'DDD', 'XXX', 'IL', '84965');
INSERT INTO bmsql_district VALUES (2,2, 123.42, .2234, 100, 'VVV', 'FFF', 'DDD', 'XXX', 'IL', '84965');
INSERT INTO bmsql_district VALUES (2,3, 123.22, .1234, 100, 'VVV', 'FFF', 'DDD', 'XXX', 'IL', '84965');

INSERT INTO bmsql_district VALUES (3,1, 123.22, .175, 100, 'VVV', 'FFF', 'DDD', 'XXX', 'IL', '84965');
INSERT INTO bmsql_district VALUES (3,2, 123.42, .1234, 100, 'VVV', 'FFF', 'DDD', 'XXX', 'IL', '84965');
INSERT INTO bmsql_district VALUES (3,3, 123.22, .2234, 100, 'VVV', 'FFF', 'DDD', 'XXX', 'IL', '84965');

INSERT INTO bmsql_warehouse VALUES (1, 300000.00, .1234, 'FirstWare', 'zoqVwMnEDh3ON', 'sooG1kQQ8Pz', 'NvUlyevdvfoZME2q', 'IL', '442111111');
INSERT INTO bmsql_warehouse VALUES (2, 133.22, .0940, 'SecondWare', 'zoqVwMnEDh3ON', 'sooG1kQQ8Pz', 'NvUlyevdvfoZME2q', 'IL', '442111111');
INSERT INTO bmsql_warehouse VALUES (3, 423.22, .2940, 'Y8tGa6iFq', 'zoqVwMnEDh3ON', 'sooG1kQQ8Pz', 'NvUlyevdvfoZME2q', 'IL', '442111111');
INSERT INTO bmsql_warehouse VALUES (4, 100.00, .0940, 'Y8tGa6iFq', 'zoqVwMnEDh3ON', 'sooG1kQQ8Pz', 'NvUlyevdvfoZME2q', 'IL', '442111111');
INSERT INTO bmsql_warehouse VALUES (5, 723.22, .1234, 'Y8tGa6iFq', 'zoqVwMnEDh3ON', 'sooG1kQQ8Pz', 'NvUlyevdvfoZME2q', 'IL', '442111111');

-- Liran's addition:

SELECT * FROM bmsql_district ORDER BY d_id, d_w_id;

SELECT DISTINCT d_id FROM bmsql_district ORDER BY d_id;

SELECT * FROM bmsql_district WHERE d_w_id=1 ORDER BY d_id;

-- issue #10 in isource:
--SELECT * FROM bmsql_district WHERE d_w_id=1 OR d_id=2 ORDER BY d_id, d_w_id;

--The SQL ORDER BY Keyword

SELECT * FROM bmsql_district ORDER BY d_id, d_w_id ASC;

SELECT * FROM bmsql_district ORDER BY d_id, d_w_id DESC;

SELECT * FROM bmsql_district ORDER BY d_ytd, d_tax, d_id DESC;

SELECT * FROM bmsql_district ORDER BY d_ytd, d_tax, d_id DESC;

--The SQL GROUP BY Keyword

SELECT * FROM bmsql_district GROUP BY d_id, d_w_id ORDER BY d_id, d_w_id;

SELECT COUNT(*) FROM bmsql_district GROUP BY d_name ORDER BY d_name ASC;

--SQL MIN() and MAX() Functions

SELECT min(d_id) FROM bmsql_district;

SELECT max(d_id) FROM bmsql_district;

SELECT max(d_id) FROM bmsql_district WHERE d_w_id=1;

SELECT min(d_id) FROM bmsql_district WHERE d_w_id=1;

SELECT max(d_id) FROM bmsql_district WHERE d_tax>0.175;

SELECT min(d_id) FROM bmsql_district WHERE d_tax>0.175;

SELECT min(d_tax) FROM bmsql_district WHERE d_id BETWEEN 2 and 6;

SELECT min(d_tax) FROM bmsql_district WHERE d_id in (2,3,5,10);

-- TODO: use other VALUES.

--The SQL COUNT(), AVG() and SUM() Functions

SELECT COUNT(d_id) FROM bmsql_district WHERE d_tax>0.05;

SELECT COUNT(d_id) FROM bmsql_district WHERE d_tax<0.05;

SELECT COUNT(d_id) FROM bmsql_district;

select avg(d_tax) FROM bmsql_district;

SELECT SUM(d_next_o_id) FROM bmsql_district;

SELECT * FROM bmsql_district WHERE d_id>1 AND d_id<4 ORDER BY d_id, d_w_id;

SELECT d_id FROM bmsql_district WHERE EXISTS
 (SELECT d_tax FROM bmsql_district WHERE d_id>7 AND d_id<9) ORDER BY d_id;
-- TODO: use other VALUES.

BEGIN;
 DELETE FROM bmsql_district where d_name='BBB' and d_id=4;
 DELETE FROM bmsql_district where d_name='AAA';
 SELECT * FROM bmsql_district ORDER BY d_id, d_w_id;
ROLLBACK;
SELECT * FROM bmsql_district ORDER BY d_id, d_w_id;


UPDATE bmsql_district SET d_street_1='elm' WHERE d_name='BBB' OR d_w_id=2;
SELECT * FROM bmsql_district ORDER BY d_id, d_w_id;

SELECT * FROM bmsql_warehouse AS w INNER JOIN bmsql_district AS d ON d.d_ytd=w.w_ytd ORDER BY d_id DESC;

DELETE FROM bmsql_district WHERE d_w_id=1 AND d_id=2;

SELECT * FROM bmsql_district ORDER BY d_id, d_w_id;

DELETE FROM bmsql_district WHERE d_w_id=1;

SELECT * FROM bmsql_district ORDER BY d_id, d_w_id;

--cleanup
DROP FOREIGN TABLE bmsql_warehouse;
DROP FOREIGN TABLE bmsql_district;
