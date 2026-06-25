DROP SCHEMA IF EXISTS test_insert_onconflict CASCADE;
CREATE SCHEMA test_insert_onconflict;
SET CURRENT_SCHEMA TO test_insert_onconflict;

CREATE TABLE t1 (a int UNIQUE PRIMARY KEY, b int not null, c int, d int DEFAULT 0);

INSERT INTO t1 VALUES (0,0,0,0),(1,1,1,1),(2,2,2,2);
--- error: duplicate key update on (a)
INSERT INTO t1 VALUES (0,0,0,0);
--- on error no insert
INSERT INTO t1 VALUES (0,10,10,10) on conflict(a) do nothing;
select * from t1 order by a;
--- should update duplicate key update on (a) 
INSERT INTO t1 VALUES (0,10,10,10) on conflict(a) do update set b=1,c=2,d=3;
select * from t1 order by a;

--- error: (a,b) is no unique constraint
INSERT INTO t1 VALUES (1,1,10,10) on conflict(a,b) do nothing;

create unique index i_t1 on t1(a,b);
--- no error no insert/update
INSERT INTO t1 VALUES (1,1,10,10) on conflict(a,b) do nothing;
--- should update duplicate key update on (a,b) 
INSERT INTO t1 VALUES (1,1,10,10) on conflict(a,b) do update set b=11,c=22,d=33;
select * from t1 order by a;

select 
  a.relname,
  b.indnatts,
  b.indisusable,
  b.indisunique,
  b.indisprimary 
from
  pg_class a,pg_index b 
where 
  a.oid = b.indexrelid 
  and b.indrelid = (select oid from pg_class where relname = 't1')
  order by 1;

--- no error update (2,2,2,2)=>(2,102,2,2)
INSERT INTO t1 VALUES (2,2,2,2),(3,3,3,3),(4,4,4,4) on conflict(a) do update set b=b+100;
select * from t1 order by a;

--- no error only insert (5,5,5,5), (6,6,6,6)
INSERT INTO t1 VALUES (4,4,4,4),(5,5,5,5), (6,6,6,6) on conflict(a,b) do nothing;
select * from t1 order by a;
drop table t1 cascade;

-- test partition table
CREATE TABLE t2 (a int UNIQUE PRIMARY KEY, b int not null, c int, d int DEFAULT 0)
partition by range(a)
(
 partition p1 values less than (10),
 partition p2 values less than (20),
 partition p3 values less than (30),
 partition p5 values less than (MAXVALUE)
);

INSERT INTO t2 VALUES (0,1,0,0),(1,2,1,1),(2,3,2,2);
INSERT INTO t2 VALUES (11,12,11,11),(12,13,12,12),(13,14,13,13);
INSERT INTO t2 VALUES (21,22,21,21),(22,23,22,22),(23,24,23,23);
select * from t2 order by a;

--- error: duplicate key update on (a)
INSERT INTO t2 VALUES (0,1,10,10),(3,4,3,3);
--- no error: duplicate key update on (a) do nothing
INSERT INTO t2 VALUES (0,1,10,10),(3,4,3,3) on conflict(a) do nothing;
select * from t2 order by a;

--- should update duplicate key update on (a) 
INSERT INTO t2 VALUES (3,30,30,30),(11,110,110,110),(14,15,14,14) on conflict(a) do update set c=c+1000,d=d+1000;
select * from t2 order by a;

--- error: (a,b) is no unique constraint
INSERT INTO t2 VALUES (0,1,10,10) on conflict(a,b) do nothing;

create unique index i_t2 on t2(a,b);
--- no error no insert/update
INSERT INTO t2 VALUES (0,1,10,10) on conflict(a,b) do nothing;
--- should update duplicate key update on (a,b) 
INSERT INTO t2 VALUES (0,1,10,10),(50,51,10,11) on conflict(a,b) do update set c=c+1000,d=d+1000;
select * from t2 order by a;
drop table t2 cascade;

-- 创建表
CREATE TABLE t3 ( 
    a INT NOT NULL DEFAULT 0,
    b INT,
    c INT,
    d INT,
    UNIQUE (a, b)  -- 复合唯一约束
);

CREATE OR REPLACE PROCEDURE insert_t3_value(
    update_on_conflict boolean,
    a_v int,
    b_v int,
    c_v int,
    d_v int
)
LANGUAGE plpgsql 
AS $$ 
BEGIN
    IF update_on_conflict THEN
      INSERT INTO t3 (a,b,c,d) VALUES (a_v,b_v,c_v,d_v) on conflict(a,b) do update set a=a+a_v,b=b+b_v;
    ELSE
      INSERT INTO t3 (a,b,c,d) VALUES (a_v,b_v,c_v,d_v) on conflict(a,b) do nothing;
    END IF;
END; 
$$;
/

CALL insert_t3_value(true,1,2,0,0);
SELECT * FROM t3 ORDER BY a,b;
CALL insert_t3_value(true,2,3,1,1);
SELECT * FROM t3 ORDER BY a,b;
CALL insert_t3_value(true,3,4,2,2);
SELECT * FROM t3 ORDER BY a,b;

CALL insert_t3_value(true,2,3,3,3); -- 更新操作
SELECT * FROM t3 ORDER BY a,b;
CALL insert_t3_value(false,3,4,10,10); -- 忽略
SELECT * FROM t3 ORDER BY a,b;

drop function insert_t3_value;
drop table t3 cascade;

CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER,
    order_ref VARCHAR(50),
    status VARCHAR(20) DEFAULT 'pending'
);

CREATE UNIQUE INDEX idx_orders_reference 
ON orders (order_ref) 
WHERE status != 'cancelled';

INSERT INTO orders (customer_id, order_ref, status) 
VALUES (100, 'ORD-001', 'pending'),(101, 'ORD-001', 'cancelled');

INSERT INTO orders (customer_id, order_ref, status) 
VALUES (102, 'ORD-001', 'pending')
ON CONFLICT (order_ref) WHERE status != 'cancelled'
DO UPDATE SET 
    customer_id = EXCLUDED.customer_id,
    status = EXCLUDED.status;

SELECT * FROM orders WHERE order_ref = 'ORD-001';
drop table orders cascade;

-- ON CONFLICT index_predicate
CREATE TABLE active_users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50),
    email VARCHAR(100),
    status VARCHAR(20) DEFAULT 'active'
);

-- only active status using UQIQUE INDEX
CREATE UNIQUE INDEX idx_active_users_username 
ON active_users (username) 
WHERE status = 'active';

INSERT INTO active_users (username, email, status) 
VALUES ('john_active', 'john_active@example.com', 'active');

INSERT INTO active_users (username, email, status) 
VALUES ('john_active', 'john_inactive@example.com', 'inactive');

-- insert same active username
INSERT INTO active_users (username, email, status) 
VALUES ('john_active', 'john_updated@example.com', 'active')  -- conflict
ON CONFLICT (username) WHERE status = 'active'  -- using partion UNION index
DO UPDATE SET 
    email = EXCLUDED.email,
    status = EXCLUDED.status;

SELECT * FROM active_users WHERE username = 'john_active';
drop table active_users cascade;

-- PBE test case
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) UNIQUE,
    email VARCHAR(100) UNIQUE,
    age INTEGER,
    status VARCHAR(20) DEFAULT 'active',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE user_profiles (
    user_id INTEGER,
    profile_type VARCHAR(20),
    data TEXT,
    version INTEGER DEFAULT 1,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id, profile_type)
);

PREPARE basic_upsert AS
INSERT INTO users (username, email, age) 
VALUES ($1, $2, $3)
ON CONFLICT (username)
DO UPDATE SET 
    email = EXCLUDED.email,
    age = EXCLUDED.age,
    updated_at = CURRENT_TIMESTAMP;

EXECUTE basic_upsert('john_doe', 'john@example.com', 25);
EXECUTE basic_upsert('john_doe1', 'john1@example.com', 25);
EXECUTE basic_upsert('john_doe', 'john.updated@example.com', 26);
SELECT id, username, email, age, status FROM users WHERE username = 'john_doe';
DEALLOCATE basic_upsert;

-- 复合键预处理
PREPARE composite_upsert AS
INSERT INTO user_profiles (user_id, profile_type, data) 
VALUES ($1, $2, $3)
ON CONFLICT (user_id, profile_type)
DO UPDATE SET 
    data = user_profiles.data || EXCLUDED.data,
    version = user_profiles.version + 1,
    updated_at = CURRENT_TIMESTAMP;

EXECUTE composite_upsert(1, 'personal', '{"name": "John", "age": 25}');
EXECUTE composite_upsert(1, 'personal', '{"city": "New York", "job": "Engineer"}');
SELECT user_id, profile_type, data, version FROM user_profiles WHERE user_id = 1 AND profile_type = 'personal';
DEALLOCATE composite_upsert;


-- 带条件的预处理语句
CREATE UNIQUE INDEX idx_users_active_username 
ON users (username) 
WHERE status = 'active';

PREPARE conditional_upsert AS
INSERT INTO users (username, email, age, status) 
VALUES ($1, $2, $3, $4)
ON CONFLICT (username) WHERE status = 'active'
DO UPDATE SET 
    email = EXCLUDED.email,
    age = EXCLUDED.age,
    updated_at = CURRENT_TIMESTAMP
WHERE users.status = 'active';

EXECUTE conditional_upsert('active_user', 'active@example.com', 30, 'active');
EXECUTE conditional_upsert('active_user', 'inactive@example.com', 31, 'inactive');
EXECUTE conditional_upsert('active_user', 'active.updated@example.com', 32, 'active');
SELECT id, username, email, age, status FROM users WHERE username = 'active_user';
DEALLOCATE conditional_upsert;

INSERT INTO users (username, email, age, status) 
VALUES ('active_user', 'active.updated@example.com', 32, 'active')
ON CONFLICT ON CONSTRAINT idx_users_active_username
DO UPDATE SET 
    email = EXCLUDED.email,
    age = EXCLUDED.age,
    updated_at = CURRENT_TIMESTAMP
WHERE users.status = 'active';

-- 表达唯一索引预处理语句
CREATE UNIQUE INDEX idx_users_email_lower 
ON users (LOWER(email));

PREPARE expression_upsert AS
INSERT INTO users (username, email, age) 
VALUES ($1, $2, $3)
ON CONFLICT (LOWER(email))
DO UPDATE SET 
    username = EXCLUDED.username,
    age = EXCLUDED.age,
    updated_at = CURRENT_TIMESTAMP;

EXECUTE expression_upsert('alice_user', 'ALICE@EXAMPLE.COM', 28);
EXECUTE expression_upsert('alice_updated', 'alice@example.com', 29);
SELECT id, username, email, age, status FROM users WHERE LOWER(email) = 'alice@example.com';
DEALLOCATE expression_upsert;

-- 批量绑定变量
PREPARE batch_upsert AS
INSERT INTO users (username, email, age) 
VALUES 
    ($1, $2, $3),
    ($4, $5, $6),
    ($7, $8, $9)
ON CONFLICT (username)
DO UPDATE SET 
    email = EXCLUDED.email,
    age = EXCLUDED.age,
    updated_at = CURRENT_TIMESTAMP;

EXECUTE batch_upsert(
    'user1', 'user1@example.com', 25,
    'user2', 'user2@example.com', 30,
    'user3', 'user3@example.com', 35
);

EXECUTE batch_upsert(
    'user1', 'user1.updated@example.com', 26,
    'user4', 'user4@example.com', 40,
    'user2', 'user2.updated@example.com', 31
);

SELECT id, username, email, age, status FROM users WHERE username IN ('user1', 'user2', 'user3', 'user4') 
ORDER BY username;
DEALLOCATE batch_upsert;

-- test insert on conflict not supported on foreign table.
create server foreign_server FOREIGN DATA WRAPPER file_fdw;
CREATE FOREIGN TABLE test_foreign_tbl (id INT)
SERVER foreign_server
OPTIONS (filename '/tmp/foreign_tbl.csv', format 'csv');
insert into test_foreign_tbl values(1) on conflict(emp_id) do nothing;
insert into test_foreign_tbl values(1) on conflict(emp_id) do update set emp_id = 10;
drop foreign table test_foreign_tbl;
drop server foreign_server;

-- INSERT ON CONFLICT DO UPDATE is not supported on column orientated table
create table tbl_col (c1 int primary key, c2 text) with (orientation = column);
insert into tbl_col values (1, 'a001') on conflict(c1) do nothing;
insert into tbl_col values (1, 'a001') on conflict(c1) do update set id= id*10;
drop table tbl_col;

-- NSERT ON CONFLICT DO UPDATE is not supported on view
create table tbl_1(key int4, fruit text);
create view tbl_1_view as select * from tbl_1;
insert into tbl_1_view as t values (1, 'aa') on conflict(key) do nothing;
insert into tbl_1_view as t values (1, 'aa') on conflict(key) do update set id=id*10;
drop view tbl_1_view;
drop table tbl_1;

-- clean
drop table user_profiles cascade;
drop table users cascade;
DROP SCHEMA IF EXISTS test_insert_onconflict CASCADE;
