-- case 1
CREATE TABLE tx_record_registry (
int_id BIGINT,
tx_id VARCHAR(50),
tx_flag INT
);

CREATE TABLE addr_segm_a (
int_id BIGINT,
stateflag INT,
name VARCHAR(100),
create_time TIMESTAMP
);

CREATE TABLE addr_segm_b (
int_id BIGINT,
stateflag INT,
name VARCHAR(100),
create_time TIMESTAMP
);

CREATE INDEX idx_tx_registry_tx_id0 ON tx_record_registry(tx_id, tx_flag);
CREATE INDEX idx_addr_segm_id_state1 ON addr_segm_a(int_id, stateflag);
CREATE INDEX idx_addr_segm_id_state2 ON addr_segm_b(int_id, stateflag);

CREATE OR REPLACE VIEW V_ADDR_SEGM_UNIQUE_NEW AS
SELECT int_id, stateflag, name, create_time, 'A' as source FROM addr_segm_a
WHERE stateflag = 0
UNION ALL
SELECT int_id, stateflag, name, create_time, 'B' as source FROM addr_segm_b
WHERE stateflag = 0;

INSERT INTO tx_record_registry VALUES
(1089160340628635648, '1089155245382361088', 1),
(1089160470784761856, '1089155245382361088', 2),
(1089160542693425152, '1089155245382361088', 5),
(1999999999999999999, 'other_tx_id', 1);

INSERT INTO addr_segm_a VALUES
(1089160340628635648, 0, '测试数据A1', NOW()),
(1089160470784761856, 0, '测试数据A2', NOW()),
(1089160542693425152, 0, '测试数据A3', NOW()),
(1111111111111111111, 0, '其他数据A1', NOW()),
(2222222222222222222, 1, '无效数据A', NOW()); -- stateflag != 0

INSERT INTO addr_segm_b VALUES
(1089160340628635648, 0, '测试数据B1', NOW()),
(1089160470784761856, 0, '测试数据B2', NOW()),
(1089160542693425152, 0, '测试数据B3', NOW()),
(3333333333333333333, 0, '其他数据B1', NOW()),
(4444444444444444444, 1, '无效数据B', NOW()); -- stateflag != 0

INSERT INTO addr_segm_a
SELECT generate_series,
0,
'Test_' || generate_series,
NOW() - (random() * interval '365 days')
FROM generate_series(1, 1000);

INSERT INTO addr_segm_b
SELECT generate_series + 1000,
0,
'Test_' || (generate_series + 1000),
NOW() - (random() * interval '365 days')
FROM generate_series(1, 1000);

select count(*) from tx_record_registry;
select count(*) from addr_segm_a;
select count(*) from addr_segm_b;

analyse tx_record_registry;
analyse addr_segm_a;
analyse addr_segm_b;

set rewrite_rule = 'none';

-- can not push down
EXPLAIN (COSTS OFF)
SELECT *
FROM V_ADDR_SEGM_UNIQUE_NEW T
WHERE 1 = 1
AND (
stateflag = 0
AND int_id IN (
SELECT int_id
FROM tx_record_registry r
WHERE r.tx_id = '1089155245382361088'
AND tx_flag IN (1, 2, 5)
)
);

SELECT int_id
FROM tx_record_registry r
WHERE r.tx_id = '1089155245382361088'
AND tx_flag IN (1, 2, 5);

-- should push down
EXPLAIN (COSTS OFF)
SELECT *
FROM V_ADDR_SEGM_UNIQUE_NEW T
WHERE 1 = 1
AND stateflag = 0
AND int_id IN (
1089160340628635648,
1089160470784761856,
1089160542693425152
);

-- should push down
EXPLAIN (COSTS OFF)
SELECT /*+set(query_dop 2) set(rewrite_rule predpushforce) predpush(r)*/ *
FROM V_ADDR_SEGM_UNIQUE_NEW T
WHERE 1 = 1
AND (
stateflag = 0
AND int_id IN (
SELECT int_id
FROM tx_record_registry r
WHERE r.tx_id = '1089155245382361088'
AND tx_flag IN (1, 2, 5)
)
);

-- should push down
EXPLAIN (COSTS OFF)
SELECT /*+set(rewrite_rule predpushforce) predpush(r)*/ *
FROM V_ADDR_SEGM_UNIQUE_NEW T
WHERE 1 = 1
AND (
stateflag = 0
AND int_id IN (
SELECT int_id
FROM tx_record_registry r
WHERE r.tx_id = '1089155245382361088'
AND tx_flag IN (1, 2, 5)
)
);

-- should push down, table not specified
set rewrite_rule = 'predpushforce';
EXPLAIN (COSTS OFF)
SELECT *
FROM V_ADDR_SEGM_UNIQUE_NEW T
WHERE 1 = 1
AND (
stateflag = 0
AND int_id IN (
SELECT int_id
FROM tx_record_registry r
WHERE r.tx_id = '1089155245382361088'
AND tx_flag IN (1, 2, 5)
)
);

drop index idx_addr_segm_id_state1;
drop index idx_addr_segm_id_state2;

-- should not push down, table not specified, but no index
set rewrite_rule = 'predpushforce';
EXPLAIN (COSTS OFF)
SELECT *
FROM V_ADDR_SEGM_UNIQUE_NEW T
WHERE 1 = 1
AND (
stateflag = 0
AND int_id IN (
SELECT int_id
FROM tx_record_registry r
WHERE r.tx_id = '1089155245382361088'
AND tx_flag IN (1, 2, 5)
)
);

-- Push into both branches when only A has a matching prefix index.
CREATE INDEX idx_addr_segm_id_state1 ON addr_segm_a(int_id, stateflag);
set rewrite_rule = 'predpushforce';
EXPLAIN (COSTS OFF)
SELECT *
FROM V_ADDR_SEGM_UNIQUE_NEW T
WHERE 1 = 1
AND (
stateflag = 0
AND int_id IN (
SELECT int_id
FROM tx_record_registry r
WHERE r.tx_id = '1089155245382361088'
AND tx_flag IN (1, 2, 5)
)
);


-- Push into both branches when only B has a matching prefix index.
drop index idx_addr_segm_id_state1;
CREATE INDEX idx_addr_segm_id_state2 ON addr_segm_b(int_id, stateflag);
set rewrite_rule = 'predpushforce';
EXPLAIN (COSTS OFF)
SELECT *
FROM V_ADDR_SEGM_UNIQUE_NEW T
WHERE 1 = 1
AND (
stateflag = 0
AND int_id IN (
SELECT int_id
FROM tx_record_registry r
WHERE r.tx_id = '1089155245382361088'
AND tx_flag IN (1, 2, 5)
)
);

-- Push into both branches when both have a matching prefix index.
CREATE INDEX idx_addr_segm_id_state1 ON addr_segm_a(int_id, stateflag);
set rewrite_rule = 'predpushforce';
EXPLAIN (COSTS OFF)
SELECT *
FROM V_ADDR_SEGM_UNIQUE_NEW T
WHERE 1 = 1
AND (
stateflag = 0
AND int_id IN (
SELECT int_id
FROM tx_record_registry r
WHERE r.tx_id = '1089155245382361088'
AND tx_flag IN (1, 2, 5)
)
);

DROP TABLE IF EXISTS tx_record_registry2;
CREATE TABLE tx_record_registry2 (
    int_id BIGINT,
    tx_id VARCHAR(50),
    tx_flag INT
);
CREATE INDEX idx_tx_registry2_tx_id ON tx_record_registry2(tx_id, tx_flag);

INSERT INTO tx_record_registry2 VALUES
(1089160340628635648, '2009155245382361099', 3),
(1089160470784761856, '2009155245382361099', 4),
(1111111111111111111, '2009155245382361099', 3),
(2999999999999999999, 'other_tx_id2', 3);

ANALYZE tx_record_registry2;

EXPLAIN (COSTS OFF)
SELECT /*+set(rewrite_rule predpushforce) predpush(r1 r2)*/ *
FROM V_ADDR_SEGM_UNIQUE_NEW T
WHERE 1 = 1
AND stateflag = 0
AND (
    int_id IN (
        SELECT int_id
        FROM tx_record_registry r1
        WHERE r1.tx_id = '1089155245382361088'
        AND r1.tx_flag IN (1, 2, 5)
    )
    AND
    int_id IN (
        SELECT int_id
        FROM tx_record_registry2 r2
        WHERE r2.tx_id = '2009155245382361099'
        AND r2.tx_flag IN (3, 4)
    )
);

EXPLAIN (COSTS OFF)
SELECT /*+set(rewrite_rule predpushforce)*/ *
FROM V_ADDR_SEGM_UNIQUE_NEW T
WHERE 1 = 1
AND stateflag = 0
AND (
    int_id IN (
        SELECT int_id
        FROM tx_record_registry r1
        WHERE r1.tx_id = '1089155245382361088'
        AND r1.tx_flag IN (1, 2, 5)
    )
    AND
    int_id IN (
        SELECT int_id
        FROM tx_record_registry2 r2
        WHERE r2.tx_id = '2009155245382361099'
        AND r2.tx_flag IN (3, 4)
    )
);

set rewrite_rule = 'predpushforce';
EXPLAIN (COSTS OFF)
SELECT *
FROM V_ADDR_SEGM_UNIQUE_NEW T
WHERE 1 = 1
AND (
stateflag = 0
AND int_id IN (
SELECT int_id
FROM tx_record_registry r1
WHERE r1.tx_id = '1089155245382361088'
AND r1.tx_flag IN (1, 2, 5)
)
AND
int_id IN (
SELECT int_id
FROM tx_record_registry2 r2
WHERE r2.tx_id = '2009155245382361099'
AND r2.tx_flag IN (3, 4)
)
);

CREATE TABLE tx_record_registry_multi (
    int_id BIGINT,
    tx_id VARCHAR(50),
    tx_flag INT
);

CREATE INDEX idx_tx_registry_multi_tx
ON tx_record_registry_multi(tx_id, tx_flag);

INSERT INTO tx_record_registry_multi VALUES
(1089160340628635648, '2009155245382361099', 3),
(1089160470784761856, '2009155245382361099', 4),
(1111111111111111111, '2009155245382361099', 3),
(2999999999999999999, 'other_tx_id2', 3);

ANALYZE tx_record_registry;
ANALYZE tx_record_registry_multi;
ANALYZE addr_segm_a;
ANALYZE addr_segm_b;

SET rewrite_rule = 'none';

EXPLAIN (COSTS OFF)
SELECT /*+ set(rewrite_rule predpushforce) predpush(r1 r2) */
       t.*
FROM V_ADDR_SEGM_UNIQUE_NEW t
JOIN tx_record_registry r1
  ON t.int_id = r1.int_id
JOIN tx_record_registry_multi r2
  ON t.int_id = r2.int_id
WHERE t.stateflag = 0
  AND r1.tx_id = '1089155245382361088'
  AND r1.tx_flag IN (1, 2, 5)
  AND r2.tx_id = '2009155245382361099'
  AND r2.tx_flag IN (3, 4);

EXPLAIN (COSTS OFF)
SELECT /*+ set(rewrite_rule predpushforce)*/
       t.*
FROM V_ADDR_SEGM_UNIQUE_NEW t
JOIN tx_record_registry r1
  ON t.int_id = r1.int_id
JOIN tx_record_registry_multi r2
  ON t.int_id = r2.int_id
WHERE t.stateflag = 0
  AND r1.tx_id = '1089155245382361088'
  AND r1.tx_flag IN (1, 2, 5)
  AND r2.tx_id = '2009155245382361099'
  AND r2.tx_flag IN (3, 4);

-- case 2
CREATE TABLE test01 (
object_id INT,
data VARCHAR(50)
);

CREATE TABLE test02 (
object_id INT,
owner VARCHAR(50),
name VARCHAR(50)
);

INSERT INTO test01 SELECT generate_series(1, 1000), 'data' || generate_series(1, 3000);

INSERT INTO test02 (object_id, owner, name)
SELECT
gs,
CASE
WHEN random() < 0.1 THEN 'SCOTT' -- 10%的概率为SCOTT
ELSE 'OTHER'
END,
'name' || gs
FROM generate_series(1, 100) gs;

CREATE INDEX idx_test02_owner ON test02(owner);
CREATE INDEX idx_test01_object_id ON test01(object_id);

select count(*) from test01;
select count(*) from test02;

analyse test01;
analyse test02;

set rewrite_rule = 'none';

-- can not push down
EXPLAIN (COSTS OFF)
SELECT
t1.object_id, t2.cnt
FROM test02 t1
LEFT JOIN (
SELECT object_id, COUNT(*) AS cnt
FROM test01 t2
GROUP BY object_id
) t2 ON (t1.object_id = t2.object_id)
WHERE t1.owner = 'SCOTT';

-- should push down
EXPLAIN (COSTS OFF)
SELECT /*+set(query_dop 2) set(rewrite_rule predpushforce) predpush(t1)*/
t1.object_id, t2.cnt
FROM test02 t1
LEFT JOIN (
SELECT object_id, COUNT(*) AS cnt
FROM test01 t2
GROUP BY object_id
) t2 ON (t1.object_id = t2.object_id)
WHERE t1.owner = 'SCOTT';

-- should push down
EXPLAIN (COSTS OFF)
SELECT /*+set(rewrite_rule predpushforce) predpush(t1)*/
t1.object_id, t2.cnt
FROM test02 t1
LEFT JOIN (
SELECT object_id, COUNT(*) AS cnt
FROM test01 t2
GROUP BY object_id
) t2 ON (t1.object_id = t2.object_id)
WHERE t1.owner = 'SCOTT';

-- should push down
EXPLAIN (COSTS OFF)
SELECT /*+set(rewrite_rule predpushforce) predpush(t1, t2)*/
t1.object_id, t2.cnt
FROM test02 t1
LEFT JOIN (
SELECT object_id, COUNT(*) AS cnt
FROM test01 t2
GROUP BY object_id
) t2 ON (t1.object_id = t2.object_id)
WHERE t1.owner = 'SCOTT';

-- should push down, table not specified
set rewrite_rule = 'predpushforce';
EXPLAIN (COSTS OFF)
SELECT
t1.object_id, t2.cnt
FROM test02 t1
LEFT JOIN (
SELECT object_id, COUNT(*) AS cnt
FROM test01 t2
GROUP BY object_id
) t2 ON (t1.object_id = t2.object_id)
WHERE t1.owner = 'SCOTT';

-- case 3
CREATE TABLE rme_eqp (
eqp_id INT PRIMARY KEY,
eqp_no VARCHAR(50),
eqp_name VARCHAR(100),
speciality_id INT,
res_type_id INT,
eqp_type_id INT,
posit_id INT,
delete_state CHAR(1),
delete_time TIMESTAMP,
region_id INT
);

CREATE TABLE trs_trs_ne (
trs_ne_id INT PRIMARY KEY,
ne_no VARCHAR(50),
ne_name VARCHAR(100),
ne_type INT,
room_id INT,
delete_state CHAR(1),
delete_time TIMESTAMP
);

CREATE TABLE spc_room (
room_id INT PRIMARY KEY,
china_name VARCHAR(100),
delete_state CHAR(1),
region_id INT
);

INSERT INTO rme_eqp (eqp_id, eqp_no, eqp_name, speciality_id, res_type_id, eqp_type_id, posit_id, delete_state, delete_time, region_id)

SELECT
gs,
'EQP_' || gs,
'设备_' || gs,
floor(random() * 10)::int,
floor(random() * 10)::int,
floor(random() * 10)::int,
CASE WHEN random() < 0.8 THEN floor(random() * 1000)::int ELSE NULL END,
CASE WHEN gs <= 500 THEN '1' ELSE '0' END,
CASE WHEN random() < 0.1 THEN now() - interval '1 day' * floor(random() * 100)::int ELSE NULL END,
floor(random() * 5)::int
FROM generate_series(1, 1000) gs;

INSERT INTO spc_room (room_id, china_name, delete_state, region_id)
SELECT
gs,
CASE
WHEN gs <= 5 THEN '特定机房'
ELSE '机房_' || gs
END,
CASE WHEN random() < 0.8 THEN '0' ELSE '1' END,
floor(random() * 5)::int
FROM generate_series(1, 100) gs;

INSERT INTO trs_trs_ne (trs_ne_id, ne_no, ne_name, ne_type, room_id, delete_state, delete_time)
SELECT
gs,
'NE_' || gs,
'网元_' || gs,
floor(random() * 10)::int,
floor(random() * 100)::int,
CASE WHEN random() < 0.9 THEN '0' ELSE '1' END,
CASE WHEN random() < 0.1 THEN now() - interval '1 day' * floor(random() * 100)::int ELSE NULL END
FROM generate_series(1, 500) gs;

CREATE INDEX idx_rme_eqp_delete_state ON rme_eqp(delete_state);
CREATE INDEX idx_rme_eqp_posit_id ON rme_eqp(posit_id);

CREATE INDEX idx_trs_trs_ne_delete_state ON trs_trs_ne(delete_state);
CREATE INDEX idx_trs_trs_ne_room_id ON trs_trs_ne(room_id);

CREATE INDEX idx_spc_room_delete_state ON spc_room(delete_state);
CREATE INDEX idx_spc_room_china_name ON spc_room(china_name);

CREATE OR REPLACE VIEW VW_RME_EQP AS
SELECT
rme_eqp.eqp_id,
rme_eqp.eqp_no,
rme_eqp.eqp_name,
rme_eqp.speciality_id,
rme_eqp.res_type_id AS eqp_type_id,
rme_eqp.posit_id,
rme_eqp.res_type_id,
rme_eqp.delete_state,
rme_eqp.delete_time,
rme_eqp.region_id
FROM rme_eqp
WHERE rme_eqp.delete_state = '0'
UNION ALL
SELECT
a.trs_ne_id AS eqp_id,
a.ne_no AS eqp_no,
a.ne_name AS eqp_name,
50 AS speciality_id,
a.ne_type AS eqp_type_id,
a.room_id AS posit_id,
1053 AS res_type_id,
a.delete_state,
a.delete_time,
b.region_id
FROM trs_trs_ne a
JOIN spc_room b ON a.room_id = b.room_id
WHERE a.delete_state = '0';

ANALYZE rme_eqp;
ANALYZE trs_trs_ne;
ANALYZE spc_room;

-- should not push down
SET rewrite_rule = 'none';
EXPLAIN (COSTS OFF)
SELECT *
FROM VW_RME_EQP v1
LEFT JOIN (
SELECT *
FROM SPC_ROOM
WHERE NVL(DELETE_STATE, '0') = '0'
) B1 ON v1.POSIT_ID = B1.ROOM_ID
WHERE B1.CHINA_NAME = '特定机房';

-- should push down,  table not specified
SET rewrite_rule = 'predpushforce';
EXPLAIN (COSTS OFF)
SELECT *
FROM VW_RME_EQP v1
LEFT JOIN (
SELECT *
FROM SPC_ROOM
WHERE NVL(DELETE_STATE, '0') = '0'
) B1 ON v1.POSIT_ID = B1.ROOM_ID
WHERE B1.CHINA_NAME = '特定机房';

-- should push down
SET rewrite_rule = 'none';
EXPLAIN (COSTS OFF)
SELECT /*+set(rewrite_rule predpushforce) predpush(spc_room)*/ *
FROM VW_RME_EQP v1
LEFT JOIN (
SELECT *
FROM SPC_ROOM
WHERE NVL(DELETE_STATE, '0') = '0'
) B1 ON v1.POSIT_ID = B1.ROOM_ID
WHERE B1.CHINA_NAME = '特定机房';

-- case 4
CREATE TABLE a_apply_detail_cutover (
int_id INT PRIMARY KEY,
stateflag NUMERIC,
apply_id INT,
mroom_id_b INT,
mroom_id_e INT,
mroom_id_ee INT,
ne_b INT,
apply_data VARCHAR(100),
create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE c_ne_bureau (
int_id INT PRIMARY KEY,
time_stamp TIMESTAMP,
stateflag NUMERIC,
province_id INT,
region_id INT,
city_id INT,
bureau_zh_name VARCHAR(100),
bureau_code VARCHAR(50),
kernel_bureau NUMERIC,
longitude VARCHAR(20),
latitude VARCHAR(20),
site_type NUMERIC
);

CREATE TABLE c_ne_eponpoint (
int_id INT PRIMARY KEY,
time_stamp TIMESTAMP,
stateflag NUMERIC,
province_id INT,
region_id INT,
city_id INT,
zh_label VARCHAR(100),
epon_code VARCHAR(50),
location_desc TEXT
);

CREATE TABLE c_ne_customer_site (
int_id INT PRIMARY KEY,
time_stamp TIMESTAMP,
stateflag NUMERIC,
province_id INT,
region_id INT,
city_id INT,
zh_label VARCHAR(100),
customer_code VARCHAR(50),
longitude VARCHAR(20),
latitude VARCHAR(20)
);

CREATE TABLE c_ne_remote_innerport (
int_id INT PRIMARY KEY,
time_stamp TIMESTAMP,
stateflag NUMERIC,
province_id INT,
region_id INT,
city_id INT,
machineroom_zh_name VARCHAR(100),
port_code VARCHAR(50),
longitude VARCHAR(20),
latitude VARCHAR(20)
);

CREATE TABLE c_ne_machineroom_main (
int_id INT PRIMARY KEY,
time_stamp TIMESTAMP,
stateflag NUMERIC,
bureau_id INT,
machineroom_zh_name VARCHAR(100),
machineroom_code VARCHAR(50),
address VARCHAR(200),
manager VARCHAR(50)
);

CREATE TABLE c_ne_machineroom_backup (
int_id INT PRIMARY KEY,
time_stamp TIMESTAMP,
stateflag NUMERIC,
bureau_id INT,
machineroom_zh_name VARCHAR(100),
machineroom_code VARCHAR(50),
backup_level INT,
capacity NUMERIC
);

CREATE TABLE c_ne_remote_machineroom (
int_id INT PRIMARY KEY,
time_stamp TIMESTAMP,
stateflag NUMERIC,
bureau_id INT,
machineroom_zh_name VARCHAR(100),
remote_code VARCHAR(50),
area_size NUMERIC,
power_supply VARCHAR(50)
);

CREATE TABLE c_ne_dotne (
int_id INT PRIMARY KEY,
time_stamp TIMESTAMP,
stateflag NUMERIC,
dotne_zh_name VARCHAR(100),
dotne_code VARCHAR(50),
address VARCHAR(200),
contact_person VARCHAR(50),
phone VARCHAR(20)
);

INSERT INTO c_ne_bureau (int_id, time_stamp, stateflag, province_id, region_id, city_id, bureau_zh_name, bureau_code, kernel_bureau, longitude, latitude, site_type) VALUES
(101, '2024-01-01 10:00:00', 0, 1, 11, 111, '北京局点', 'BJ001', 1, '116.4074', '39.9042', 1),
(102, '2024-01-01 10:00:00', 0, 2, 22, 222, '上海局点', 'SH002', 1, '121.4737', '31.2304', 1),
(103, '2024-01-01 10:00:00', 0, 3, 33, 333, '广州局点', 'GZ003', 1, '113.2644', '23.1291', 1),
(104, '2024-01-01 10:00:00', 0, 4, 44, 444, '深圳局点', 'SZ004', 0, '114.0579', '22.5431', 2),
(105, '2024-01-01 10:00:00', 0, 5, 55, 555, '杭州局点', 'HZ005', 0, '120.1551', '30.2741', 2);

INSERT INTO c_ne_eponpoint (int_id, time_stamp, stateflag, province_id, region_id, city_id, zh_label, epon_code, location_desc) VALUES
(201, '2024-01-01 10:00:00', 0, 1, 11, 111, '北京EPON点', 'BJ_EPON001', '北京朝阳区'),
(202, '2024-01-01 10:00:00', 1, 1, 11, 111, '北京EPON点2', 'BJ_EPON002', '北京海淀区'),
(203, '2024-01-01 10:00:00', 0, 2, 22, 222, '上海EPON点', 'SH_EPON001', '上海浦东区');

INSERT INTO c_ne_customer_site (int_id, time_stamp, stateflag, province_id, region_id, city_id, zh_label, customer_code, longitude, latitude) VALUES
(301, '2024-01-01 10:00:00', 0, 1, 11, 111, '客户站点A', 'CUST001', '116.4000', '39.9000'),
(102, '2024-01-01 10:00:00', 0, 1, 11, 111, '重复ID站点', 'CUST002', '116.4100', '39.9100'),
(302, '2024-01-01 10:00:00', 0, 2, 22, 222, '客户站点B', 'CUST003', '121.4700', '31.2300');

INSERT INTO c_ne_remote_innerport (int_id, time_stamp, stateflag, province_id, region_id, city_id, machineroom_zh_name, port_code, longitude, latitude) VALUES
(401, '2024-01-01 10:00:00', 0, 1, 11, 111, '远程内端口A', 'REMOTE001', '116.4200', '39.9200'),
(402, '2024-01-01 10:00:00', 0, 2, 22, 222, '远程内端口B', 'REMOTE002', '121.4800', '31.2400');

INSERT INTO c_ne_machineroom_main (int_id, time_stamp, stateflag, bureau_id, machineroom_zh_name, machineroom_code, address, manager) VALUES
(501, '2024-01-01 10:00:00', 0, 101, '北京主机房A', 'BJ_MR001', '北京朝阳区XX路', '张三'),
(502, '2024-01-01 10:00:00', 0, 102, '上海主机房A', 'SH_MR001', '上海浦东区XX路', '李四'),
(503, '2024-01-01 10:00:00', 0, 103, '广州主机房A', 'GZ_MR001', '广州天河区XX路', '王五');

INSERT INTO c_ne_machineroom_backup (int_id, time_stamp, stateflag, bureau_id, machineroom_zh_name, machineroom_code, backup_level, capacity) VALUES
(601, '2024-01-01 10:00:00', 0, 101, '北京备份机房A', 'BJ_BK001', 1, 1000),
(602, '2024-01-01 10:00:00', 0, 102, '上海备份机房A', 'SH_BK001', 1, 1200),
(603, '2024-01-01 10:00:00', 0, 104, '深圳备份机房A', 'SZ_BK001', 2, 800);

INSERT INTO c_ne_remote_machineroom (int_id, time_stamp, stateflag, bureau_id, machineroom_zh_name, remote_code, area_size, power_supply) VALUES
(701, '2024-01-01 10:00:00', 0, 101, '北京远程机房A', 'BJ_RM001', 500, '双路供电'),
(702, '2024-01-01 10:00:00', 0, 105, '杭州远程机房A', 'HZ_RM001', 600, '单路供电');

INSERT INTO c_ne_dotne (int_id, time_stamp, stateflag, dotne_zh_name, dotne_code, address, contact_person, phone) VALUES
(801, '2024-01-01 10:00:00', 0, '北京网点1', 'BJ_DOT001', '北京朝阳区XX街道', '赵六', '13800138001'),
(802, '2024-01-01 10:00:00', 0, '上海网点1', 'SH_DOT001', '上海浦东区XX街道', '钱七', '13800138002'),
(803, '2024-01-01 10:00:00', 0, '广州网点1', 'GZ_DOT001', '广州天河区XX街道', '孙八', '13800138003');

INSERT INTO a_apply_detail_cutover (int_id, stateflag, apply_id, mroom_id_b, mroom_id_e, mroom_id_ee, ne_b, apply_data) VALUES
(1, 0, 14943649, 501, 502, 503, 801, '申请数据1 - 北京上海广州'),
(2, 0, 14943649, 601, 602, 603, 802, '申请数据2 - 备份机房'),
(3, 0, 14943649, 701, 702, 501, 803, '申请数据3 - 远程机房'),
(4, 1, 14943649, 502, 503, 601, 801, '申请数据4 - 状态非0'),
(5, 0, 14943650, 503, 601, 602, 802, '申请数据5 - 申请ID不同'),
(6, 0, 14943649, 501, 502, 503, 801, '申请数据6 - 与1相同机房网点'),
(7, 0, 14943649, 502, 503, 601, 802, '申请数据7'),
(8, 0, 14943649, 503, 601, 602, 803, '申请数据8'),
(9, 0, 14943649, 601, 602, 603, 801, '申请数据9'),
(10, 0, 14943649, 702, 501, 502, 802, '申请数据10 - 远程机房开始');

CREATE INDEX idx_a_stateflag ON a_apply_detail_cutover(stateflag);
CREATE INDEX idx_a_apply_id ON a_apply_detail_cutover(apply_id);
CREATE INDEX idx_a_int_id ON a_apply_detail_cutover(int_id);
CREATE INDEX idx_a_mroom_id_b ON a_apply_detail_cutover(mroom_id_b);
CREATE INDEX idx_a_mroom_id_e ON a_apply_detail_cutover(mroom_id_e);
CREATE INDEX idx_a_mroom_id_ee ON a_apply_detail_cutover(mroom_id_ee);
CREATE INDEX idx_a_ne_b ON a_apply_detail_cutover(ne_b);

CREATE INDEX idx_bureau_stateflag ON c_ne_bureau(stateflag);
CREATE INDEX idx_bureau_int_id ON c_ne_bureau(int_id);

CREATE INDEX idx_eponpoint_stateflag ON c_ne_eponpoint(stateflag);
CREATE INDEX idx_eponpoint_int_id ON c_ne_eponpoint(int_id);

CREATE INDEX idx_customer_site_stateflag ON c_ne_customer_site(stateflag);
CREATE INDEX idx_customer_site_int_id ON c_ne_customer_site(int_id);

CREATE INDEX idx_remote_innerport_stateflag ON c_ne_remote_innerport(stateflag);
CREATE INDEX idx_remote_innerport_int_id ON c_ne_remote_innerport(int_id);

CREATE INDEX idx_machineroom_main_stateflag ON c_ne_machineroom_main(stateflag);
CREATE INDEX idx_machineroom_main_int_id ON c_ne_machineroom_main(int_id);
CREATE INDEX idx_machineroom_main_bureau_id ON c_ne_machineroom_main(bureau_id);

CREATE INDEX idx_machineroom_backup_stateflag ON c_ne_machineroom_backup(stateflag);
CREATE INDEX idx_machineroom_backup_int_id ON c_ne_machineroom_backup(int_id);
CREATE INDEX idx_machineroom_backup_bureau_id ON c_ne_machineroom_backup(bureau_id);

CREATE INDEX idx_remote_machineroom_stateflag ON c_ne_remote_machineroom(stateflag);
CREATE INDEX idx_remote_machineroom_int_id ON c_ne_remote_machineroom(int_id);
CREATE INDEX idx_remote_machineroom_bureau_id ON c_ne_remote_machineroom(bureau_id);

CREATE OR REPLACE VIEW v_ne_bureau AS
SELECT int_id, time_stamp, stateflag, province_id, region_id, city_id,
bureau_zh_name, kernel_bureau, longitude, latitude, site_type
FROM c_ne_bureau
WHERE stateflag = 0
UNION ALL
SELECT int_id, time_stamp, stateflag, province_id, region_id, city_id,
zh_label AS bureau_zh_name, 0 AS kernel_bureau,
''::VARCHAR AS longitude, ''::VARCHAR AS latitude,
NULL::NUMERIC AS site_type
FROM c_ne_eponpoint
WHERE stateflag = 0
UNION ALL
SELECT a.int_id, a.time_stamp, a.stateflag, a.province_id,
a.region_id, a.city_id, a.zh_label AS bureau_zh_name,
0 AS kernel_bureau, a.longitude, a.latitude,
NULL::NUMERIC AS site_type
FROM c_ne_customer_site a
WHERE a.stateflag = 0
AND NOT EXISTS (
SELECT 1
FROM c_ne_bureau b
WHERE b.stateflag = 0 AND b.int_id = a.int_id
)
UNION ALL
SELECT int_id, time_stamp, stateflag, province_id, region_id, city_id,
machineroom_zh_name AS bureau_zh_name, 0 AS kernel_bureau,
longitude, latitude, NULL::NUMERIC AS site_type
FROM c_ne_remote_innerport
WHERE stateflag = 0;

CREATE OR REPLACE VIEW v_ne_machineroom AS
SELECT int_id, time_stamp, stateflag, bureau_id,
machineroom_zh_name, 'MAIN' AS machineroom_type,
machineroom_code, address AS location_info,
manager AS contact_person
FROM c_ne_machineroom_main
WHERE stateflag = 0
UNION ALL
SELECT int_id, time_stamp, stateflag, bureau_id,
machineroom_zh_name, 'BACKUP' AS machineroom_type,
machineroom_code,
'备份等级:' || backup_level::VARCHAR || ',容量:' || capacity::VARCHAR AS location_info,
NULL::VARCHAR AS contact_person
FROM c_ne_machineroom_backup
WHERE stateflag = 0
UNION ALL
SELECT int_id, time_stamp, stateflag, bureau_id,
machineroom_zh_name, 'REMOTE' AS machineroom_type,
remote_code AS machineroom_code,
'面积:' || area_size::VARCHAR || '㎡,供电:' || power_supply AS location_info,
NULL::VARCHAR AS contact_person
FROM c_ne_remote_machineroom
WHERE stateflag = 0;

ANALYZE a_apply_detail_cutover;
ANALYZE c_ne_bureau;
ANALYZE c_ne_eponpoint;
ANALYZE c_ne_customer_site;
ANALYZE c_ne_remote_innerport;
ANALYZE c_ne_machineroom_main;
ANALYZE c_ne_machineroom_backup;
ANALYZE c_ne_remote_machineroom;
ANALYZE c_ne_dotne;

set rewrite_rule = 'none';

-- can not push down
EXPLAIN (COSTS OFF)
SELECT
A.int_id AS ID,
e2.int_id AS bbureau,
e2.bureau_zh_name AS bbureaux,
f2.int_id AS eebureau,
f2.bureau_zh_name AS eebureaux,
g2.int_id AS ebureau,
g2.bureau_zh_name AS ebureaux
FROM
a_apply_detail_cutover A
LEFT JOIN v_ne_machineroom e ON e.int_id = A.mroom_id_b
LEFT JOIN v_ne_machineroom f ON f.int_id = A.mroom_id_e
LEFT JOIN v_ne_machineroom G ON G.int_id = A.mroom_id_ee
LEFT JOIN v_ne_bureau e2 ON e.bureau_id = e2.int_id
LEFT JOIN v_ne_bureau f2 ON f.bureau_id = f2.int_id
LEFT JOIN v_ne_bureau g2 ON G.bureau_id = g2.int_id
WHERE
A.stateflag = 0
AND A.apply_id = 14943649
ORDER BY
A.int_id;

-- can push down
EXPLAIN (COSTS OFF)
SELECT /*+set(query_dop 2) set(rewrite_rule predpushforce) predpush(A)*/
A.int_id AS ID,
e2.int_id AS bbureau,
e2.bureau_zh_name AS bbureaux,
f2.int_id AS eebureau,
f2.bureau_zh_name AS eebureaux,
g2.int_id AS ebureau,
g2.bureau_zh_name AS ebureaux
FROM
a_apply_detail_cutover A
LEFT JOIN v_ne_machineroom e ON e.int_id = A.mroom_id_b
LEFT JOIN v_ne_machineroom f ON f.int_id = A.mroom_id_e
LEFT JOIN v_ne_machineroom G ON G.int_id = A.mroom_id_ee
LEFT JOIN v_ne_bureau e2 ON e.bureau_id = e2.int_id
LEFT JOIN v_ne_bureau f2 ON f.bureau_id = f2.int_id
LEFT JOIN v_ne_bureau g2 ON G.bureau_id = g2.int_id
WHERE
A.stateflag = 0
AND A.apply_id = 14943649
ORDER BY
A.int_id;

-- should push down, table not specified
EXPLAIN (COSTS OFF)
SELECT /*+set(query_dop 2) set(rewrite_rule predpushforce)*/
A.int_id AS ID,
e2.int_id AS bbureau,
e2.bureau_zh_name AS bbureaux,
f2.int_id AS eebureau,
f2.bureau_zh_name AS eebureaux,
g2.int_id AS ebureau,
g2.bureau_zh_name AS ebureaux
FROM
a_apply_detail_cutover A
LEFT JOIN v_ne_machineroom e ON e.int_id = A.mroom_id_b
LEFT JOIN v_ne_machineroom f ON f.int_id = A.mroom_id_e
LEFT JOIN v_ne_machineroom G ON G.int_id = A.mroom_id_ee
LEFT JOIN v_ne_bureau e2 ON e.bureau_id = e2.int_id
LEFT JOIN v_ne_bureau f2 ON f.bureau_id = f2.int_id
LEFT JOIN v_ne_bureau g2 ON G.bureau_id = g2.int_id
WHERE
A.stateflag = 0
AND A.apply_id = 14943649
ORDER BY
A.int_id;

-- can not push down
EXPLAIN (COSTS OFF)
SELECT
*
FROM
a_apply_detail_cutover A
LEFT JOIN v_ne_machineroom e ON e.int_id = A.mroom_id_b
LEFT JOIN v_ne_machineroom f ON f.int_id = A.mroom_id_e
LEFT JOIN v_ne_machineroom G ON G.int_id = A.mroom_id_ee
---LEFT JOIN v_ne_bureau e2 ON e.bureau_id = e2.int_id
---LEFT JOIN v_ne_bureau f2 ON f.bureau_id = f2.int_id
---LEFT JOIN v_ne_bureau g2 ON G.bureau_id = g2.int_id
LEFT JOIN c_ne_dotne h ON h.int_id = A.ne_b
WHERE
A.stateflag = 0
AND A.apply_id = 14943649
ORDER BY
A.int_id;

-- can push down
EXPLAIN (COSTS OFF)
SELECT /*+set(query_dop 2) set(rewrite_rule predpushforce) predpush(A)*/
*
FROM
a_apply_detail_cutover A
LEFT JOIN v_ne_machineroom e ON e.int_id = A.mroom_id_b
LEFT JOIN v_ne_machineroom f ON f.int_id = A.mroom_id_e
LEFT JOIN v_ne_machineroom G ON G.int_id = A.mroom_id_ee
---LEFT JOIN v_ne_bureau e2 ON e.bureau_id = e2.int_id
---LEFT JOIN v_ne_bureau f2 ON f.bureau_id = f2.int_id
---LEFT JOIN v_ne_bureau g2 ON G.bureau_id = g2.int_id
LEFT JOIN c_ne_dotne h ON h.int_id = A.ne_b
WHERE
A.stateflag = 0
AND A.apply_id = 14943649
ORDER BY
A.int_id;

-- should push down, table not specified
EXPLAIN (COSTS OFF)
SELECT /*+set(query_dop 2) set(rewrite_rule predpushforce)*/
*
FROM
a_apply_detail_cutover A
LEFT JOIN v_ne_machineroom e ON e.int_id = A.mroom_id_b
LEFT JOIN v_ne_machineroom f ON f.int_id = A.mroom_id_e
LEFT JOIN v_ne_machineroom G ON G.int_id = A.mroom_id_ee
---LEFT JOIN v_ne_bureau e2 ON e.bureau_id = e2.int_id
---LEFT JOIN v_ne_bureau f2 ON f.bureau_id = f2.int_id
---LEFT JOIN v_ne_bureau g2 ON G.bureau_id = g2.int_id
LEFT JOIN c_ne_dotne h ON h.int_id = A.ne_b
WHERE
A.stateflag = 0
AND A.apply_id = 14943649
ORDER BY
A.int_id;

-- case 5
DROP TABLE IF EXISTS alarmrms CASCADE;
CREATE SCHEMA IF NOT EXISTS alarmrms;

DROP TABLE IF EXISTS alarmrms.c_ne_enodeb_mid;
CREATE TABLE alarmrms.c_ne_enodeb_mid (
    enodeb_id NUMERIC NOT NULL,
    userlabel VARCHAR,
    zh_label VARCHAR,
    PRIMARY KEY(enodeb_id)
) WITH (ORIENTATION = ROW);

DROP TABLE IF EXISTS alarmrms.c_ne_gnodeb_mid;
CREATE TABLE alarmrms.c_ne_gnodeb_mid (
    enodeb_id NUMERIC NOT NULL,
    zh_label VARCHAR,
    PRIMARY KEY(enodeb_id)
) WITH (ORIENTATION = ROW);

DROP INDEX IF EXISTS alarmrms.c_ne_enodeb_gz_enodeb_id_idx;
CREATE INDEX c_ne_enodeb_gz_enodeb_id_idx
ON alarmrms.c_ne_enodeb_mid((enodeb_id::TEXT));

DROP INDEX IF EXISTS alarmrms.c_ne_gnodeb_gz_enodeb_id_idx;
CREATE INDEX c_ne_gnodeb_gz_enodeb_id_idx
ON alarmrms.c_ne_gnodeb_mid((enodeb_id::TEXT));

DROP INDEX IF EXISTS alarmrms.idx_cne_enodeb_userlabel;
CREATE INDEX idx_cne_enodeb_userlabel
ON alarmrms.c_ne_enodeb_mid(userlabel);

DROP VIEW IF EXISTS alarmrms.v_remote_transform4g5g;
CREATE VIEW alarmrms.v_remote_transform4g5g AS
SELECT
    userlabel AS attr_value,
    zh_label AS obj_name,
    'ENodeB'::TEXT AS obj_type_text
FROM alarmrms.c_ne_enodeb_mid
UNION ALL
SELECT
    enodeb_id::TEXT AS attr_value,
    zh_label AS obj_name,
    'ENodeB'::TEXT AS obj_type_text
FROM alarmrms.c_ne_enodeb_mid
UNION ALL
SELECT
    '00'::TEXT || LOWER(TO_CHAR(enodeb_id::NUMERIC, 'FMXXXXXXXX')) AS attr_value,
    zh_label AS obj_name,
    'ENodeB'::TEXT AS obj_type_text
FROM alarmrms.c_ne_enodeb_mid
UNION ALL
SELECT
    enodeb_id::TEXT AS attr_value,
    zh_label AS obj_name,
    'GNodeB'::TEXT AS obj_type_text
FROM alarmrms.c_ne_gnodeb_mid;

-- The data types are consistent, so the predicate can be pushed down.
CREATE VIEW alarmrms.v_remote_transform4g5g_1 AS
SELECT
    userlabel::TEXT AS attr_value,
    zh_label AS obj_name,
    'ENodeB'::TEXT AS obj_type_text
FROM alarmrms.c_ne_enodeb_mid
UNION ALL
SELECT
    enodeb_id::TEXT AS attr_value,
    zh_label AS obj_name,
    'ENodeB'::TEXT AS obj_type_text
FROM alarmrms.c_ne_enodeb_mid
UNION ALL
SELECT
    '00'::TEXT || LOWER(TO_CHAR(enodeb_id::NUMERIC, 'FMXXXXXXXX')) AS attr_value,
    zh_label AS obj_name,
    'ENodeB'::TEXT AS obj_type_text
FROM alarmrms.c_ne_enodeb_mid
UNION ALL
SELECT
    enodeb_id::TEXT AS attr_value,
    zh_label AS obj_name,
    'GNodeB'::TEXT AS obj_type_text
FROM alarmrms.c_ne_gnodeb_mid;

INSERT INTO alarmrms.c_ne_enodeb_mid (enodeb_id, userlabel, zh_label)
SELECT
    100000000 + i AS enodeb_id,
    'L基站_' || LPAD(i::TEXT, 5, '0') AS userlabel,
    '4G基站_' || LPAD(i::TEXT, 5, '0') AS zh_label
FROM generate_series(1, 2000) AS i;

INSERT INTO alarmrms.c_ne_gnodeb_mid (enodeb_id, zh_label)
SELECT
    200000000 + i AS enodeb_id,
    '5G基站_' || LPAD(i::TEXT, 4, '0') AS zh_label
FROM generate_series(1, 2000) AS i;

ANALYZE alarmrms.c_ne_enodeb_mid;
ANALYZE alarmrms.c_ne_gnodeb_mid;

set rewrite_rule = 'none';

-- can not push down, data types are not consistent
EXPLAIN (COSTS OFF)
SELECT *
FROM alarmrms.v_remote_transform4g5g
WHERE attr_value = 'L新东搬迁_01';

-- can push down, data types are consistent
EXPLAIN (COSTS OFF)
SELECT *
FROM alarmrms.v_remote_transform4g5g_1
WHERE attr_value = 'L新东搬迁_01';

SELECT COUNT(*) FROM alarmrms.v_remote_transform4g5g;
SELECT COUNT(*) FROM alarmrms.v_remote_transform4g5g_1;

-- can not push down, data types are not consistent
EXPLAIN (COSTS OFF)
SELECT /*+set(rewrite_rule predpushforce)*/ *
FROM alarmrms.v_remote_transform4g5g
WHERE attr_value = 'L新东搬迁_01';

-- can push down, data types are consistent
EXPLAIN (COSTS OFF)
SELECT /*+set(rewrite_rule predpushforce)*/ *
FROM alarmrms.v_remote_transform4g5g_1
WHERE attr_value = 'L新东搬迁_01';

-- push down
EXPLAIN (COSTS OFF)
SELECT
    userlabel AS attr_value,
    zh_label AS obj_name,
    'ENodeB'::TEXT AS obj_type_text
FROM alarmrms.c_ne_enodeb_mid WHERE attr_value = 'L新东搬迁_01'
UNION ALL
SELECT
    enodeb_id::TEXT AS attr_value,
    zh_label AS obj_name,
    'ENodeB'::TEXT AS obj_type_text
FROM alarmrms.c_ne_enodeb_mid WHERE attr_value = 'L新东搬迁_01'
UNION ALL
SELECT
    '00'::TEXT || LOWER(TO_CHAR(enodeb_id::NUMERIC, 'FMXXXXXXXX')) AS attr_value,
    zh_label AS obj_name,
    'ENodeB'::TEXT AS obj_type_text
FROM alarmrms.c_ne_enodeb_mid WHERE attr_value = 'L新东搬迁_01'
UNION ALL
SELECT
    enodeb_id::TEXT AS attr_value,
    zh_label AS obj_name,
    'GNodeB'::TEXT AS obj_type_text
FROM alarmrms.c_ne_gnodeb_mid WHERE attr_value = 'L新东搬迁_01' ;

DROP VIEW IF EXISTS alarmrms.v_remote_transform4g5g;
DROP VIEW IF EXISTS alarmrms.v_remote_transform4g5g_1;
DROP TABLE IF EXISTS alarmrms.c_ne_enodeb_mid;
DROP TABLE IF EXISTS alarmrms.c_ne_gnodeb_mid;
DROP SCHEMA IF EXISTS alarmrms;

-- case 6
create table k1(id int,id1 int,id2 int);
create table k2(id int,id1 int,id2 int);
create table k3(id int,id1 int,id2 int);
create table k4(id int,id1 int,id2 int);

insert into k1 select id,id from generate_series(1,1000) id;
insert into k2 select id,id from generate_series(1,1000) id;
insert into k3 select id,id from generate_series(1,10) id;

insert into k1 select id,id from generate_series(1,10000) id;
insert into k2 select id,id from generate_series(1,10000) id;

create index i_id_k1 on k1(id);
create index i_id_k2 on k2(id);

insert into k4 select id,id from generate_series(1,1)id;

analyze k1;
analyze k2;
analyze k3;
analyze k4;

-- should push down, table not specified
EXPLAIN (COSTS OFF)
    select /*+set(rewrite_rule predpushforce)*/
    * from (select * from k1 union select * from k2)kk
    join k3 on kk.id=k3.id left join k4 on 1=1;

insert into k3 select id,id from generate_series(1,10) id;

analyze k3;

-- should push down, table not specified
EXPLAIN (COSTS OFF)
    select /*+set(rewrite_rule predpushforce)*/
    * from (select * from k1 union select * from k2)kk
    join k3 on kk.id=k3.id left join k4 on 1=1;

-- can push down
EXPLAIN (COSTS OFF)
    select /*+set(rewrite_rule predpushforce) predpush(k3)*/
    * from (select * from k1 union select * from k2)kk
    join k3 on kk.id=k3.id left join k4 on 1=1;

-- can not push down
set rewrite_rule = 'none';
EXPLAIN (COSTS OFF)
    select * from (select * from k1 union select * from k2)kk
    join k3 on kk.id=k3.id left join k4 on 1=1;
