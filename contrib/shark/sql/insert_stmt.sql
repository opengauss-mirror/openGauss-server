
CREATE TABLE into_test(id int, name text);
-- 不使用into插入数据
INSERT into_test (id, name)
VALUES (1, 'Alice')
RETURNING id, name;

INSERT into_test (id, name)
VALUES (2, 'Alice')
ON DUPLICATE KEY UPDATE name = VALUES(name);

SELECT * FROM into_test;
DROP TABLE IF EXISTS into_test;