CREATE TABLE delete_test (
    id int PRIMARY KEY,
    a INT,
    b text
);

INSERT INTO delete_test (a) VALUES (10);
INSERT INTO delete_test (a, b) VALUES (50, repeat('x', 10000));
INSERT INTO delete_test (a) VALUES (100);

-- allow an alias to be specified for DELETE's target table
DELETE FROM delete_test AS dt WHERE dt.a > 75;

-- if an alias is specified, don't allow the original table name
-- to be referenced
DELETE FROM delete_test dt WHERE delete_test.a > 25;

SELECT id, a, char_length(b) FROM delete_test ORDER BY id;

-- delete a row with a TOASTed value
DELETE FROM delete_test WHERE a > 25;

SELECT id, a, char_length(b) FROM delete_test ORDER BY id;

DROP TABLE delete_test;

--multiple delete, report error except B format;
create table t_t_mutil_t1(col1 int,col2 int);
create table t_t_mutil_t2(col1 int,col2 int);
delete from t_t_mutil_t1 a,t_t_mutil_t2 b where a.col1=b.col1;
delete a from t_t_mutil_t1 a,t_t_mutil_t2 b where a.col1=b.col1;
delete a,b from t_t_mutil_t1 a,t_t_mutil_t2 b where a.col1=b.col1;