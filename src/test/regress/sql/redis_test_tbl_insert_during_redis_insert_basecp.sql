INSERT INTO tbl_insert_during_redis VALUES (generate_series(101, 200), 5);
DELETE INTO tbl_insert_during_redis where i = 105;
INSERT INTO tbl_insert_during_redis VALUES (105, 5);
-- INSERT INTO tbl_insert_during_redis VALUES (150, 5);
