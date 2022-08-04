--
-- REINDEX CONCURRENTLY PARALLEL
--
CREATE TABLE reind_con_tab(id serial primary key, data text);
INSERT INTO reind_con_tab(data) VALUES ('aa');
INSERT INTO reind_con_tab(data) VALUES ('aaa');
INSERT INTO reind_con_tab(data) VALUES ('aaaa');
INSERT INTO reind_con_tab(data) VALUES ('aaaaa');
\d reind_con_tab;

\parallel on
REINDEX TABLE CONCURRENTLY reind_con_tab;
SELECT data FROM reind_con_tab WHERE id =3;
\parallel off

\parallel on
REINDEX TABLE CONCURRENTLY reind_con_tab;
UPDATE reind_con_tab SET data = 'bbbb' WHERE id = 3;
\parallel off

\parallel on
REINDEX TABLE CONCURRENTLY reind_con_tab;
INSERT INTO reind_con_tab(data) VALUES('cccc');
\parallel off

\parallel on
REINDEX TABLE CONCURRENTLY reind_con_tab;
DELETE FROM reind_con_tab WHERE data = 'aaa';
\parallel off

SELECT * FROM reind_con_tab;
\d reind_con_tab;
DROP TABLE reind_con_tab;