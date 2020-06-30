--
-- MERGE INTO, privileges test
--

-- initial
CREATE USER merge_privs PASSWORD 'merge@123';
CREATE USER merge_no_privs PASSWORD 'merge@123';

CREATE FOREIGN TABLE merge_privs_target (id integer, balance integer) SERVER mot_server ;
CREATE FOREIGN TABLE merge_privs_source (id integer, delta integer) SERVER mot_server ;

ALTER TABLE merge_privs_target OWNER TO merge_privs;
ALTER TABLE merge_privs_source OWNER TO merge_privs;

CREATE FOREIGN TABLE merge_privs_source2 (id integer, delta integer) SERVER mot_server ;
CREATE FOREIGN TABLE merge_privs_target2 (id integer, balance integer) SERVER mot_server ;

ALTER TABLE merge_privs_target2 OWNER TO merge_no_privs;
ALTER TABLE merge_privs_source2 OWNER TO merge_no_privs;

-- merge_no_privs has INSERT permission for merge_privs_target, but has no SELECT permission for merge_privs_target
GRANT INSERT ON merge_privs_target TO merge_no_privs;
SET SESSION AUTHORIZATION merge_no_privs PASSWORD 'merge@123';

MERGE INTO merge_privs_target
USING merge_privs_source2
ON merge_privs_target.id = merge_privs_source2.id
WHEN MATCHED THEN
UPDATE SET balance = 0;

MERGE INTO merge_privs_target
USING merge_privs_source2
ON merge_privs_target.id = merge_privs_source2.id
WHEN NOT MATCHED THEN
INSERT VALUES (4, NULL);

MERGE INTO merge_privs_target
USING merge_privs_source2
ON merge_privs_target.id = merge_privs_source2.id
WHEN MATCHED THEN
UPDATE SET balance = 0
WHEN NOT MATCHED THEN
INSERT VALUES (4, NULL);

-- merge_no_privs has INSERT, SELECT permissions for merge_privs_target, , but has no UPDATE permission for merge_privs_target
RESET SESSION AUTHORIZATION;
GRANT SELECT ON merge_privs_target TO merge_no_privs;
SET SESSION AUTHORIZATION merge_no_privs PASSWORD 'merge@123';

MERGE INTO merge_privs_target
USING merge_privs_source2
ON merge_privs_target.id = merge_privs_source2.id
WHEN MATCHED THEN
UPDATE SET balance = 0;

MERGE INTO merge_privs_target
USING merge_privs_source2
ON merge_privs_target.id = merge_privs_source2.id
WHEN NOT MATCHED THEN
INSERT VALUES (4, NULL);

MERGE INTO merge_privs_target
USING merge_privs_source2
ON merge_privs_target.id = merge_privs_source2.id
WHEN MATCHED THEN
UPDATE SET balance = 0
WHEN NOT MATCHED THEN
INSERT VALUES (4, NULL);

-- merge_privs has UPDATE permission for merge_privs_target2, but has no SELECT permission for merge_privs_target2
GRANT UPDATE ON merge_privs_target2 TO merge_privs;
SET SESSION AUTHORIZATION merge_privs PASSWORD 'merge@123';

MERGE INTO merge_privs_target2
USING merge_privs_source
ON merge_privs_target2.id = merge_privs_source.id
WHEN MATCHED THEN
UPDATE SET balance = 0;

MERGE INTO merge_privs_target2
USING merge_privs_source
ON merge_privs_target2.id = merge_privs_source.id
WHEN NOT MATCHED THEN
INSERT VALUES (4, NULL);

MERGE INTO merge_privs_target2
USING merge_privs_source
ON merge_privs_target2.id = merge_privs_source.id
WHEN MATCHED THEN
UPDATE SET balance = 0
WHEN NOT MATCHED THEN
INSERT VALUES (4, NULL);

-- merge_privs has UPDATE, INSERT, SELECT permission for merge_privs_target2, but has no SELECT permission for merge_privs_source2
RESET SESSION AUTHORIZATION;
GRANT UPDATE ON merge_privs_target2 TO merge_privs;
GRANT INSERT ON merge_privs_target2 TO merge_privs;
GRANT SELECT ON merge_privs_target2 TO merge_privs;
SET SESSION AUTHORIZATION merge_privs PASSWORD 'merge@123';

MERGE INTO merge_privs_target2
USING merge_privs_source2
ON merge_privs_target2.id = merge_privs_source2.id
WHEN MATCHED THEN
UPDATE SET balance = 0;

MERGE INTO merge_privs_target2
USING merge_privs_source2
ON merge_privs_target2.id = merge_privs_source2.id
WHEN NOT MATCHED THEN
INSERT VALUES (4, NULL);

MERGE INTO merge_privs_target2
USING merge_privs_source2
ON merge_privs_target2.id = merge_privs_source2.id
WHEN MATCHED THEN
UPDATE SET balance = 0
WHEN NOT MATCHED THEN
INSERT VALUES (4, NULL);

-- merge_privs has UPDATE, INSERT, SELECT permission for merge_privs_target2, and has SELECT permission for merge_privs_source2
RESET SESSION AUTHORIZATION;
GRANT UPDATE ON merge_privs_target2 TO merge_privs;
GRANT INSERT ON merge_privs_target2 TO merge_privs;
GRANT SELECT ON merge_privs_target2 TO merge_privs;
GRANT SELECT ON merge_privs_source2 TO merge_privs;
SET SESSION AUTHORIZATION merge_privs PASSWORD 'merge@123';

MERGE INTO merge_privs_target2
USING merge_privs_source2
ON merge_privs_target2.id = merge_privs_source2.id
WHEN MATCHED THEN
UPDATE SET balance = 0;

MERGE INTO merge_privs_target2
USING merge_privs_source2
ON merge_privs_target2.id = merge_privs_source2.id
WHEN NOT MATCHED THEN
INSERT VALUES (4, NULL);

MERGE INTO merge_privs_target2
USING merge_privs_source2
ON merge_privs_target2.id = merge_privs_source2.id
WHEN MATCHED THEN
UPDATE SET balance = 0
WHEN NOT MATCHED THEN
INSERT VALUES (4, NULL);

-- revoke some permissions
RESET SESSION AUTHORIZATION;
REVOKE SELECT ON merge_privs_source2 FROM merge_privs;
SET SESSION AUTHORIZATION merge_privs PASSWORD 'merge@123';

MERGE INTO merge_privs_target2
USING merge_privs_source2
ON merge_privs_target2.id = merge_privs_source2.id
WHEN MATCHED THEN
UPDATE SET balance = 0
WHEN NOT MATCHED THEN
INSERT VALUES (4, NULL);

RESET SESSION AUTHORIZATION;
REVOKE INSERT ON merge_privs_target2 FROM merge_privs;
SET SESSION AUTHORIZATION merge_privs PASSWORD 'merge@123';

MERGE INTO merge_privs_target2
USING merge_privs_source2
ON merge_privs_target2.id = merge_privs_source2.id
WHEN MATCHED THEN
UPDATE SET balance = 0
WHEN NOT MATCHED THEN
INSERT VALUES (4, NULL);


-- clean up
RESET SESSION AUTHORIZATION;
DROP FOREIGN TABLE merge_privs_target;
DROP FOREIGN TABLE merge_privs_target2;
DROP FOREIGN TABLE merge_privs_source;
DROP FOREIGN TABLE merge_privs_source2;
DROP USER merge_privs;
DROP USER merge_no_privs;
