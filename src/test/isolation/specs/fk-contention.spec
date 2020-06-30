setup { CREATE TABLE foo (a int PRIMARY KEY, b text); }
setup { CREATE TABLE bar (a int NOT NULL REFERENCES foo); }
setup { INSERT INTO foo VALUES (42); }

teardown { DROP TABLE bar; }
teardown { DROP TABLE foo CASCADE; }

session "s1"
setup		{ START TRANSACTION; }
step "ins"	{ INSERT INTO bar VALUES (42); }
step "com"	{ COMMIT; }

session "s2"
step "upd"	{ UPDATE foo SET b = 'Hello World'; }
