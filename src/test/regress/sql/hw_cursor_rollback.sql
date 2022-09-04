create schema hw_cursor_rollback;
set current_schema = hw_cursor_rollback;
create table pl_txn_t(tc1 int,b number(3));
create index tc1_index on pl_txn_t(tc1);

create or replace package savepck is
PROCEDURE SavepointTest(c1 out sys_refcursor);
procedure save2;
end savepck;
/

--case 1  seqscan
explain  (costs off) SELECT tc1 FROM pl_txn_t;

create or replace package body savepck is
PROCEDURE SavepointTest (c1 out sys_refcursor)IS
val INT;
val1 INT;
BEGIN
INSERT INTO pl_txn_t VALUES(1,1);
INSERT INTO pl_txn_t VALUES(2,2);
OPEN c1 for SELECT tc1 FROM pl_txn_t;
FETCH c1 INTO val;
rollback;
raise notice '1';
END;
procedure save2 is
c1 sys_refcursor;
aaa int;
begin
SavepointTest(c1);
raise notice '2';
FETCH c1 INTO aaa;
raise notice '3';
CLOSE c1;
end;
end savepck;
/
call savepck.save2();

--case 2  indexscan
set enable_seqscan = 0;
set enable_indexscan = 1;
set enable_indexonlyscan = 0;
set enable_bitmapscan = 0;
explain  (costs off) SELECT /*+ indexscan(pl_txn_t tc1_index) */ tc1 FROM pl_txn_t where tc1 > 0;

create or replace package body savepck is
PROCEDURE SavepointTest (c1 out sys_refcursor)IS
val INT;
val1 INT;
BEGIN
INSERT INTO pl_txn_t VALUES(1,1);
INSERT INTO pl_txn_t VALUES(2,2);
OPEN c1 for SELECT /*+ indexscan(pl_txn_t tc1_index) */ tc1 FROM pl_txn_t where tc1 > 0;
FETCH c1 INTO val;
rollback;
raise notice '1';
END;
procedure save2 is
c1 sys_refcursor;
aaa int;
begin
SavepointTest(c1);
raise notice '2';
FETCH c1 INTO aaa;
raise notice '3';
CLOSE c1;
end;
end savepck;
/
call savepck.save2();

--case 3  indexonlyscan
set enable_seqscan = 0;
set enable_indexscan = 0;
set enable_indexonlyscan = 1;
set enable_bitmapscan = 0;
explain  (costs off) SELECT /*+ indexonlyscan(pl_txn_t tc1_index) */ tc1 FROM pl_txn_t;

create or replace package body savepck is
PROCEDURE SavepointTest (c1 out sys_refcursor)IS
val INT;
val1 INT;
BEGIN
INSERT INTO pl_txn_t VALUES(1,1);
INSERT INTO pl_txn_t VALUES(2,2);
OPEN c1 for SELECT /*+ indexonlyscan(pl_txn_t tc1_index) */  tc1 FROM pl_txn_t;
FETCH c1 INTO val;
rollback;
raise notice '1';
END;
procedure save2 is
c1 sys_refcursor;
aaa int;
begin
SavepointTest(c1);
raise notice '2';
FETCH c1 INTO aaa;
raise notice '3';
CLOSE c1;
end;
end savepck;
/
call savepck.save2();

--case 4  bitmapscan
set enable_seqscan = 0;
set enable_indexscan = 0;
set enable_indexonlyscan = 0;
set enable_bitmapscan = 1;
explain  (costs off) SELECT tc1 FROM pl_txn_t where tc1 > 0;

create or replace package body savepck is
PROCEDURE SavepointTest (c1 out sys_refcursor)IS
val INT;
val1 INT;
BEGIN
INSERT INTO pl_txn_t VALUES(1,1);
INSERT INTO pl_txn_t VALUES(2,2);
OPEN c1 for SELECT tc1 FROM pl_txn_t where tc1 > 0;
FETCH c1 INTO val;
rollback;
raise notice '1';
END;
procedure save2 is
c1 sys_refcursor;
aaa int;
begin
SavepointTest(c1);
raise notice '2';
FETCH c1 INTO aaa;
raise notice '3';
CLOSE c1;
end;
end savepck;
/
call savepck.save2();

--case 5  tidscan
drop table pl_txn_t;
create table pl_txn_t(tc1 int,b number(3));
create index tc1_index on pl_txn_t(tc1);
explain  (costs off) SELECT tc1 FROM pl_txn_t where ctid = '(0,1)';

create or replace package body savepck is
PROCEDURE SavepointTest (c1 out sys_refcursor)IS
val INT;
val1 tid;
BEGIN
INSERT INTO pl_txn_t VALUES(1,1);
INSERT INTO pl_txn_t VALUES(2,2);
SELECT ctid into val1 FROM pl_txn_t limit 1;
OPEN c1 for SELECT tc1 FROM pl_txn_t where ctid = '(0,1)';
raise notice '----%', val1;
FETCH c1 INTO val;
raise notice '----%', val;
rollback;
raise notice '1';
END;
procedure save2 is
c1 sys_refcursor;
aaa int;
begin
SavepointTest(c1);
raise notice '2';
FETCH c1 INTO aaa;
raise notice '----%', aaa;
raise notice '3';
CLOSE c1;
end;
end savepck;
/
call savepck.save2();

--case 6  Multiple tables: Open after insert，rollback
--case 6-1 seqscan
drop table pl_txn_t;
create table pl_txn_t(tc1 int,b number(3));
insert into pl_txn_t values (1,100);
insert into pl_txn_t values (2,200);
create table pl_txn_t2(tc1 int,b number(3));
insert into pl_txn_t2 values (1,100);
insert into pl_txn_t2 values (2,200);
create index tc1_index on pl_txn_t(tc1);
create index tc1_index2 on pl_txn_t2(tc1);

set enable_seqscan = 1;
set enable_indexscan = 0;
set enable_indexonlyscan = 0;
set enable_bitmapscan = 0;
explain  (costs off) select t1.tc1 from pl_txn_t t1,pl_txn_t2 t2 where t1.tc1=t2.tc1;

create or replace package body savepck is
PROCEDURE SavepointTest (c1 out sys_refcursor)IS
val INT;
val1 INT;
BEGIN
INSERT INTO pl_txn_t VALUES(1,1);
INSERT INTO pl_txn_t VALUES(2,2);
OPEN c1 for select t1.tc1 from pl_txn_t t1,pl_txn_t2 t2 where t1.tc1=t2.tc1;
FETCH c1 INTO val;
rollback;
raise notice '1';
END;
procedure save2 is
c1 sys_refcursor;
aaa int;
begin
SavepointTest(c1);
raise notice '2';
FETCH c1 INTO aaa;
raise notice '3';
CLOSE c1;
end;
end savepck;
/
call savepck.save2();

--case 6-2 indexscan
set enable_seqscan = 0;
set enable_indexscan = 1;
set enable_indexonlyscan = 0;
set enable_bitmapscan = 0;

explain  (costs off) select t1.tc1 from pl_txn_t t1,pl_txn_t2 t2 where t1.tc1=t2.tc1;

create or replace package body savepck is
PROCEDURE SavepointTest (c1 out sys_refcursor)IS
val INT;
val1 INT;
BEGIN
INSERT INTO pl_txn_t VALUES(1,1);
INSERT INTO pl_txn_t VALUES(2,2);
OPEN c1 for select t1.tc1 from pl_txn_t t1,pl_txn_t2 t2 where t1.tc1=t2.tc1;
FETCH c1 INTO val;
rollback;
raise notice '1';
END;
procedure save2 is
c1 sys_refcursor;
aaa int;
begin
SavepointTest(c1);
raise notice '2';
FETCH c1 INTO aaa;
raise notice '3';
CLOSE c1;
end;
end savepck;
/
call savepck.save2();

--case 6-3 indexonlyscan
set enable_seqscan = 0;
set enable_indexscan = 0;
set enable_indexonlyscan = 1;
set enable_bitmapscan = 0;

explain  (costs off) select t1.tc1 from pl_txn_t t1,pl_txn_t2 t2 where t1.tc1=t2.tc1;

create or replace package body savepck is
PROCEDURE SavepointTest (c1 out sys_refcursor)IS
val INT;
val1 INT;
BEGIN
INSERT INTO pl_txn_t VALUES(1,1);
INSERT INTO pl_txn_t VALUES(2,2);
OPEN c1 for select t1.tc1 from pl_txn_t t1,pl_txn_t2 t2 where t1.tc1=t2.tc1;
FETCH c1 INTO val;
rollback;
raise notice '1';
END;
procedure save2 is
c1 sys_refcursor;
aaa int;
begin
SavepointTest(c1);
raise notice '2';
FETCH c1 INTO aaa;
raise notice '3';
CLOSE c1;
end;
end savepck;
/
call savepck.save2();

--case 6-4 bitmapscan
set enable_seqscan = 0;
set enable_indexscan = 0;
set enable_indexonlyscan = 0;
set enable_bitmapscan = 1;

explain  (costs off) select t1.tc1 from pl_txn_t t1,pl_txn_t2 t2 where t1.tc1=t2.tc1;

create or replace package body savepck is
PROCEDURE SavepointTest (c1 out sys_refcursor)IS
val INT;
val1 INT;
BEGIN
INSERT INTO pl_txn_t VALUES(1,1);
INSERT INTO pl_txn_t VALUES(2,2);
OPEN c1 for select t1.tc1 from pl_txn_t t1,pl_txn_t2 t2 where t1.tc1=t2.tc1;
FETCH c1 INTO val;
rollback;
raise notice '1';
END;
procedure save2 is
c1 sys_refcursor;
aaa int;
begin
SavepointTest(c1);
raise notice '2';
FETCH c1 INTO aaa;
raise notice '3';
CLOSE c1;
end;
end savepck;
/
call savepck.save2();

--case 6-5 tidscan
drop table pl_txn_t;
drop table pl_txn_t2;
create table pl_txn_t(tc1 int,b number(3));
create table pl_txn_t2(tc1 int,b number(3));
create index tc1_index on pl_txn_t(tc1);
create index tc1_index2 on pl_txn_t2(tc1);

explain  (costs off) select t1.tc1 from pl_txn_t t1,pl_txn_t2 t2 where t1.tc1=t2.tc1 and t1.ctid = '(0,1)';

create or replace package body savepck is
PROCEDURE SavepointTest (c1 out sys_refcursor)IS
val INT;
val1 tid;
BEGIN
INSERT INTO pl_txn_t VALUES(1,1);
INSERT INTO pl_txn_t VALUES(2,2);
INSERT INTO pl_txn_t2 VALUES(1,1);
INSERT INTO pl_txn_t2 VALUES(2,2);
SELECT ctid into val1 FROM pl_txn_t limit 1;
OPEN c1 for select t1.tc1 from pl_txn_t t1,pl_txn_t2 t2 where t1.tc1=t2.tc1 and t1.ctid = '(0,1)';
raise notice '----%', val1;
FETCH c1 INTO val;
raise notice '----%', val;
rollback;
raise notice '1';
END;
procedure save2 is
c1 sys_refcursor;
aaa int;
begin
SavepointTest(c1);
raise notice '2';
FETCH c1 INTO aaa;
raise notice '----%', aaa;
raise notice '3';
CLOSE c1;
end;
end savepck;
/
call savepck.save2();


drop schema hw_cursor_rollback CASCADE;