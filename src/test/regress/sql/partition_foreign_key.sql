DROP TABLE IF EXISTS pt2;
DROP TABLE IF EXISTS pt1;
CREATE TABLE pt1(
                d_date date UNIQUE
)partition BY RANGE (d_date) INTERVAL('1 day')
(PARTITION part1 VALUES LESS THAN ('1900-01-01'));

CREATE TABLE pt2(
                d_date date
)partition BY RANGE (d_date) INTERVAL('1 day')
(PARTITION part1 VALUES LESS THAN ('1900-01-01'));

ALTER TABLE pt2 ADD CONSTRAINT test_foreign FOREIGN KEY(d_date)
REFERENCES pt1(d_date) on UPDATE CASCADE ON DELETE CASCADE;

INSERT into pt1 VALUES('1999-01-01'),('1999-01-02'),('1999-01-03'),('1999-01-04');

INSERT into pt2 VALUES('1999-01-01');

INSERT into pt2 VALUES('2000-01-01');

update pt2 set pt2.d_date = '2000-01-01' where pt2.d_date = '1999-01-01';

update pt2 set pt2.d_date = '1999-01-02' where pt2.d_date = '1999-01-01';

SELECT * FROM pt2;

SELECT * FROM pt2 PARTITION(sys_p2);

delete from pt2;
INSERT into pt2 VALUES('1999-01-01'), ('1999-01-03'),('1999-01-04');

INSERT into pt1 VALUES('1999-01-08');

INSERT into pt2 VALUES('1999-01-08');

update pt1 set pt1.d_date = '2000-01-01' where pt1.d_date = '1999-01-08';

delete from pt1 where pt1.d_date = '2000-01-01';

DROP TABLE pt2;
DROP TABLE pt1;

CREATE TABLE pt1(
                d_date date UNIQUE
)partition BY RANGE (d_date) INTERVAL('1 month')
(PARTITION part1 VALUES LESS THAN ('1900-01-01'));

CREATE TABLE pt2(
                d_date date
)partition BY RANGE (d_date) INTERVAL('1 month')
(PARTITION part1 VALUES LESS THAN ('1900-01-01'));

ALTER TABLE pt2 ADD CONSTRAINT test_foreign FOREIGN KEY(d_date)
REFERENCES pt1(d_date) on UPDATE CASCADE ON DELETE CASCADE;

INSERT INTO pt1 VALUES
('1999-01-01'),('1999-01-02'),('1999-01-019'),('1999-01-20'),
('1999-02-01'),('1999-02-02'),('1999-02-19'),('1999-02-20'),
('1999-03-01'),('1999-03-02'),('1999-03-19'),('1999-03-20'),
('1999-04-01'),('1999-04-02'),('1999-04-19'),('1999-04-20'),
('1999-05-01'),('1999-05-02'),('1999-05-19'),('1999-05-20'),
('1999-06-01'),('1999-06-02'),('1999-06-19'),('1999-06-20'),
('1999-07-01'),('1999-07-02'),('1999-07-19'),('1999-07-20'),
('1999-08-01'),('1999-08-02'),('1999-08-19'),('1999-08-20'),
('1999-09-01'),('1999-09-02'),('1999-09-19'),('1999-09-20'),
('1999-10-01'),('1999-10-02'),('1999-10-19'),('1999-10-20'),
('1999-11-01'),('1999-11-02'),('1999-11-19'),('1999-11-20'),
('1999-12-01'),('1999-12-02'),('1999-12-19'),('1999-12-20');

INSERT INTO pt2 VALUES
('1999-01-01'),('1999-01-02'),('1999-01-019'),('1999-01-20'),
('1999-02-01'),('1999-02-02'),('1999-02-19'),('1999-02-20'),
('1999-03-01'),('1999-03-02'),('1999-03-19'),('1999-03-20'),
('1999-04-01'),('1999-04-02'),('1999-04-19'),('1999-04-20'),
('1999-05-01'),('1999-05-02'),('1999-05-19'),('1999-05-20'),
('1999-06-01'),('1999-06-02'),('1999-06-19'),('1999-06-20'),
('1999-07-01'),('1999-07-02'),('1999-07-19'),('1999-07-20'),
('1999-08-01'),('1999-08-02'),('1999-08-19'),('1999-08-20'),
('1999-09-01'),('1999-09-02'),('1999-09-19'),('1999-09-20'),
('1999-10-01'),('1999-10-02'),('1999-10-19'),('1999-10-20'),
('1999-11-01'),('1999-11-02'),('1999-11-19'),('1999-11-20'),
('1999-12-01'),('1999-12-02'),('1999-12-19'),('1999-12-20');

delete from pt2 where pt2.d_date<'1999-03-01';

select * from pt2 where pt2.d_date<'1999-06-01';

delete from pt1 where pt1.d_date<'1999-05-01';

delete from pt2 where pt2.d_date>'1999-11-15' and pt2.d_date<'1999-12-15';
SELECT * FROM pt2 WHERE pt2.d_date>='1999-10-01';

delete from pt1 where pt1.d_date>'1999-10-15' and pt1.d_date<'1999-11-15';
SELECT * FROM pt1 WHERE pt1.d_date>='1999-10-01';

SELECT * FROM pt2 WHERE pt2.d_date>='1999-10-01';

UPDATE pt2 set d_date = d_date - INTERVAL '4' month where d_date>='1999-05-01' and d_date<'1999-06-01';

UPDATE pt1 set d_date = d_date - INTERVAL '4' month where d_date>='1999-05-01' and d_date<'1999-06-01';
SELECT * FROM pt1 WHERE d_date<'1999-07-01';

SELECT * FROM pt1 WHERE d_date<'1999-07-01';

UPDATE pt2 set d_date = d_date - INTERVAL '1' month where d_date>='1999-06-01' and d_date<='1999-07-15';

UPDATE pt1 set d_date = d_date - INTERVAL '1' month where d_date>='1999-06-01' and d_date<='1999-07-15';
SELECT * FROM pt1 WHERE d_date<'1999-08-01';

SELECT * FROM pt2 WHERE d_date<'1999-08-01';

DROP TABLE IF EXISTS pt2;
DROP TABLE IF EXISTS pt1;

CREATE TABLE pt1(
d_date date UNIQUE
)partition BY RANGE (d_date) INTERVAL('1 month')
(PARTITION part1 VALUES LESS THAN ('1900-01-01'))
;

CREATE TABLE pt2(
d_date date,
d_date_backup date
)partition BY RANGE (d_date_backup) INTERVAL('1 month')
(PARTITION part1 VALUES LESS THAN ('1900-01-01'));


ALTER TABLE pt2 ADD CONSTRAINT test_foreign FOREIGN KEY(d_date)
REFERENCES pt1(d_date) on UPDATE SET NULL ON DELETE SET NULL;

INSERT INTO pt1 VALUES
('1999-01-01'),('1999-01-02'),('1999-01-019'),('1999-01-20'),
('1999-02-01'),('1999-02-02'),('1999-02-19'),('1999-02-20'),
('1999-03-01'),('1999-03-02'),('1999-03-19'),('1999-03-20'),
('1999-04-01'),('1999-04-02'),('1999-04-19'),('1999-04-20'),
('1999-05-01'),('1999-05-02'),('1999-05-19'),('1999-05-20'),
('1999-06-01'),('1999-06-02'),('1999-06-19'),('1999-06-20'),
('1999-07-01'),('1999-07-02'),('1999-07-19'),('1999-07-20'),
('1999-08-01'),('1999-08-02'),('1999-08-19'),('1999-08-20'),
('1999-09-01'),('1999-09-02'),('1999-09-19'),('1999-09-20'),
('1999-10-01'),('1999-10-02'),('1999-10-19'),('1999-10-20'),
('1999-11-01'),('1999-11-02'),('1999-11-19'),('1999-11-20'),
('1999-12-01'),('1999-12-02'),('1999-12-19'),('1999-12-20');

INSERT INTO pt2 VALUES
('1999-01-01','1999-01-01'),('1999-01-02','1999-01-02'),
('1999-01-19','1999-01-19'),('1999-01-20','1999-01-20'),
('1999-02-01','1999-02-01'),('1999-02-02','1999-02-02'),
('1999-02-19','1999-02-19'),('1999-02-20','1999-02-20'),
('1999-03-01','1999-03-01'),('1999-03-02','1999-03-02'),
('1999-03-19','1999-03-19'),('1999-03-20','1999-03-20'),
('1999-04-01','1999-04-01'),('1999-04-02','1999-04-02'),
('1999-04-19','1999-04-19'),('1999-04-20','1999-04-20'),
('1999-05-01','1999-05-01'),('1999-05-02','1999-05-02'),
('1999-05-19','1999-05-19'),('1999-05-20','1999-05-20'),
('1999-06-01','1999-06-01'),('1999-06-02','1999-06-02'),
('1999-06-19','1999-06-19'),('1999-06-20','1999-06-20'),
('1999-07-01','1999-07-01'),('1999-07-02','1999-07-02'),
('1999-07-19','1999-07-19'),('1999-07-20','1999-07-20'),
('1999-08-01','1999-08-01'),('1999-08-02','1999-08-02'),
('1999-08-19','1999-08-19'),('1999-08-20','1999-08-20'),
('1999-09-01','1999-09-01'),('1999-09-02','1999-09-02'),
('1999-09-19','1999-09-19'),('1999-09-20','1999-09-20'),
('1999-10-01','1999-10-01'),('1999-10-02','1999-10-02'),
('1999-10-19','1999-10-19'),('1999-10-20','1999-10-20'),
('1999-11-01','1999-11-01'),('1999-11-02','1999-11-02'),
('1999-11-19','1999-11-19'),('1999-11-20','1999-11-20'),
('1999-12-01','1999-12-01'),('1999-12-02','1999-12-02'),
('1999-12-19','1999-12-19'),('1999-12-20','1999-12-20');

insert into pt1 VALUES('2000-01-01'),('2000-01-02'),
('2000-01-19'),('2000-01-20'),
('2000-02-01'),('2000-02-02'),
('2000-02-19'),('2000-02-20');
SELECT * FROM pt1 WHERE d_date>='2000-01-01';

update pt1 set d_date = '2000-02-21' WHERE d_date = '2000-02-20';
SELECT * FROM pt1 WHERE d_date>='2000-02-01';

DELETE FROM pt1 WHERE d_date = '2000-02-21';
SELECT * FROM pt1 WHERE d_date>='2000-02-01';

SELECT * FROM pt1 PARTITION FOR('1999-06-16');

INSERT into pt2 VALUES('2020-08-19','2020-08-19');

INSERT into pt2 VALUES('2000-01-01','2000-01-01');

update pt2 set pt2.d_date = '2020-08-13'
where pt2.d_date = '1999-01-01';

update pt2 set pt2.d_date = '2000-02-01',pt2.d_date_backup = '2000-02-01'
where pt2.d_date = '2000-01-01';

SELECT * FROM pt2;

SELECT * FROM pt2 PARTITION(sys_p2);

delete from pt2 where pt2.d_date<'1999-03-01';
select * from pt2 where pt2.d_date<'1999-06-01';

delete from pt1 where d_date<'1999-05-01';
select * from pt1 where d_date<'1999-06-01';

select * from pt2 where d_date_backup<'1999-06-01';

delete from pt2 where pt2.d_date>'1999-11-15' and pt2.d_date<'1999-12-15';
SELECT * FROM pt2 WHERE pt2.d_date>='1999-10-01';

delete from pt1 where pt1.d_date>'1999-10-15' and pt1.d_date<'1999-11-15';
SELECT * FROM pt1 WHERE pt1.d_date>='1999-10-01';

SELECT * FROM pt2 WHERE pt2.d_date_backup>='1999-10-01';

UPDATE pt2 set d_date = d_date - INTERVAL '4' month where d_date>='1999-05-01' and d_date<'1999-06-01';

UPDATE pt1 set d_date = d_date - INTERVAL '4' month where d_date>='1999-05-01' and d_date<'1999-06-01';
SELECT * FROM pt1 WHERE d_date<'1999-07-01';

SELECT * FROM pt2 WHERE d_date_backup<'1999-07-01';

UPDATE pt2 set d_date = d_date - INTERVAL '1' month where d_date>='1999-06-01' and d_date<='1999-07-15';

UPDATE pt1 set d_date = d_date - INTERVAL '1' month where d_date>='1999-06-01' and d_date<='1999-07-15';
SELECT * FROM pt1 WHERE d_date<'1999-08-01';

SELECT * FROM pt2 WHERE d_date_backup<'1999-08-01';

DROP TABLE IF EXISTS pt2;
DROP TABLE IF EXISTS pt1;

CREATE TABLE pt1(
d_date date UNIQUE
)partition BY RANGE (d_date) INTERVAL('1 month')
(PARTITION part1 VALUES LESS THAN ('1900-01-01'));

CREATE TABLE pt2(
d_date date,
d_date_backup date
)partition BY RANGE (d_date_backup) INTERVAL('1 month')
(PARTITION part1 VALUES LESS THAN ('1900-01-01'));


ALTER TABLE pt2 ADD CONSTRAINT test_foreign FOREIGN KEY(d_date) 
REFERENCES pt1(d_date) on UPDATE RESTRICT ON DELETE RESTRICT;

INSERT INTO pt1 VALUES
('1999-01-01'),('1999-01-02'),('1999-01-019'),('1999-01-20'),
('1999-02-01'),('1999-02-02'),('1999-02-19'),('1999-02-20'),
('1999-03-01'),('1999-03-02'),('1999-03-19'),('1999-03-20'),
('1999-04-01'),('1999-04-02'),('1999-04-19'),('1999-04-20'),
('1999-05-01'),('1999-05-02'),('1999-05-19'),('1999-05-20'),
('1999-06-01'),('1999-06-02'),('1999-06-19'),('1999-06-20'),
('1999-07-01'),('1999-07-02'),('1999-07-19'),('1999-07-20'),
('1999-08-01'),('1999-08-02'),('1999-08-19'),('1999-08-20'),
('1999-09-01'),('1999-09-02'),('1999-09-19'),('1999-09-20'),
('1999-10-01'),('1999-10-02'),('1999-10-19'),('1999-10-20'),
('1999-11-01'),('1999-11-02'),('1999-11-19'),('1999-11-20'),
('1999-12-01'),('1999-12-02'),('1999-12-19'),('1999-12-20');

INSERT INTO pt2 VALUES
('1999-01-01','1999-01-01'),('1999-01-02','1999-01-02'),
('1999-01-19','1999-01-19'),('1999-01-20','1999-01-20'),
('1999-02-01','1999-02-01'),('1999-02-02','1999-02-02'),
('1999-02-19','1999-02-19'),('1999-02-20','1999-02-20'),
('1999-03-01','1999-03-01'),('1999-03-02','1999-03-02'),
('1999-03-19','1999-03-19'),('1999-03-20','1999-03-20'),
('1999-04-01','1999-04-01'),('1999-04-02','1999-04-02'),
('1999-04-19','1999-04-19'),('1999-04-20','1999-04-20'),
('1999-05-01','1999-05-01'),('1999-05-02','1999-05-02'),
('1999-05-19','1999-05-19'),('1999-05-20','1999-05-20'),
('1999-06-01','1999-06-01'),('1999-06-02','1999-06-02'),
('1999-06-19','1999-06-19'),('1999-06-20','1999-06-20'),
('1999-07-01','1999-07-01'),('1999-07-02','1999-07-02'),
('1999-07-19','1999-07-19'),('1999-07-20','1999-07-20'),
('1999-08-01','1999-08-01'),('1999-08-02','1999-08-02'),
('1999-08-19','1999-08-19'),('1999-08-20','1999-08-20'),
('1999-09-01','1999-09-01'),('1999-09-02','1999-09-02'),
('1999-09-19','1999-09-19'),('1999-09-20','1999-09-20'),
('1999-10-01','1999-10-01'),('1999-10-02','1999-10-02'),
('1999-10-19','1999-10-19'),('1999-10-20','1999-10-20'),
('1999-11-01','1999-11-01'),('1999-11-02','1999-11-02'),
('1999-11-19','1999-11-19'),('1999-11-20','1999-11-20'),
('1999-12-01','1999-12-01'),('1999-12-02','1999-12-02'),
('1999-12-19','1999-12-19'),('1999-12-20','1999-12-20'); 

delete from pt2 where pt2.d_date<'1999-03-01';
select * from pt2 where pt2.d_date<'1999-06-01';

delete from pt1 where d_date<'1999-05-01';

delete from pt2 where pt2.d_date>'1999-11-15' and pt2.d_date<'1999-12-15';
SELECT * FROM pt2 WHERE pt2.d_date>='1999-10-01';

delete from pt1 where pt1.d_date>'1999-10-15' and pt1.d_date<'1999-11-15';

delete from pt2 where pt2.d_date>='1999-01-01' and pt2.d_date<'1999-05-01';
delete from pt1 where pt1.d_date>='1999-01-01' and pt1.d_date<'1999-05-01';

UPDATE pt2 set d_date = d_date - INTERVAL '4' month where d_date>='1999-05-01' and d_date<'1999-06-01';

UPDATE pt1 set d_date = d_date - INTERVAL '4' month where d_date>='1999-05-01' and d_date<'1999-06-01';
SELECT * FROM pt1 WHERE d_date<'1999-07-01';

UPDATE pt2 set d_date = d_date - INTERVAL '1' month where d_date>='1999-06-01' and d_date<='1999-07-15';

UPDATE pt1 set d_date = d_date - INTERVAL '5' month where d_date>='1999-06-01' and d_date<='1999-07-15';

delete from pt2 where d_date>='1999-06-01' and d_date<='1999-07-15';
UPDATE pt1 set d_date = d_date - INTERVAL '5' month where d_date>='1999-06-01' and d_date<='1999-07-15';

