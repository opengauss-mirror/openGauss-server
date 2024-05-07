DROP TABLE if exists zytest1;
DROP TABLE if exists zytest2;
DROP TABLE if exists zytest3;

CREATE TABLE zytest1
(aid NUMERIC,
name varchar(20)
);

CREATE TABLE zytest2
(bid NUMERIC,
des varchar(20),
zid NUMERIC --zytest1.aid
);

CREATE TABLE zytest3
(aid NUMERIC, ----zytest1.aid
numb NUMERIC --
);

INSERT INTO zytest1 values(1,'NAME1');
INSERT INTO zytest1 values(2,'NAME2');
INSERT INTO zytest1 values(3,'NAME3');
INSERT INTO zytest1 values(4,'NAME4');
INSERT INTO zytest1 values(5,'NAME5');
INSERT INTO zytest1 values(6,'NAME6');

INSERT INTO zytest2 values(1,'des1',1);
INSERT INTO zytest2 values(2,'des2',2);
INSERT INTO zytest2 values(3,'des3',3);
INSERT INTO zytest2 values(4,'des4',4);
INSERT INTO zytest2 values(5,'des5',5);
INSERT INTO zytest2 values(6,'des6',6);

INSERT INTO zytest3 values(1,1);
INSERT INTO zytest3 values(2,2);
INSERT INTO zytest3 values(3,3);
INSERT INTO zytest3 values(1,4);
INSERT INTO zytest3 values(2,5);
INSERT INTO zytest3 values(3,6);

SELECT *
FROM (
SELECT bid,COALESCE((SELECT SUM(numb) FROM zytest3 WHERE aid = zid),0) nsum
FROM zytest2
ORDER BY bid
)
WHERE nsum IS NOT NULL OR nsum>5;

explain (analyze, costs off, timing off) SELECT *
FROM (
SELECT bid,COALESCE((SELECT SUM(numb) FROM zytest3 WHERE aid = zid),0) nsum
FROM zytest2
ORDER BY bid
)
WHERE nsum IS NOT NULL OR nsum>5;

SELECT *
FROM (
SELECT bid,COALESCE((SELECT SUM(numb) FROM zytest3 WHERE aid = zid),0) nsum
FROM zytest2
)
WHERE nsum IS NOT NULL OR nsum>5 ORDER BY bid;

explain (analyze, costs off, timing off) SELECT *
FROM (
SELECT bid,COALESCE((SELECT SUM(numb) FROM zytest3 WHERE aid = zid),0) nsum
FROM zytest2
)
WHERE nsum IS NOT NULL OR nsum>5 ORDER BY bid;
