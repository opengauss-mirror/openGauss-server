--
-- VARCHAR
--

CREATE TABLE NVARCHAR_TBL(f1 nvarchar(1));

INSERT INTO NVARCHAR_TBL (f1) VALUES ('a');

INSERT INTO NVARCHAR_TBL (f1) VALUES ('A');

-- any of the following three input formats are acceptable
INSERT INTO NVARCHAR_TBL (f1) VALUES ('1');

INSERT INTO NVARCHAR_TBL (f1) VALUES (2);

INSERT INTO NVARCHAR_TBL (f1) VALUES ('3');

-- zero-length char
INSERT INTO NVARCHAR_TBL (f1) VALUES ('');

-- try varchar's of greater than 1 length
INSERT INTO NVARCHAR_TBL (f1) VALUES ('cd');
INSERT INTO NVARCHAR_TBL (f1) VALUES ('c     ');


SELECT '' AS seven, * FROM NVARCHAR_TBL;

SELECT '' AS six, c.*
   FROM NVARCHAR_TBL c
   WHERE c.f1 <> 'a';

SELECT '' AS one, c.*
   FROM NVARCHAR_TBL c
   WHERE c.f1 = 'a';

SELECT '' AS five, c.*
   FROM NVARCHAR_TBL c
   WHERE c.f1 < 'a';

SELECT '' AS six, c.*
   FROM NVARCHAR_TBL c
   WHERE c.f1 <= 'a';

SELECT '' AS one, c.*
   FROM NVARCHAR_TBL c
   WHERE c.f1 > 'a';

SELECT '' AS two, c.*
   FROM NVARCHAR_TBL c
   WHERE c.f1 >= 'a';

DROP TABLE NVARCHAR_TBL;

--
-- Now test longer arrays of char
--

CREATE TABLE NVARCHAR_TBL(f1 nvarchar(4));

INSERT INTO NVARCHAR_TBL (f1) VALUES ('a');
INSERT INTO NVARCHAR_TBL (f1) VALUES ('ab');
INSERT INTO NVARCHAR_TBL (f1) VALUES ('abcd');
INSERT INTO NVARCHAR_TBL (f1) VALUES ('abcde');
INSERT INTO NVARCHAR_TBL (f1) VALUES ('abcd    ');

SELECT '' AS four, * FROM NVARCHAR_TBL;
