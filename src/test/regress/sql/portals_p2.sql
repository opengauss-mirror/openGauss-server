--
-- PORTALS_P2
--

START TRANSACTION;

CURSOR foo13 FOR
   SELECT * FROM onek WHERE unique1 = 50;

CURSOR foo14 FOR
   SELECT * FROM onek WHERE unique1 = 51;

CURSOR foo15 FOR
   SELECT * FROM onek WHERE unique1 = 52;

CURSOR foo16 FOR
   SELECT * FROM onek WHERE unique1 = 53;

CURSOR foo17 FOR
   SELECT * FROM onek WHERE unique1 = 54;

CURSOR foo18 FOR
   SELECT * FROM onek WHERE unique1 = 55;

CURSOR foo19 FOR
   SELECT * FROM onek WHERE unique1 = 56;

CURSOR foo20 FOR
   SELECT * FROM onek WHERE unique1 = 57;

CURSOR foo21 FOR
   SELECT * FROM onek WHERE unique1 = 58;

CURSOR foo22 FOR
   SELECT * FROM onek WHERE unique1 = 59;

CURSOR foo23 FOR
   SELECT * FROM onek WHERE unique1 = 60;

CURSOR foo24 FOR
   SELECT * FROM onek2 WHERE unique1 = 50;

CURSOR foo25 FOR
   SELECT * FROM onek2 WHERE unique1 = 60;

FETCH all in foo13;

FETCH all in foo14;

FETCH all in foo15;

FETCH all in foo16;

FETCH all in foo17;

FETCH all in foo18;

FETCH all in foo19;

FETCH all in foo20;

FETCH all in foo21;

FETCH all in foo22;

FETCH all in foo23;

FETCH all in foo24;

FETCH all in foo25;

CLOSE foo13;

CLOSE foo14;

CLOSE foo15;

CLOSE foo16;

CLOSE foo17;

CLOSE foo18;

CLOSE foo19;

CLOSE foo20;

CLOSE foo21;

CLOSE foo22;

CLOSE foo23;

CLOSE foo24;

CLOSE foo25;

END;
