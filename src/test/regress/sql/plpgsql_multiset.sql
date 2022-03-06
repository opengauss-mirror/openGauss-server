-- check compatibility --
show sql_compatibility; -- expect A --
drop schema if exists plpgsql_multiset;
create schema plpgsql_multiset;
set current_schema = plpgsql_multiset;

create type m_type as (
	id integer,
	name varchar,
	addr text
);

create type m_type1 as (
	id integer[],
	name varchar,
	addr text
);

-----------------------------------------------------
------------------ multiset union -------------------
-----------------------------------------------------
-- test index by error
declare 
    TYPE SalTabTyp is TABLE OF integer index by integer;
	aa SalTabTyp;
	bb SalTabTyp;
	a integer;
 begin
	aa(0) = 1;
	aa(2) = 2;
	bb(0) = 2;
	bb(1) = NULL;
	aa = aa multiset union bb;
	RAISE INFO '%', aa.count;
end;
/

-- test base type
declare 
    TYPE SalTabTyp is TABLE OF integer;  
	aa SalTabTyp;
	bb SalTabTyp;
 begin
	aa(0) = 1;
	aa(2) = 2;
	bb(0) = 2;
	bb(1) = NULL;
	aa = aa multiset union bb;
	RAISE INFO '%', aa;
end;
/

-- test different type
declare 
    TYPE SalTabTyp is TABLE OF integer;  
	aa SalTabTyp;
    TYPE SalTabTyp1 is TABLE OF varchar(10);  
	bb SalTabTyp1;
 begin
	aa(0) = 1;
	aa(2) = 2;
	bb(0) = 'aa';
	bb(1) = NULL;
	aa = aa multiset union bb;
	RAISE INFO '%', aa;
end;
/

declare 
    TYPE SalTabTyp is TABLE OF varchar(10);  
	aa SalTabTyp;
    TYPE SalTabTyp1 is TABLE OF integer;  
	bb SalTabTyp1;
 begin
	bb(0) = 1;
	bb(2) = 2;
	aa(0) = 'aa';
	aa(1) = NULL;
	aa = aa multiset union bb;
	RAISE INFO '%', aa;
end;
/

declare
TYPE tabint is TABLE OF integer;
bint1 tabint ;
bint2 tabint ;
begin
bint1(2) = 2;
bint1(3) = null;
bint1(4) = 4;
bint2(-1) = null;
bint2(0) = 0;
bint2(1) = null;
bint2(5) = 1;
bint2 = bint1 multiset union bint2;
RAISE INFO '%,%,%,%', bint2,bint2.first,bint2.last,bint2.count;
RAISE INFO '1:%', bint2(1);
RAISE INFO '2:%', bint2(2);
RAISE INFO '3:%', bint2(3);
RAISE INFO '4:%', bint2(4);
RAISE INFO '5:%', bint2(5);
end;
/

declare
TYPE tabint is TABLE OF integer;
bint1 tabint ;
bint2 tabint ;
begin
bint1(-1) = 2;
bint1(0) = 4;
bint2(4) = 3;
bint2(6) = 1;
bint2(5) = 0;
bint2 = bint1 multiset union bint2;
RAISE INFO '%,%,%,%', bint2,bint2.first,bint2.last,bint2.count;
RAISE INFO '1:%', bint2(1);
RAISE INFO '2:%', bint2(2);
RAISE INFO '3:%', bint2(3);
RAISE INFO '4:%', bint2(4);
RAISE INFO '5:%', bint2(5);
end;
/

-- test left null right non-null
declare 
    TYPE SalTabTyp is TABLE OF integer;  
	aa SalTabTyp;  
	bb SalTabTyp;
 begin
	bb(0) = 1;
	bb(2) = 2;
	aa = aa multiset union bb;
	RAISE INFO '%', aa;
end;
/

-- test varchar
declare 
    TYPE SalTabTyp is TABLE OF varchar(10);  
	aa SalTabTyp;  
	bb SalTabTyp;
 begin
    aa(1) = 'abcde';
    aa(2) = 'mgssq';
    bb(1) = 'zxcvb';
    bb(2) = 'abcde';
	aa = aa multiset union distinct bb;
	RAISE INFO '%', aa;
end;
/

-- test int[]  error
declare 
    TYPE SalTabTyp is TABLE OF int[];  
	aa SalTabTyp;  
	bb SalTabTyp;
 begin
	aa = aa multiset union distinct bb;
	RAISE INFO '%', aa;
end;
/

-- distinct base type
declare 
    TYPE SalTabTyp is TABLE OF integer;  
	aa SalTabTyp;
	bb SalTabTyp;
 begin
	aa(0) = 1;
	aa(2) = 2;
	bb(0) = 2;
	bb(1) = NULL;
	aa = aa multiset union distinct bb;
	RAISE INFO '%', aa;
end;
/

-- test array
declare 
    TYPE SalTabTyp is TABLE OF m_type1;  
	aa SalTabTyp;
	bb SalTabTyp;
	cc SalTabTyp;
 begin
	aa(1) = (ARRAY[1,2], 'lisi', 'beijing');
    bb(1) = (ARRAY[1,2], 'lisi', 'beijing');
    bb(2) = (ARRAY[2,1], 'lisi', 'beijing');
	cc = aa multiset union distinct bb;
	RAISE INFO '%', cc;
end;
/

-- test left non-null right null
declare 
    TYPE SalTabTyp is TABLE OF m_type;  
	aa SalTabTyp;
	bb SalTabTyp;
	cc SalTabTyp;
 begin
	aa(1) = (1, 'lisi', 'beijing');
    aa(2) = (2, 'lisi', 'beijing');
    aa(3) = (1, 'lisi', 'beijing');
	cc = aa multiset union distinct bb;
	RAISE INFO '%', cc;
	cc(2) = (3, 'lisi', 'beijing');
	RAISE INFO '%', cc;
end;
/

-- test left null right non-null
declare 
    TYPE SalTabTyp is TABLE OF m_type;  
	aa SalTabTyp;
	bb SalTabTyp;
	cc SalTabTyp;
 begin
	bb(1) = (1, 'lisi', 'beijing');
    bb(2) = (2, 'lisi', 'beijing');
    bb(3) = (1, 'lisi', 'beijing');
	cc = aa multiset union distinct bb;
	RAISE INFO '%', cc;
end;
/

-- test both null
declare 
    TYPE SalTabTyp is TABLE OF m_type;  
	aa SalTabTyp;
	bb SalTabTyp;
 begin
	aa = aa multiset union distinct bb;
	RAISE INFO '%', aa;
	aa(1) = (1, 'lisi', 'beijing');
	RAISE INFO '%', aa;
end;
/

-- test both non-null
declare 
    TYPE SalTabTyp is TABLE OF m_type;  
	aa SalTabTyp;
	bb SalTabTyp;
	a integer;
 begin
	aa(1) = NULL;
	aa(2) = (2, 'lisi', 'beijing');
	aa(3) = (3, 'lisi', 'beijing');
	bb(0) = (3, 'lisi', 'beijing');
	bb(1) = (4, 'lisi', 'beijing');
    bb(2) = NULL;
	aa = aa multiset union distinct bb;
	RAISE INFO '%', aa;
end;
/

-----------------------------------------------------
---------------- multiset intersect -----------------
-----------------------------------------------------
declare 
    TYPE SalTabTyp is TABLE OF integer;  
	aa SalTabTyp;
	bb SalTabTyp;
	a integer;
 begin
	aa(0) = 1;
	aa(2) = 2;
	bb(0) = 2;
	bb(1) = NULL;
	aa = aa multiset intersect bb;
	RAISE INFO '%', aa;
end;
/

-- test left non-null right null
declare 
    TYPE SalTabTyp is TABLE OF m_type;  
	aa SalTabTyp;
	bb SalTabTyp;
	cc SalTabTyp;
 begin
	aa(1) = (1, 'lisi', 'beijing');
	cc = aa multiset intersect bb;
	RAISE INFO '%', cc;
	cc(2) = (2, 'lisi', 'beijing');
	RAISE INFO '%', cc;
end;
/

-- test left null right non-null
declare 
    TYPE SalTabTyp is TABLE OF m_type;  
	aa SalTabTyp;
	bb SalTabTyp;
	cc SalTabTyp;
 begin
	bb(1) = (1, 'lisi', 'beijing');
	cc = aa multiset intersect bb;
	RAISE INFO '%', cc;
	cc(2) = (2, 'lisi', 'beijing');
	RAISE INFO '%', cc;
end;
/

-- test both null
declare 
    TYPE SalTabTyp is TABLE OF m_type;  
	aa SalTabTyp;
	bb SalTabTyp;
 begin
	aa = aa multiset intersect bb;
	RAISE INFO '%', aa;
	aa(1) = (1, 'lisi', 'beijing');
	RAISE INFO '%', aa;
end;
/

-- test both non-null
declare 
    TYPE SalTabTyp is TABLE OF m_type;  
	aa SalTabTyp;
	bb SalTabTyp;
	a integer;
 begin
	aa(1) = NULL;
	aa(2) = (2, 'lisi', 'beijing');
	aa(3) = (3, 'lisi', 'beijing');
	bb(0) = (3, 'lisi', 'beijing');
	bb(1) = (4, 'lisi', 'beijing');
	aa = aa multiset intersect bb;
	RAISE INFO '%', aa;
end;
/

-- test both non-null left 2 same value
declare 
    TYPE SalTabTyp is TABLE OF m_type;  
	aa SalTabTyp;
	bb SalTabTyp;
	a integer;
 begin
	aa(1) = NULL;
	aa(2) = (2, 'lisi', 'beijing');
	aa(3) = (3, 'lisi', 'beijing');
	aa(4) = (3, 'lisi', 'beijing');
    aa(5) = (3, 'lisi', 'beijing');
    aa(6) = (4, 'lisi', 'beijing');

	bb(0) = (3, 'lisi', 'beijing');
    bb(2) = (3, 'lisi', 'beijing');
	bb(1) = (4, 'lisi', 'beijing');
	aa = aa multiset intersect bb;
	RAISE INFO '%', aa;
end;
/

declare 
    TYPE SalTabTyp is TABLE OF m_type;  
	aa SalTabTyp;
	bb SalTabTyp;
	a integer;
 begin
	aa(1) = NULL;
	aa(2) = (2, 'lisi', 'beijing');
	aa(3) = (3, 'lisi', 'beijing');
	aa(4) = (3, 'lisi', 'beijing');
    aa(6) = (4, 'lisi', 'beijing');

	bb(0) = (3, 'lisi', 'beijing');
    bb(2) = (3, 'lisi', 'beijing');
    aa(3) = (3, 'lisi', 'beijing');
	bb(1) = (4, 'lisi', 'beijing');
	aa = aa multiset intersect bb;
	RAISE INFO '%', aa;
end;
/

-- test both non-null right 2 same value
declare 
    TYPE SalTabTyp is TABLE OF m_type;  
	aa SalTabTyp;
	bb SalTabTyp;
	a integer;
 begin
	aa(1) = NULL;
	aa(2) = (2, 'lisi', 'beijing');
	aa(3) = (3, 'lisi', 'beijing');
	bb(0) = (3, 'lisi', 'beijing');
	bb(1) = (3, 'lisi', 'beijing');
	bb(2) = (4, 'lisi', 'beijing');
	aa = aa multiset intersect bb;
	RAISE INFO '%', aa;
end;
/

declare 
    TYPE SalTabTyp is TABLE OF m_type;  
	aa SalTabTyp;
	bb SalTabTyp;
	a integer;
 begin
	aa(1) = NULL;
	aa(2) = (2, 'lisi', 'beijing');
	aa(3) = (3, 'lisi', 'beijing');
	aa(4) = (3, 'lisi', 'beijing');
	aa(5) = NULL;
	bb(0) = (3, 'lisi', 'beijing');
	bb(1) = (3, 'lisi', 'beijing');
	bb(2) = (4, 'lisi', 'beijing');
	bb(3) = NULL;
	bb(4) = NULL;
	aa = aa multiset intersect bb;
	RAISE INFO '%', aa;
end;
/

-- test multiset intersect distinct 
declare 
    TYPE SalTabTyp is TABLE OF m_type;  
	aa SalTabTyp;
	bb SalTabTyp;
	a integer;
 begin
	aa(1) = NULL;
	aa(2) = (2, 'lisi', 'beijing');
	aa(3) = (3, 'lisi', 'beijing');
	aa(4) = (3, 'lisi', 'beijing');
    aa(5) = NULL;
	bb(0) = (3, 'lisi', 'beijing');
	bb(1) = (3, 'lisi', 'beijing');
	bb(2) = (4, 'lisi', 'beijing');
	bb(3) = NULL;
    bb(4) = NULL;
	aa = aa multiset intersect distinct bb;
	RAISE INFO '%', aa;
end;
/
-----------------------------------------------------
---------------- multiset except --------------------
-----------------------------------------------------
declare 
    TYPE SalTabTyp is TABLE OF m_type;  
	aa SalTabTyp;
	bb SalTabTyp;
	a integer;
 begin
	aa(1) = NULL;
	aa(2) = (2, 'lisi', 'beijing');
	aa(3) = (3, 'lisi', 'beijing');
	aa(4) = (3, 'lisi', 'beijing');
	aa(5) = NULL;
	bb(0) = (3, 'lisi', 'beijing');
	bb(1) = (3, 'lisi', 'beijing');
	bb(2) = (4, 'lisi', 'beijing');
	bb(3) = NULL;
	aa = aa multiset except bb;
	RAISE INFO '%', aa;
end;
/

-- test multiset except distinct
declare 
    TYPE SalTabTyp is TABLE OF m_type;  
	aa SalTabTyp;
	bb SalTabTyp;
	a integer;
 begin
	aa(1) = NULL;
	aa(2) = (2, 'lisi', 'beijing');
	aa(3) = (3, 'lisi', 'beijing');
	aa(4) = (3, 'lisi', 'beijing');
	aa(5) = NULL;
	bb(0) = (3, 'lisi', 'beijing');
	bb(2) = (4, 'lisi', 'beijing');
	bb(3) = NULL;
	aa = aa multiset except distinct bb;
	RAISE INFO '%', aa;
end;
/

-- test left non-null right null
declare 
    TYPE SalTabTyp is TABLE OF m_type;  
	aa SalTabTyp;
	bb SalTabTyp;
	cc SalTabTyp;
 begin
	aa(1) = (1, 'lisi', 'beijing');
    aa(2) = (1, 'lisi', 'beijing');
	cc = aa multiset except bb;
	RAISE INFO '%', cc;
	cc(2) = (2, 'lisi', 'beijing');
	RAISE INFO '%', cc;
end;
/

-- test left non-null right null
declare 
    TYPE SalTabTyp is TABLE OF m_type;  
	aa SalTabTyp;
	bb SalTabTyp;
	cc SalTabTyp;
 begin
	aa(1) = (1, 'lisi', 'beijing');
    aa(2) = (1, 'lisi', 'beijing');
	cc = aa multiset except distinct bb;
	RAISE INFO '%', cc;
	cc(2) = (2, 'lisi', 'beijing');
	RAISE INFO '%', cc;
end;
/

-- test left null right non-null
declare 
    TYPE SalTabTyp is TABLE OF m_type;  
	aa SalTabTyp;
	bb SalTabTyp;
	cc SalTabTyp;
 begin
	bb(1) = (1, 'lisi', 'beijing');
	cc = aa multiset except distinct bb;
	RAISE INFO '%', cc;
	cc(2) = (2, 'lisi', 'beijing');
	RAISE INFO '%', cc;
end;
/

-- test left null right non-null
declare 
    TYPE SalTabTyp is TABLE OF m_type;  
	aa SalTabTyp;
	bb SalTabTyp;
	cc SalTabTyp;
 begin
	bb(1) = (1, 'lisi', 'beijing');
	cc = aa multiset except bb;
	RAISE INFO '%', cc;
	cc(2) = (2, 'lisi', 'beijing');
	RAISE INFO '%', cc;
end;
/

-- test both null
declare 
    TYPE SalTabTyp is TABLE OF m_type;  
	aa SalTabTyp;
	bb SalTabTyp;
 begin
	aa = aa multiset except bb;
	RAISE INFO '%', aa;
	aa(1) = (1, 'lisi', 'beijing');
	RAISE INFO '%', aa;
end;
/

drop type m_type;
drop type m_type1;
drop schema if exists plpgsql_multiset cascade;