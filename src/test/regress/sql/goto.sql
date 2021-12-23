create schema goto_schema;
set current_schema=goto_schema;

/* TEST1: There are tests for nornal GOTO statements */

/*
 * 1. GOTO from one to another positon in current block
 */
CREATE or REPLACE FUNCTION GOTO_base()
RETURNS text
AS $$
DECLARE
    v0  int;
    v1 int;
    v2 int;
    test_result text;
BEGIN
    v0  := 0;
    v1  := 0;
    v2  := 0;
    test_result = '';

GOTO pos1;
    v0 := v0 + 1;  --- should miss
    v0 := v0 + 1;  --- should miss
    v1 := v1 + 10;
    
<<pos1>>
    v0 := v0 + 1;
    v0 := v0 + 1;
    v1 := v1 + 1000;

    /* output result */
    test_result := 'GOTO_base=>' ||
                   ' v0:(' || v0 || ')' ||
                   ' v1:(' || v1 || ')' ||
                   ' v2:(' || v2 || ')'; 
    return test_result;
END;
$$
LANGUAGE 'plpgsql';

select GOTO_base();


/*
 * 2. GOTO from IF inner to current block assign
 */
CREATE or REPLACE FUNCTION GOTO_if_assign()
RETURNS text
AS $$
DECLARE
    v0  int;
    v1 int;
    v2 int;
    test_result text;
BEGIN
    v0  := 0;
    v1  := 0;
    v2  := 0;
    test_result = '';

    IF v0 = 0 THEN
        GOTO pos1;
    END IF;

    v0 := v0 + 1;  --- should miss
    v0 := v0 + 1;  --- should miss
    v1 := v1 + 10;

<<pos1>>
    v0 := v0 + 1;
    v0 := v0 + 1;
    v1 := v1 + 1000;

    /* output result */
    test_result := 'GOTO_base=>' ||
                   ' v0:(' || v0 || ')' ||
                   ' v1:(' || v1 || ')' ||
                   ' v2:(' || v2 || ')';
    return test_result;
END;
$$
LANGUAGE 'plpgsql';

select GOTO_if_assign();

/*
 * 3. GOTO from loop(if goto) inner to current block assign
 */
CREATE or REPLACE FUNCTION GOTO_loop_if_goto_assign()
RETURNS text
AS $$
DECLARE
    v0 int;
    v1 int;
    v2 int;
    test_result text;
BEGIN
    v0  := 0;
    v1  := 0;
    v2  := 0;
    test_result = '';

	LOOP
	EXIT WHEN v1 > 100;
		v0 := v0 + 1;
		v1 := v1 + 2;
		if v1 > 25 THEN
			GOTO position1;
		END IF;
	END LOOP;

<<position1>>
    /* output result */
    test_result := 'GOTO_base=>' ||
                   ' v0:(' || v0 || ')' ||
                   ' v1:(' || v1 || ')' ||
                   ' v2:(' || v2 || ')';
    return test_result;
END;
$$
LANGUAGE 'plpgsql';

select GOTO_loop_if_goto_assign();

/*
 * 4. GOTO from loop(if goto) inner to current block return;
 */
CREATE or REPLACE FUNCTION GOTO_loop_if_goto_return()
RETURNS text
AS $$
DECLARE
    v0 int;
    v1 int;
    v2 int;
    test_result text;
BEGIN
    v0  := 0;
    v1  := 0;
    v2  := 0;
    test_result = '';

	LOOP
	EXIT WHEN v1 > 100;
		v0 := v0 + 1;
		v1 := v1 + 2;
		if v1 > 28 THEN
		    test_result := 'GOTO_base=>' ||
                   ' v0:(' || v0 || ')' ||
                   ' v1:(' || v1 || ')' ||
                   ' v2:(' || v2 || ')';
			GOTO position1;
		END IF;
	END LOOP;

    /* output result */
    test_result := 'GOTO_base=>' ||
                   ' v0:(' || v0 || ')' ||
                   ' v1:(' || v1 || ')' ||
                   ' v2:(' || v2 || ')';
<<position1>>
    return test_result;
END;
$$
LANGUAGE 'plpgsql';

select GOTO_loop_if_goto_return();

/*
 * 5. GOTO from while-if to current block assign
 */
CREATE or REPLACE FUNCTION GOTO_while_goto()
RETURNS text
AS $$
DECLARE
    v0  int;
    v1 int;
    v2 int;
    test_result text;
BEGIN
    v0  := 1;
    v1  := 10;
    v2  := 100;
    test_result = '';
    WHILE v1 < 100 LOOP
        v1 := v1+1;
        v2 := v2+1;
        IF v1 > 25 THEN
            GOTO pos1;
        END IF;
    END LOOP;

<<pos1>>
    /* output result */
    test_result := 'GOTO_base=>' ||
                   ' v0:(' || v0 || ')' ||
                   ' v1:(' || v1 || ')' ||
                   ' v2:(' || v2 || ')';
    return test_result;
END;
$$
LANGUAGE 'plpgsql';

select GOTO_while_goto();

/*
 * 6. GOTO from case-when to current block assign
 */
CREATE or REPLACE FUNCTION GOTO_while_goto(
    IN p1 int
)
RETURNS text
AS $$
DECLARE
    v0  int;
    v1 int;
    v2 int;
    test_result text;
BEGIN
    test_result = '';
    CASE
        WHEN p1 = 1 THEN
            v1 := 10;
            GOTO pos1;
        WHEN p1 = 2 THEN
            v1 := 100;
            GOTO pos1;
        WHEN p1 = 3 THEN
            v1 := 1000;
            GOTO pos1;
        ELSE
            v1 = -1;
            GOTO pos1;
    END CASE;

    v1:= -99;
<<pos1>>
    /* output result */
    test_result := 'GOTO_base=>' ||
                   ' v0:(' || v0 || ')' ||
                   ' v1:(' || v1 || ')' ||
                   ' v2:(' || v2 || ')';
    return test_result;
END;
$$
LANGUAGE 'plpgsql';

select GOTO_while_goto(0);
select GOTO_while_goto(1);
select GOTO_while_goto(2);
select GOTO_while_goto(3);
select GOTO_while_goto(4);

/*
 * 7. GOTO from case-when to current block sql
 */
create table t1(c1 int, c2 int);
insert into t1 select v,v from generate_series(1,10) as v;

CREATE or REPLACE FUNCTION GOTO_case_togo_sql(
    IN optype text
)
RETURNS text
AS $$
DECLARE
    v0  int;
    v1 int;
    v2 int;
    test_result text;
BEGIN
    v1 := 0;
    test_result = '';
    CASE
        WHEN optype = 'insert' THEN
            GOTO pos_insert;
        WHEN optype = 'delete' THEN
            GOTO pos_delete;
        WHEN optype = 'update' THEN
            GOTO pos_update;
        WHEN optype = 'select' THEN
            GOTO pos_select;
        ELSE
            v1 = -1;
            GOTO pos1;
    END CASE;

    v1:= -99;

<<pos_insert>>
    insert into t1 values(99,99);
    GOTO pos1;

<<pos_delete>>
    delete from t1 where c2 = 8;
    GOTO pos1;

<<pos_update>>
    update t1 set c2 = c2 * 10 where c1 = 7;
    GOTO pos1;

<<pos_select>>
    select max(c1) from t1 into v1;
    GOTO pos1;

<<pos1>>
    return test_result || v1;
END;
$$
LANGUAGE 'plpgsql';

select GOTO_case_togo_sql('insert');
select GOTO_case_togo_sql('update');
select GOTO_case_togo_sql('delete');
select GOTO_case_togo_sql('select');

select * from t1 order by 1,2;

/*
 * 8. GOTO from foreach to current block
 */
CREATE or REPLACE FUNCTION foreach_test(anyarray)
RETURNS void as $$
DECLARE x int;
BEGIN
  foreach x in array $1
  loop
    raise notice '%', x;
	goto abc;
  end loop;
  <<abc>>
  raise notice 'end all';
END;
$$ language plpgsql;

select foreach_test(ARRAY[1,2,3,4]);


/*
 * 9. GOTO from fori/fors/forc block to current block
 */
create table conttesttbl(idx serial, v integer);
insert into conttesttbl(v) values(10);
insert into conttesttbl(v) values(20);
insert into conttesttbl(v) values(30);
insert into conttesttbl(v) values(40);

CREATE or REPLACE FUNCTION for_test()
RETURNS void
as $$
DECLARE
	_i integer = 0;
	_r record;
BEGIN
	<<pos1>>
	for _i in 1..10 loop
		BEGIN
			continue when _i < 5;
			raise notice '%', _i;
			goto pos2;
		END;
	end loop;
	raise notice 'end ---1---';

	<<pos2>>
	raise notice '---2---';
	for _r in select * from conttesttbl order by v loop
		continue when _r.v <= 20;
		raise notice '%', _r.v;
		goto pos3;
	end loop;
	raise notice 'end ---2---';

	<<pos3>>
	raise notice '---3---';
	for _r in execute 'select * from conttesttbl order by v' loop
		continue when _r.v <= 20;
		raise notice '%', _r.v;
		goto posend;
	end loop;
	raise notice 'end ---3---';

	<<posend>>
	raise notice 'end all';
END;
$$
LANGUAGE 'plpgsql';

select for_test();


/*
 * 10. GOTO from inner block to current block
 */
CREATE or REPLACE FUNCTION GOTO_outer()
RETURNS text
AS $$
DECLARE
    v0  int;
    v1 int;
    v2 int;
    test_result text;
BEGIN
    v0  := 0;
    v1  := 0;
    v2  := 0;
    test_result := '';

	BEGIN
		goto outer;
		v0 := v0 + 1;
		v0 := v0 + 1;
		v1 := v1 + 1000;
	END;

	<<outer>>
    v0 := v0 + 2;
    v0 := v0 + 2;
    v1 := v1 + 2000;

    /* output result */
    test_result := 'GOTO_base=>' ||
                   ' v0:(' || v0 || ')' ||
                   ' v1:(' || v1 || ')' ||
                   ' v2:(' || v2 || ')';
    return test_result;
END;
$$
LANGUAGE 'plpgsql';

select GOTO_outer();

/*
 * 11. GOTO from current block to current begin
 */
CREATE or REPLACE FUNCTION GOTO_begin()
RETURNS text
AS $$
DECLARE
    v0  int;
    v1 int;
    v2 int;
	test_result text;
BEGIN
    v0  := 0;
    v1  := 0;
    v2  := 0;
	GOTO position1;
	v2 := v2 + 1;
	<<position1>>
	begin
		v0 := v0 + 1;
		v1 := v1 + 2;
		v1 := v1 + 2000000000;
	end;
	/* output result */
    test_result := 'GOTO_base=>' ||
                   ' v0:(' || v0 || ')' ||
                   ' v1:(' || v1 || ')' ||
                   ' v2:(' || v2 || ')';
    return test_result;
END;
$$
LANGUAGE 'plpgsql';

select GOTO_begin();

/*
 * 11. GOTO in DYNEXECUTE from call procedure
 */
CREATE OR REPLACE PROCEDURE sp_test_1
(
    param1    in   INTEGER,
    param2    out  INTEGER,
    param3    in   INTEGER
)
AS
BEGIN
   goto result;
   param2:= 10;
   <<result>>
   param2:= param1 + param3;
END;
/

DECLARE
    input1 INTEGER:=1;
    input2 INTEGER:=2;
    l_param2     INTEGER;
BEGIN
	goto exe;
	input2 := 3;
	<<exe>>
    EXECUTE IMMEDIATE 'begin sp_test_1(:col_1, :col_2, :col_3);end;'
        USING IN input1, OUT l_param2, IN input2;
	<<output>>
END;
/


DROP FUNCTION GOTO_base();
DROP FUNCTION GOTO_if_assign();
DROP FUNCTION GOTO_loop_if_goto_assign();
DROP FUNCTION GOTO_loop_if_goto_return();
DROP FUNCTION GOTO_while_goto();
DROP FUNCTION GOTO_while_goto();
DROP FUNCTION GOTO_case_togo_sql();
DROP FUNCTION foreach_test();
DROP FUNCTION for_test();
DROP FUNCTION GOTO_outer();
DROP FUNCTION GOTO_begin();
DROP PROCEDURE sp_test_1;


drop table t1;



/* TEST2: There are tests for unexpected GOTO statements */

/*
 * 1. cannot GOTO from IF-ELSE to another IF-ELSE
 */
CREATE or REPLACE FUNCTION GOTO_if_assign(IN p1 int)
RETURNS text
AS $$
DECLARE
    v0 int;
    v1 int;
    v2 int;
    test_result text;
BEGIN
    v0  := 0;
    v1  := 0;
    v2  := 0;
    test_result = '';

    IF p1 = 1 THEN
		<<pos1>>
		v0 := v0 + 1;
	ELSIF p1 = 2 THEN
		GOTO pos1;
	ELSE
        GOTO pos2;
    END IF;


	IF v0 = 1 THEN
		v0 := v0 + 1;
	ELSE
        <<pos2>>
		v0 := v0 + 2;
		v0 := v0 + 2;
		v1 := v1 + 2000;
    END IF;

    test_result := 'GOTO_base=>' ||
                   ' v0:(' || v0 || ')' ||
                   ' v1:(' || v1 || ')' ||
                   ' v2:(' || v2 || ')';
    return test_result;
END;
$$
LANGUAGE 'plpgsql';

select GOTO_if_assign(1);
select GOTO_if_assign(2);
select GOTO_if_assign(3);


/*
 * 2. cannot GOTO from CASE-WHEN to another CASE-WHEN OR IF-ELSE
 */
CREATE or REPLACE FUNCTION GOTO_while_goto(
    IN p1 int
)
RETURNS text
AS $$
DECLARE
    v0 int;
    v1 int;
    v2 int;
    test_result text;
BEGIN
    test_result = '';
    CASE
        WHEN p1 = 1 THEN
            v1 := 10;
            GOTO othercase;
        WHEN p1 = 2 THEN
			<<othercase>>
            v1 := 100;
            GOTO otherinnerblock;
        WHEN p1 = 3 THEN
            v1 := 1000;
            GOTO pos1;
        ELSE
            v1 := -1;
            GOTO pos1;
    END CASE;

	IF v0 = 1 THEN
		<<otherinnerblock>>
		v0 := v0 + 1;
	ELSE
        GOTO pos1;
    END IF;

    v1:= -99;
<<pos1>>
    /* output result */
    test_result := 'GOTO_base=>' ||
                   ' v0:(' || v0 || ')' ||
                   ' v1:(' || v1 || ')' ||
                   ' v2:(' || v2 || ')';
    return test_result;
END;
$$
LANGUAGE 'plpgsql';

select GOTO_while_goto(1);
select GOTO_while_goto(2);

/*
 * 3. cannot GOTO from inner foreach to current block
 */
CREATE or REPLACE FUNCTION foreach_test(anyarray)
RETURNS void as $$
DECLARE x int;
BEGIN
  goto inner;
  foreach x in array $1
  loop
	<<inner>>
    raise notice '%', x;
	goto abc;
  end loop;
  <<abc>>
  raise notice 'end all';
END;
$$ language plpgsql;

select foreach_test(ARRAY[1,2,3,4]);

/*
 * 4. cannot GOTO from inner for to current block
 */
CREATE or REPLACE FUNCTION for_test()
RETURNS void
as $$
DECLARE
	_i integer = 0;
	_r record;
BEGIN
	goto pos1;
	for _i in 1..10 loop
		<<pos1>>
		BEGIN
			continue when _i < 5;
			raise notice '%', _i;
		END;
	end loop;
END;
$$
LANGUAGE 'plpgsql';

select for_test();

CREATE or REPLACE FUNCTION for_test()
RETURNS void
as $$
DECLARE
	_i integer = 0;
	_r record;
BEGIN
	goto pos2;
	raise notice '---2---';
	for _r in select * from conttesttbl order by v loop
		<<pos2>>
		continue when _r.v <= 20;
		raise notice '%', _r.v;
	end loop;
END;
$$
LANGUAGE 'plpgsql';

select for_test();

CREATE or REPLACE FUNCTION for_test()
RETURNS void
as $$
DECLARE
	_i integer = 0;
	_r record;
BEGIN
	goto pos3;
	raise notice '---3---';
	for _r in execute 'select * from conttesttbl order by v' loop
		<<pos3>>
		continue when _r.v <= 20;
		raise notice '%', _r.v;
	end loop;
END;
$$
LANGUAGE 'plpgsql';

select for_test();

/*
 * 5. cannot GOTO from outer block to inner block
 */
CREATE or REPLACE FUNCTION GOTO_innerbegin()
RETURNS text
AS $$
DECLARE
    v0  int;
    v1 int;
    v2 int;
    test_result text;
BEGIN
    v0  := 0;
    v1  := 0;
    v2  := 0;
    test_result = '';

	BEGIN
		<<innerbegin>>
		v0 := v0 + 1;
		v0 := v0 + 1;
		v1 := v1 + 1000;
	END;

	goto innerbegin;
    v0 := v0 + 2;
    v0 := v0 + 2;
    v1 := v1 + 2000;

    /* output result */
    test_result := 'GOTO_base=>' ||
                   ' v0:(' || v0 || ')' ||
                   ' v1:(' || v1 || ')' ||
                   ' v2:(' || v2 || ')';
    return test_result;
END;
$$
LANGUAGE 'plpgsql';

select GOTO_innerbegin();

DROP FUNCTION GOTO_if_assign();
DROP FUNCTION GOTO_while_goto();
DROP FUNCTION foreach_test();
DROP FUNCTION for_test();
DROP FUNCTION GOTO_innerbegin();


/* TEST3: There are test for EXCEPTION */
/*
 * 1. cannot GOTO from unexception to exception inner
 * 2. cannot GOTO from exception inner to current block
 * 3. GOTO from exception to enclosing (father or ancestor) block
 * 4. cannot GOTO exception declaration *
 */

/* 1. cannot GOTO from unexception to exception inner */
CREATE or REPLACE FUNCTION GOTO_exception_from_normal()
returns text
AS $$
DECLARE
    v0  int;
    v1 int;
    v2 int;
    test_result text;
BEGIN
    v0  := 0;
    v1  := 1;
    v2  := 2;
    test_result = '';
    
    goto pos1;
    v0 := v0 + 1;
    v1 := v1 + 1;
    v2 := v2 + 1000;
    test_result := 'GOTO_base=>' ||
                   ' v0:(' || v0 || ')' ||
                   ' v1:(' || v1 || ')' ||
                   ' v2:(' || v2 || ')';
    return test_result;
    EXCEPTION
	WHEN DATA_EXCEPTION THEN
		<<pos1>>
	test_result := 'GOTO_base_exception=>' ||
                   ' v0:(' || v0 || ')' ||
                   ' v1:(' || v1 || ')' ||
                   ' v2:(' || v2 || ')';
END;
$$
LANGUAGE 'plpgsql';

select GOTO_exception_from_normal();

/* 2. cannot GOTO from exception inner to current block */
CREATE or REPLACE FUNCTION GOTO_exception_to_current(IN p1 int)
returns text
AS $$
DECLARE
    v0  int;
    v1 int;
    v2 int;
    test_result text;
BEGIN
    v0  := 0;
    v1  := 1;
    v2  := v1 / p1;
    test_result = '';

<<pos1>>
   v0 := v0 + 1;
   v1 := v1 + 1;
   v2 := v2 + 1000;
	
   test_result := 'GOTO_base=>' ||
                   ' v0:(' || v0 || ')' ||
                   ' v1:(' || v1 || ')' ||
                   ' v2:(' || v2 || ')';
   return test_result;
   EXCEPTION
	WHEN DATA_EXCEPTION THEN
		goto pos1;		
    test_result := 'GOTO_base_exception=>' ||
                   ' v0:(' || v0 || ')' ||
                   ' v1:(' || v1 || ')' ||
                   ' v2:(' || v2 || ')';
END;
$$
LANGUAGE 'plpgsql';

select GOTO_exception_to_current(1);
select GOTO_exception_to_current(0);

/* 3. GOTO from exception to enclosing (father or ancestor) block */
CREATE or REPLACE FUNCTION GOTO_exception_to_upper()
RETURNS text 
AS $$
DECLARE
    v0  int;
    v1 int;
    v2 int;
    test_result text;
BEGIN
    v0  := 0;
    v1  := 1;
    test_result = '';

    BEGIN
	v2  := v1/v0;
	v0 := 100;
	EXCEPTION
		WHEN DATA_EXCEPTION THEN
			goto pos1;
    END;
    v0 := 100;
    <<pos1>>
    v0 := v0 + 1;
    v1 := v1 + 1000;
    v2 := v2 + 2000;

    test_result := 'GOTO_base=>' ||
                   ' v0:(' || v0 || ')' ||
                   ' v1:(' || v1 || ')' ||
                   ' v2:(' || v2 || ')';
    return test_result;
END;
$$
LANGUAGE 'plpgsql';

select GOTO_exception_to_upper();

/* 4. cannot GOTO exception declaration */
CREATE or REPLACE FUNCTION GOTO_exception_from_normal()
returns text
AS $$
DECLARE
    v0  int;
    v1 int;
    v2 int;
    test_result text;
BEGIN
    v0  := 0;
    v1  := 1;
    v2  := 2;
    test_result = '';
    goto pos1;
    v0 := v0 + 1;
    v1 := v1 + 1;
    v2 := v2 + 1000;
    test_result := 'GOTO_base=>' ||
                   ' v0:(' || v0 || ')' ||
                   ' v1:(' || v1 || ')' ||
                   ' v2:(' || v2 || ')';
    return test_result;
    <<pos1>>
    EXCEPTION
        WHEN DATA_EXCEPTION THEN
              <<pos1>>
END;
$$
LANGUAGE 'plpgsql';

drop function GOTO_exception_from_normal();
drop function GOTO_exception_to_current();
drop function GOTO_exception_to_upper();


/* TEST4: There are tests for NULL statements */

/* 1. cannot GOTO the end of if, loop, block，such as end loop; end; end if; */
CREATE or REPLACE FUNCTION GOTO_nothing()
RETURNS text
AS $$
DECLARE
   v0  int;
   done   BOOLEAN;
BEGIN
   v0 := 0;
   done := true;
   FOR i IN 1 .. 5 LOOP
      v0 := v0 + 1;
      IF done THEN
      	GOTO end_loop;
	 v0 := v0 + 100;
      END IF;
      <<end_loop>> 
   END LOOP;  
   return v0;
END;
$$
LANGUAGE 'plpgsql';


/* 2. GOTO NULL before the end of if, loop, block */
CREATE or REPLACE FUNCTION GOTO_exception_begin()
RETURNS void 
AS $$
DECLARE
    v0  int;
    v1 int;
    v2 int;
BEGIN
    v0  := 0;
    v1  := 1;
    goto pos1;
    v2  := v1/v0;
    <<pos1>>
    NULL;
    EXCEPTION
	WHEN DATA_EXCEPTION THEN
END;
$$
LANGUAGE 'plpgsql';

select GOTO_exception_begin();

CREATE or REPLACE FUNCTION GOTO_null()
RETURNS text
AS $$
DECLARE
   v0  int;
   done   BOOLEAN;
BEGIN
   v0 := 0;
   done := true;
   FOR i IN 1 .. 5 LOOP
      v0 := v0 + 1;
      IF done THEN
          GOTO end_loop;
	  v0 := v0 + 100;
      END IF;
   <<end_loop>> 
   NULL;  
   END LOOP;  
   return v0;
END;
$$
LANGUAGE 'plpgsql';

select GOTO_null();

drop function GOTO_exception_begin();
drop function GOTO_null();


/* TEST5: There are tests for multi-labels */

/* 1. do not support multiple GOTO labels in one block */
CREATE or REPLACE FUNCTION GOTO_labels1()
RETURNS text
AS $$
DECLARE
    v0  int;
    v1 int;
    v2 int;
    test_result text;
BEGIN
    v0  := 0;
    v1  := 0;
    v2  := 0;
    test_result = '';

GOTO pos1;
    v0 := v0 + 1;
    v1 := v1 + 1;
    v2 := v2 + 10;

<<pos1>>
    v0 := v0 + 1;
    v1 := v1 + 1;
    v2 := v2 + 1000;

<<pos1>>
    v0 := v0 + 2;
    v1 := v1 + 2;
    v2 := v2 + 2000;

    /* output result */
    test_result := 'GOTO_base=>' ||
                   ' v0:(' || v0 || ')' ||
                   ' v1:(' || v1 || ')' ||
                   ' v2:(' || v2 || ')';
    return test_result;
END;
$$
LANGUAGE 'plpgsql';

select GOTO_labels1();

/* 2. do not support multiple GOTO labels in multiple blocks */
CREATE or REPLACE FUNCTION GOTO_labels2()
RETURNS text
AS $$
DECLARE
    v0  int;
    v1 int;
    v2 int;
    test_result text;
BEGIN
    v0  := 0;
    v1  := 0;
    v2  := 0;
    test_result = '';
	
	BEGIN
		GOTO pos1;
		v0 := 100;
		<<pos1>>
		v0 := v0 + 2;
		v1 := v1 + 2;
		v2 := v2 + 2000;
	END;

<<pos1>>
    v0 := v0 + 1;
    v1 := v1 + 1;
    v2 := v2 + 1000;

    /* output result */
    test_result := 'GOTO_base=>' ||
                   ' v0:(' || v0 || ')' ||
                   ' v1:(' || v1 || ')' ||
                   ' v2:(' || v2 || ')';
    return test_result;
END;
$$
LANGUAGE 'plpgsql';

select GOTO_labels2();

/* 3. support multiple UN-GOTO labels in blocks */
CREATE or REPLACE FUNCTION muti_labels_for_no_goto()
RETURNS text
AS $$
DECLARE
    v0 int;
    v1 int;
    v2 int;
    test_result text;
BEGIN
    v0  := 0;
    v1  := 1;
    v2  := 0;
    test_result = '';
<<position1>>
        LOOP
        EXIT WHEN v1 > 10;
                v0 := v0 + 1;
                v1 := v1 + 2;
        END LOOP;
<<position1>>
        LOOP
        EXIT WHEN v1 > 25;
                v0 := v0 + 1;
                v1 := v1 + 2;
        END LOOP;
    test_result := 'GOTO_base=>' ||
                   ' v0:(' || v0 || ')' ||
                   ' v1:(' || v1 || ')' ||
                   ' v2:(' || v2 || ')';
    return test_result;
END;
$$
LANGUAGE 'plpgsql';

select muti_labels_for_no_goto();

CREATE or REPLACE FUNCTION muti_labels_with_goto()
RETURNS text
AS $$
DECLARE
    v0 int;
    v1 int;
    v2 int;
    test_result text;
BEGIN
    v0  := 0;
    v1  := 1;
    v2  := 0;
    test_result = '';
	goto abc;
	v0 := v0 + 100;
    v1 := v1 + 200;
<<position1>>
        LOOP
        EXIT WHEN v1 > 10;
                v0 := v0 + 1;
                v1 := v1 + 2;
        END LOOP;
<<position1>>
        LOOP
        EXIT WHEN v1 > 25;
                v0 := v0 + 1;
                v1 := v1 + 2;
        END LOOP;
<<abc>>
	v0 := v0 + 10;
    v1 := v1 + 20;
    test_result := 'GOTO_base=>' ||
                   ' v0:(' || v0 || ')' ||
                   ' v1:(' || v1 || ')' ||
                   ' v2:(' || v2 || ')';
    return test_result;
END;
$$
LANGUAGE 'plpgsql';

select muti_labels_with_goto();

drop function GOTO_labels1();
drop function GOTO_labels2();
drop function muti_labels_for_no_goto();
drop function muti_labels_with_goto();

CREATE OR REPLACE PROCEDURE sp_test_1
(
    param1    in   INTEGER,
    param2    out  INTEGER,
    param3    in   INTEGER
)
AS
BEGIN
   goto result;
   param2:= 10;
   <<result>>
   param2:= param1 + param3;
END;
/


DECLARE
    input1 INTEGER:=1;
    input2 INTEGER:=2;
    l_param2     INTEGER;
BEGIN
	goto exe;
	input2 := 3;
	<<exe>>
    EXECUTE IMMEDIATE 'begin sp_test_1(:col_1, :col_2, :col_3);end;'
        USING IN input1, OUT l_param2, IN input2;
	<<output>>
END;
/

DECLARE
    v0  int;
    v1 int;
    v2 int;
BEGIN
    v0  := 0;
    v1  := 0;
    v2  := 0;
GOTO pos1;
    v0 := v0 + 1;  --- should miss
    v0 := v0 + 1;  --- should miss
    v2 := v2 + 10;<<pos1>>v0 := v0 + 1;
    v0 := v0 + 1;
    v1 := v1 + 1000;
END;
/

drop table if exists tb_test;
create table tb_test(group_code varchar2(5), custsort varchar2(4));
create procedure proc_test() as
v_tb_log timestamp;
v_tb_log1 timestamp;
begin
    for cm_np_duebill in (select group_code, custsort from tb_test) loop
        <<next1>>
        NULL;
    end loop;
for cm_np_duebill in (select sysdate) loop
    v_tb_log1:=cm_np_duebill.sysdate;
end loop;
end;
/

call proc_test();

drop schema goto_schema cascade;
