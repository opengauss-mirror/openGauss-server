SET check_function_bodies = false;

drop foreign table if exists table111;
create foreign table table111 (x integer not null, c1 varchar(1020), c2 varchar(1020), c3 varchar(1020), c4 varchar(1020), c5 varchar(1020), primary key(x));

CREATE OR REPLACE PROCEDURE random_text_simple(length INTEGER, out random_text_simple TEXT)
    AS
    DECLARE
        possible_chars TEXT := '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ';
        output TEXT := '';
        i INT4;
        pos INT4;
    BEGIN

        FOR i IN 1..length LOOP
            pos := random_range(1, length(possible_chars));
            output := output || substr(possible_chars, pos, 1);
        END LOOP;

        random_text_simple := output;
    END;
/

CREATE OR REPLACE FUNCTION random_range(INTEGER, INTEGER)
    RETURNS INTEGER
    LANGUAGE SQL
    AS $$
        SELECT ($1 + FLOOR(($2 - $1 + 1) * random() ))::INTEGER;
$$;
	
CREATE OR REPLACE PROCEDURE build(max integer, OUT outable111 integer, OUT err_msg character varying)
AS
DECLARE
    v_max integer := max;
	c1 character varying;
	c2 character varying;
	c3 character varying;
	c4 character varying;
	c5 character varying;
BEGIN
	FOR i IN 1..v_max LOOP
		select random_text_simple(5) into c1;
		select random_text_simple(5) into c2;
		select random_text_simple(5) into c3;
		select random_text_simple(5) into c4;
		select random_text_simple(5) into c5;
		insert into table111 values (i, c1, c2, c3, c4, c5);
    END LOOP;
	select count(*) into outable111 from table111;
EXCEPTION
    WHEN OTHERS THEN
        err_msg := SQLERRM;

END;
/

CREATE OR REPLACE PROCEDURE delfunc(id numeric) AS
    BEGIN
      DELETE FROM table111
		  WHERE x = id;
	EXCEPTION
		WHEN OTHERS
		THEN
		   return;
    END;
/

CREATE OR REPLACE PROCEDURE updatefunc(id numeric) AS
	DECLARE
		v_c1 character varying;
		v_c2 character varying;
		v_c3 character varying;
		v_c4 character varying;
		v_c5 character varying;
    BEGIN
		select random_text_simple(5) into v_c1;
		select random_text_simple(5) into v_c2;
		select random_text_simple(5) into v_c3;
		select random_text_simple(5) into v_c4;
		select random_text_simple(5) into v_c5;
		UPDATE table111 set c1 = v_c1, c2 = v_c2, c3 = v_c3, c4 = v_c4, c5 = v_c5
			 WHERE x = id;
	EXCEPTION
		WHEN OTHERS
		THEN
		   return;
    END;
/

CREATE OR REPLACE PROCEDURE insertfunc(id numeric) AS
	DECLARE
		t_record record;
    BEGIN
		INSERT INTO table111 VALUES (id);
		select updatefunc(id) into t_record;
		
	EXCEPTION
		WHEN OTHERS
		THEN
		   return;
    END;
/

CREATE OR REPLACE PROCEDURE bench(length INTEGER, OUT outable111 character varying, OUT err_msg character varying)
AS
	DECLARE
        i integer;
		var_case integer;
		t_record record;
		count_all integer;
		distinct_count integer;
		res character varying;
    BEGIN

        FOR i IN 1..length LOOP
           	SELECT floor(random()*(3-1+1))+1 into var_case;
		
			CASE var_case
				WHEN 1 THEN select insertfunc(i+100) into t_record;
				WHEN 2 THEN select delfunc(i) into t_record;
				WHEN 3 THEN select updatefunc(i) into t_record;
			END CASE;
        END LOOP;
		
		select count (*) from table111 into count_all;
		select count (distinct(x)) from table111 into distinct_count;
		
		IF count_all = distinct_count
			THEN res := 'SUCCESS';
			ELSE res := 'FAIL';
		END IF;
		outable111 := res;
EXCEPTION
    WHEN OTHERS THEN
        err_msg := SQLERRM;
END;
/


select build(100);
select bench(100);

drop foreign table table111;












