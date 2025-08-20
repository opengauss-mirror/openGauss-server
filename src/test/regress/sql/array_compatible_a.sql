set behavior_compat_options='compatible_a_db_array';

CREATE OR REPLACE TYPE num_type IS TABLE OF NUMBER;
CREATE OR REPLACE TYPE int_type IS TABLE OF INTEGER;
CREATE OR REPLACE TYPE char_type IS TABLE OF VARCHAR(20);

DECLARE
  nt num_type := num_type(11, 22, 33, 44, 55, 66);
BEGIN
  nt.DELETE(1);
  raise notice '%',nt(1);
  raise notice '%',nt(2);
  raise notice '%',nt(3);
  raise notice '%',nt(4);
END;
/

DECLARE
  TYPE n1 IS varray(20) OF NUMBER;
  nt n1 := n1(11, 22, 33, 44, 55, 66);
BEGIN
  nt.DELETE(1);
  raise notice '%',nt(1);
  raise notice '%',nt(2);
  raise notice '%',nt(3);
  raise notice '%',nt(4);
END;
/

DECLARE
  TYPE n1 IS varray(20) OF NUMBER index by integer;
  nt n1 := n1(11, 22, 33, 44, 55, 66);
BEGIN
  nt.DELETE(1);
  raise notice '%',nt(1);
  raise notice '%',nt(2);
  raise notice '%',nt(3);
  raise notice '%',nt(4);
END;
/

declare
    type ta is table of varchar(32) index by varchar(32);
    nt ta;
begin
    -- nt := array['red','orange','yellow','green'];
    nt('311') := 'yellow';
    nt(';') := 'red';
    nt('o2') := 'orange';
	raise notice '%',nt('311');
    raise notice '%',nt(';');
    raise notice '%',nt('o2');
    nt.DELETE(';');
    raise notice '%',nt('311');
    raise notice '%',nt(';');
    raise notice '%',nt('o2');
END;
/

DECLARE
  nt num_type := num_type(11, 22, 33, 44, 55, 66);
BEGIN
  nt.DELETE(2,3);
  raise notice '%',nt(1);
  raise notice '%',nt(2);
  raise notice '%',nt(3);
  raise notice '%',nt(4);
END;
/

DECLARE
  nt int_type := int_type(11, 22, 33, 44, 55, 66);
BEGIN
  nt.DELETE(1);
  raise notice '%',nt(1);
  raise notice '%',nt(2);
  raise notice '%',nt(3);
  raise notice '%',nt(4);
END;
/

DECLARE
  nt int_type := int_type(11, 22, 33, 44, 55, 66);
BEGIN
  nt.DELETE(2,3);
  raise notice '%',nt(1);
  raise notice '%',nt(2);
  raise notice '%',nt(3);
  raise notice '%',nt(4);
END;
/

DECLARE
  nt char_type := char_type('one', 'two', 'three', 'four', 'five', 'six');
BEGIN
  nt.DELETE(1);
  raise notice '%',nt(1);
  raise notice '%',nt(2);
  raise notice '%',nt(3);
  raise notice '%',nt(4);
END;
/

DECLARE
  nt char_type := char_type('one', 'two', 'three', 'four', 'five', 'six');
BEGIN
  nt.DELETE(2,3);
  raise notice '%',nt(1);
  raise notice '%',nt(2);
  raise notice '%',nt(3);
  raise notice '%',nt(4);
END;
/

DECLARE
  nt num_type := num_type(11, 22, 33, 44, 55, 66);
BEGIN
  nt.DELETE('1');
  raise notice '%',nt(1);
  raise notice '%',nt(2);
  raise notice '%',nt(3);
  raise notice '%',nt(4);
END;
/

DECLARE
  nt num_type := num_type(11, 22, 33, 44, 55, 66);
BEGIN
  nt.DELETE('2','3');
  raise notice '%',nt(1);
  raise notice '%',nt(2);
  raise notice '%',nt(3);
  raise notice '%',nt(4);
END;
/

DECLARE
  nt int_type := int_type(11, 22, 33, 44, 55, 66);
BEGIN
  nt.DELETE('1');
  raise notice '%',nt(1);
  raise notice '%',nt(2);
  raise notice '%',nt(3);
  raise notice '%',nt(4);
END;
/

DECLARE
  nt int_type := int_type(11, 22, 33, 44, 55, 66);
BEGIN
  nt.DELETE('2','3');
  raise notice '%',nt(1);
  raise notice '%',nt(2);
  raise notice '%',nt(3);
  raise notice '%',nt(4);
END;
/

DECLARE
  nt char_type := char_type('one', 'two', 'three', 'four', 'five', 'six');
BEGIN
  nt.DELETE('1');
  raise notice '%',nt(1);
  raise notice '%',nt(2);
  raise notice '%',nt(3);
  raise notice '%',nt(4);
END;
/

DECLARE
  nt char_type := char_type('one', 'two', 'three', 'four', 'five', 'six');
BEGIN
  nt.DELETE('2','3');
  raise notice '%',nt(1);
  raise notice '%',nt(2);
  raise notice '%',nt(3);
  raise notice '%',nt(4);
END;
/

DECLARE
  nt num_type := num_type(11, 22, 33, 44, 55, 66);
  idx int := 1;
BEGIN
  nt.DELETE(idx);
  raise notice '%',nt(1);
  raise notice '%',nt(2);
  raise notice '%',nt(3);
  raise notice '%',nt(4);
END;
/

DECLARE
  nt num_type := num_type(11, 22, 33, 44, 55, 66);
  start_idx int := 2;
  end_idx int := 3;
BEGIN
  nt.DELETE(start_idx, end_idx);
  raise notice '%',nt(1);
  raise notice '%',nt(2);
  raise notice '%',nt(3);
  raise notice '%',nt(4);
END;
/

DECLARE
  nt num_type := num_type(11, 22, 33, 44, 55, 66);
  start_idx int := 2;
  end_idx int := 3;
BEGIN
  nt.DELETE(start_idx, 3);
  raise notice '%',nt(1);
  raise notice '%',nt(2);
  raise notice '%',nt(3);
  raise notice '%',nt(4);
END;
/

DECLARE
  nt num_type := num_type(11, 22, 33, 44, 55, 66);
  start_idx int := 2;
  end_idx int := 3;
BEGIN
  nt.DELETE(start_idx, '3');
  raise notice '%',nt(1);
  raise notice '%',nt(2);
  raise notice '%',nt(3);
  raise notice '%',nt(4);
END;
/

DECLARE
  nt int_type := int_type(11, 22, 33, 44, 55, 66);
  idx int := 1;
BEGIN
  nt.DELETE(idx);
  raise notice '%',nt(1);
  raise notice '%',nt(2);
  raise notice '%',nt(3);
  raise notice '%',nt(4);
END;
/

DECLARE
  nt int_type := int_type(11, 22, 33, 44, 55, 66);
  start_idx int := 2;
  end_idx int := 3;
BEGIN
  nt.DELETE(start_idx, end_idx);
  raise notice '%',nt(1);
  raise notice '%',nt(2);
  raise notice '%',nt(3);
  raise notice '%',nt(4);
END;
/

DECLARE
  nt int_type := int_type(11, 22, 33, 44, 55, 66);
  start_idx int := 2;
  end_idx int := 3;
BEGIN
  nt.DELETE(start_idx, 3);
  raise notice '%',nt(1);
  raise notice '%',nt(2);
  raise notice '%',nt(3);
  raise notice '%',nt(4);
END;
/

DECLARE
  nt int_type := int_type(11, 22, 33, 44, 55, 66);
  start_idx int := 2;
  end_idx int := 3;
BEGIN
  nt.DELETE(start_idx, '3');
  raise notice '%',nt(1);
  raise notice '%',nt(2);
  raise notice '%',nt(3);
  raise notice '%',nt(4);
END;
/

DECLARE
  nt char_type := char_type('one', 'two', 'three', 'four', 'five', 'six');
  start_idx int := 2;
  end_idx int := 3;
BEGIN
  nt.DELETE(start_idx, '3');
  raise notice '%',nt(1);
  raise notice '%',nt(2);
  raise notice '%',nt(3);
  raise notice '%',nt(4);
END;
/

declare
    type ta is table of varchar(32) index by integer;
    nt ta;
begin
    -- nt := array['red','orange','yellow','green'];
    nt(1) := 'red';
    nt(2) := 'orange';
    nt(3) := 'yellow';
    nt(4) := 'green';
    nt.DELETE(2);
    raise notice '%',nt(1);
    raise notice '%',nt(2);
    raise notice '%',nt(3);
    raise notice '%',nt(4);
END;
/

declare
    type ta is table of varchar(32) index by varchar(32);
    nt ta;
begin
    -- nt := array['red','orange','yellow','green'];
    nt('311') := 'yellow';
    nt('a11') := 'red';
    nt('o2') := 'orange';
    nt('aaa') := 'aadw';
    nt('4aw') := 'green';
    raise notice '%',nt('311');
    raise notice '%',nt('a11');
    raise notice '%',nt('o2');
    raise notice '%',nt('aaa');
    raise notice '%',nt('4aw');
    nt.DELETE('311');
    raise notice '%',nt('311');
    raise notice '%',nt('a11');
    raise notice '%',nt('o2');
    raise notice '%',nt('aaa');
    raise notice '%',nt('4aw');
END;
/

CREATE or REPLACE PROCEDURE proc1 AS
DECLARE
  type n1 is table of NUMBER;
  type nest_type is table of n1;
  nt nest_type;
BEGIN
  nt(1):=n1(1,2);
  nt(2):=n1(33, 44);
  nt(3):=n1(55, 66);
  raise notice '%',nt(1);
  raise notice '%',nt(2);
  raise notice '%',nt(3);
  nt.DELETE(2);
  raise notice '%',nt(1);
  raise notice '%',nt(2);
  raise notice '%',nt(3);
END;
/
call proc1();