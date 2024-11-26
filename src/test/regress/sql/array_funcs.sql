select array_remove(array[], 2);
select array_remove(array[1,2,2,3], 2);
select array_remove(array[1,2,2,3], 5);
select array_remove(array[1,NULL,NULL,3], NULL);
select array_remove(array['A','CC','D','C','RR'], 'RR');
select array_remove('{{1,2,2},{1,4,3}}', 2); -- not allowed
select array_remove(array['X','X','X'], 'X') = '{}';
select array_replace(array[[],[]],5,3);
select array_replace(array[1,2,5,4],5,3);
select array_replace(array[1,2,5,4],5,NULL);
select array_replace(array[1,2,NULL,4,NULL],NULL,5);
select array_replace(array['A','B','DD','B'],'B','CC');
select array_replace(array[1,NULL,3],NULL,NULL);
select array_replace(array['AB',NULL,'CDE'],NULL,'12');
select array_replace(array[[NULL,NULL],[NULL,NULL]],NULL,'12');
select array_replace(array[['AB',NULL,'CDE'],['CD','CDE',NULL]],NULL,'12');
select array_replace(array[[[1],[1]],[[1],[1]]], 1, 2);
select array_replace(array[[[1],[1]],[[NULL],[1]]], 1, NULL);

--array test in function, $1 is a string, $2 is delimiter, $3 is index, $4 is mode(for test purpose)
CREATE OR REPLACE FUNCTION substring_index(varchar, varchar, integer, integer)
RETURNS varchar AS $$
  DECLARE
    tokens varchar[];
    length integer ;
    indexnum integer;
  BEGIN
    tokens := pg_catalog.string_to_array($1, $2);
    length := pg_catalog.array_upper(tokens, 1);
    indexnum := length - ($3 * -1) + 1;
    IF $3 >= 0 THEN
      RETURN pg_catalog.array_to_string(tokens[1:$3], $2);
    ELSEIF $4 = 0 THEN
      RETURN pg_catalog.array_to_string(tokens[indexnum:length], $2);
    ELSEIF $4 = 1 THEN
      RETURN pg_catalog.array_to_string(tokens[indexnum:6], $2);
    ELSEIF $4 = 2 THEN
      RETURN pg_catalog.array_to_string(tokens[2:length], $2);
    ELSEIF $4 = 3 THEN
      RETURN pg_catalog.array_to_string(tokens[2:4], $2);
    END IF;
  END;
$$ IMMUTABLE STRICT LANGUAGE PLPGSQL;
create table array_test(a int[7]);
insert into array_test values('{12,3,4,5,6,7,8}');

select substring_index('12,3,4,5,6,7,8', ',', 3, 0), array_to_string(a[1:3], ',') from array_test;
select substring_index('12,3,4,5,6,7,8', ',', -3, 0), array_to_string(a[7 - 3 + 1:7], ',') from array_test;
select substring_index('12,3,4,5,6,7,8', ',', -2, 0), array_to_string(a[7 - 2 + 1:7], ',') from array_test;
select substring_index('12,3,4,5,6,7,8', ',', -3, 1), array_to_string(a[7 - 3 + 1:6], ',') from array_test;
select substring_index('12,3,4,5,6,7,8', ',', -3, 2), array_to_string(a[2:7], ',') from array_test;
select substring_index('12,3,4,5,6,7,8', ',', -3, 3), array_to_string(a[2:4], ',') from array_test;

drop table array_test;
drop function substring_index;
