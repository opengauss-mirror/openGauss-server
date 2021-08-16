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
