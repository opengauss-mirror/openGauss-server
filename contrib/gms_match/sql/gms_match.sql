create extension gms_match;
create extension gms_output;

select gms_match.edit_distance(NULL, NULL);
select gms_match.edit_distance(NULL, 'ab');
select gms_match.edit_distance(NULL, 'ff');
select gms_match.edit_distance('', '');
select gms_match.edit_distance('', 'ab');

select gms_match.edit_distance(' ', 'ab');
select gms_match.edit_distance('ab', 'ab');
select gms_match.edit_distance('00', 'ff');
select gms_match.edit_distance('00', 'fff');
select gms_match.edit_distance('shackleford', 'shackelford');
select gms_match.edit_distance('ssttten', 'sitting');
select gms_match.edit_distance('ssitten', 'sitting');
select gms_match.edit_distance('kitten', 'sitting');
select gms_match.edit_distance('saturdy', 'monday');
select gms_match.edit_distance('abdfdjdfjfdkkgrmgjgjfgjhffd', 'kfkgndmdgns;ccvdermdfmdfefdfngtnt');
select gms_match.edit_distance('我爱你中国', '中国是我的祖国lalalala');
select gms_match.edit_distance('&^^(($$$))aaa', '中国是我的祖国lalalala');


select gms_match.edit_distance_similarity(NULL, NULL);
select gms_match.edit_distance_similarity(NULL, 'ab');
select gms_match.edit_distance_similarity(NULL, 'ff');
select gms_match.edit_distance_similarity('', '');
select gms_match.edit_distance_similarity('', 'ab');

select gms_match.edit_distance_similarity(' ', 'ab');
select gms_match.edit_distance_similarity('ab', 'ab');
select gms_match.edit_distance_similarity('00', 'ff');
select gms_match.edit_distance_similarity('00', 'fff');
select gms_match.edit_distance_similarity('shackleford', 'shackelford');
select gms_match.edit_distance_similarity('ssttten', 'sitting');
select gms_match.edit_distance_similarity('ssitten', 'sitting');
select gms_match.edit_distance_similarity('kitten', 'sitting');
select gms_match.edit_distance_similarity('saturdy', 'monday');
select gms_match.edit_distance_similarity('abdfdjdfjfdkkgrmgjgjfgjhffd', 'kfkgndmdgns;ccvdermdfmdfefdfngtnt');
select gms_match.edit_distance_similarity('我爱你中国', '中国是我的祖国lalalala');
select gms_match.edit_distance_similarity('&^^(($$$))aaa', '中国是我的祖国lalalala');

select gms_output.enable(200);

DECLARE
   distance NUMBER;
BEGIN
   distance := GMS_MATCH.EDIT_DISTANCE('kitten', 'sitting');
   GMS_OUTPUT.PUT_LINE('Edit distance between "kitten" and "sitting": ' || distance);
end;
/


DECLARE
   similarity NUMBER;
BEGIN
   similarity := GMS_MATCH.EDIT_DISTANCE_SIMILARITY('kitten', 'sitting');
   GMS_OUTPUT.PUT_LINE('Edit distance similarity between "kitten" and "sitting": ' || similarity);
end;
/

drop schema gms_match;
drop schema gms_match cascade;

drop extension gms_match;
drop extension gms_output;

-- test create again
create extension gms_match;
drop extension gms_match;
