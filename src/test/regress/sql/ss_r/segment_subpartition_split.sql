SET CURRENT_SCHEMA TO segment_subpartition_split;
select * from list_list subpartition (p_201902_a) order by 1,2,3,4;
select * from list_list subpartition (p_201902_b) order by 1,2,3,4;
select * from list_list subpartition (p_201902_c) order by 1,2,3,4;

select * from range_range subpartition (p_201901_a) order by 1,2,3,4;
select * from range_range subpartition (p_201901_b) order by 1,2,3,4;
select * from range_range subpartition (p_201901_c) order by 1,2,3,4;
select * from range_range subpartition (p_201901_d) order by 1,2,3,4;

select * from range_range subpartition (p_201902_a) order by 1,2,3,4;
select * from range_range subpartition (p_201902_b) order by 1,2,3,4;
select * from range_range subpartition (p_201902_c) order by 1,2,3,4;
select * from range_range subpartition (p_201902_d) order by 1,2,3,4;