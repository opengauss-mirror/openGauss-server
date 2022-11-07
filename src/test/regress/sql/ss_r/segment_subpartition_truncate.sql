SET CURRENT_SCHEMA TO segment_subpartition_truncate;
select * from list_list partition (p_201901);

select * from list_list subpartition (p_201902_b);
