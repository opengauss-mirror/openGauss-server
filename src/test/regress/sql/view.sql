create view tb1_view as select * from tb1;
create view tb1_temp_view as select * from tb1_temp;
create view tb1_column_view as select * from tb1_column;
create view tb1_partition_view as select * from tb1_partition;

create view tb1_view_1 as select col_tinyint,col_numeric,col_decimal,col_char from tb1;
create view tb1_column_view_1 as select col_tinyint,col_numeric,col_decimal,col_char from tb1;
create view tb1_partition_view_1 as select col_tinyint,col_numeric,col_decimal,col_char from tb1;