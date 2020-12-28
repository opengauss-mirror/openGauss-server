--not really to use.
create foreign table redistable.redis_table_1061_ft_without_error_table(c1 int, c2 text) SERVER gsmpp_server options (location 'gsfs://10.145.130.23:15866/*',format 'text',delimiter ',',encoding 'utf8', mode 'Normal');
create foreign table redistable.redis_table_1062_ft_without_error_table(c1 int, c2 text) SERVER gsmpp_server options (location 'gsfs://10.145.130.23:15866/*',format 'text',delimiter ',',encoding 'utf8', mode 'Normal') with redis_table_1062_error_table;

--HDFS foreign table.
create server hdfs_server foreign data wrapper hdfs_fdw options (address '1.2.5.8:0',HDFSCFGPATH '/little/bird/can/remember',type 'HDFS');
create foreign table redistable.redis_table_1063_ft_hdfs(c1 int, c2 text) SERVER hdfs_server options(format 'orc', foldername '/the/niece/of/time') distribute by roundrobin;