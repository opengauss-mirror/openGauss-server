CREATE OR REPLACE FUNCTION set_installation_true(IN group_name text)
RETURNS setof int
AS $$
DECLARE
        row_data record;
        row_name record;
        query_str text;
        query_str_nodes text;
        total_num bigint;
        BEGIN
                EXECUTE 'set xc_maintenance_mode = on';

                --Get all the coordinator node names
                query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type=''C''';

                FOR row_name IN EXECUTE(query_str_nodes) LOOP
                        query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''update pgxc_group set is_installation = true where group_name = ''''' || $1 || '''''''';
                        EXECUTE(query_str);
                END LOOP;

                EXECUTE 'set xc_maintenance_mode = off';

                RETURN ;
        END; $$
LANGUAGE plpgsql NOT FENCED;

CREATE OR REPLACE FUNCTION set_installation_false(IN group_name text)
RETURNS setof int
AS $$
DECLARE
        row_data record;
        row_name record;
        query_str text;
        query_str_nodes text;
        total_num bigint;
        BEGIN
                EXECUTE 'set xc_maintenance_mode = on';

                --Get all the coordinator node names
                query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type=''C''';

                FOR row_name IN EXECUTE(query_str_nodes) LOOP
                        query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''update pgxc_group set is_installation = false where group_name = ''''' || $1 || '''''''';
                        EXECUTE(query_str);
                END LOOP;

                EXECUTE 'set xc_maintenance_mode = off';

                RETURN ;
        END; $$
LANGUAGE plpgsql NOT FENCED;


CREATE OR REPLACE FUNCTION set_installation_true_all()
RETURNS setof int
AS $$
DECLARE
        row_data record;
        row_name record;
        query_str text;
        query_str_nodes text;
        total_num bigint;
        BEGIN
                EXECUTE 'set xc_maintenance_mode = on';

                --Get all the coordinator node names
                query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type=''C''';

                FOR row_name IN EXECUTE(query_str_nodes) LOOP
                        query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''update pgxc_group set is_installation = true''';
                        EXECUTE(query_str);
                END LOOP;

                EXECUTE 'set xc_maintenance_mode = off';

                RETURN ;
        END; $$
LANGUAGE plpgsql NOT FENCED;

CREATE OR REPLACE FUNCTION set_installation_false_all()
RETURNS setof int
AS $$
DECLARE
        row_data record;
        row_name record;
        query_str text;
        query_str_nodes text;
        total_num bigint;
        BEGIN
                EXECUTE 'set xc_maintenance_mode = on';

                --Get all the coordinator node names
                query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type=''C''';

                FOR row_name IN EXECUTE(query_str_nodes) LOOP
                        query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''update pgxc_group set is_installation = false''';
                        EXECUTE(query_str);
                END LOOP;

                EXECUTE 'set xc_maintenance_mode = off';

                RETURN ;
        END; $$
LANGUAGE plpgsql NOT FENCED;


create node group ng1 with(datanode1, datanode2, datanode3, datanode4, datanode5, datanode6);
select set_installation_false_all();
select set_installation_true('ng1');

create node group ng2 with(datanode1, datanode2, datanode3, datanode4, datanode5, datanode6, datanode7 ,datanode8 ,datanode9 ,datanode10 ,datanode11 ,datanode12);
select set_installation_false_all();
drop node group ng1;
drop node group ng2;

-- !! NOTICE: there is a group in table pgxc_group necessarily here.
select set_installation_true_all();


drop FUNCTION set_installation_true;
drop FUNCTION set_installation_false;
drop FUNCTION set_installation_true_all;
drop FUNCTION set_installation_false_all;

--test create table if not exists
create node group ng1 with(datanode1);
create table if not exists  t1(x int) to group ng1;
create table if not exists  t1(x int) to group ng1;
execute direct on (datanode3) 'select relname from pg_class where relname = ''t1'' ';
drop table t1;
drop node group ng1;
