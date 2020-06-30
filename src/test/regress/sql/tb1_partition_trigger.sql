drop table if exists test_trigger_des_tbl_partition cascade;
drop sequence if exists dump_seq_3;
CREATE TABLE test_trigger_des_tbl_partition(id1 INT, id2 INT, id3 INT);
create sequence dump_seq_3;
CREATE OR REPLACE FUNCTION tri_insert_func_3() RETURNS TRIGGER AS
$$
DECLARE
BEGIN
INSERT INTO test_trigger_des_tbl_partition VALUES(nextval('dump_seq_3'),currval('dump_seq_3'),currval('dump_seq_3'));
RETURN NEW;
END
$$ LANGUAGE PLPGSQL;
drop trigger if exists insert_trigger_3 on tb1_partition cascade;
create trigger insert_trigger_3 after insert on tb1_partition
        for each row 
		execute procedure tri_insert_func_3();
		
