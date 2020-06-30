drop table if exists test_trigger_des_tbl_2 cascade;
drop sequence if exists dump_seq_2;
CREATE TABLE test_trigger_des_tbl_2(id1 INT, id2 INT, id3 INT);
create sequence dump_seq_2;
CREATE OR REPLACE FUNCTION tri_insert_func_2() RETURNS TRIGGER AS
$$
DECLARE
BEGIN
INSERT INTO test_trigger_des_tbl_2 VALUES(nextval('dump_seq_2'),currval('dump_seq_2'),currval('dump_seq_2'));
RETURN NEW;
END
$$ LANGUAGE PLPGSQL;
drop trigger if exists insert_trigger_2 on tb1_column cascade;
create trigger insert_trigger_2 after insert on tb1_column
        for each row 
		execute procedure tri_insert_func_2();
		
