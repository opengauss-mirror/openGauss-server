--
-- testserial
--
create table tbl_small_serial(a smallserial, b varchar(2));

insert into tbl_small_serial(b) values ('aa');

create table tbl_serial(a serial, b varchar(2));

insert into tbl_serial(b) values ('bb');

create table tbl_big_serial(a bigserial, b varchar(2));

insert into tbl_big_serial(b) values ('cc');

CREATE  FUNCTION h_testfun6 (c_SMALLSERIAL SMALLSERIAL) RETURNS SMALLSERIAL   AS $$
        BEGIN
                RETURN c_SMALLSERIAL ;
        END;
$$ LANGUAGE plpgsql;
CREATE  FUNCTION h_testfun7 (c_SERIAL SERIAL) RETURNS SERIAL   AS $$
        BEGIN
                RETURN c_SERIAL ;
        END;
$$ LANGUAGE plpgsql;
CREATE  FUNCTION h_testfun8 (c_BIGSERIAL BIGSERIAL) RETURNS BIGSERIAL   AS $$
        BEGIN
                RETURN c_BIGSERIAL ;
        END;
$$ LANGUAGE plpgsql;

SELECT h_testfun6((SELECT a from  tbl_small_serial));

SELECT h_testfun7((SELECT a from  tbl_serial));

SELECT h_testfun8((SELECT a from  tbl_big_serial));

drop table tbl_small_serial;
drop table tbl_serial;
drop table tbl_big_serial;

drop function h_testfun6;
drop function h_testfun7;
drop function h_testfun8;