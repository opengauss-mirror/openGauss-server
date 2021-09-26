drop table if exists t;
drop table if exists table_before_row_insert;
drop table if exists table_after_row_insert;
drop table if exists table_before_statement_insert;
drop table if exists table_after_statement_insert;

drop table if exists table_before_row_delete;
drop table if exists table_after_row_delete;
drop table if exists table_before_statement_delete;
drop table if exists table_after_statement_delete;

drop table if exists table_before_row_update;
drop table if exists table_after_row_update;
drop table if exists table_before_statement_update;
drop table if exists table_after_statement_update;

drop table if exists table_before_statement_truncate;
drop table if exists table_after_statement_truncate;

create table t(a int, b int, c int, d int)with(storage_type=ustore);
create table table_before_row_insert(a int, b int);
create table table_after_row_insert(a int, b int);
create table table_before_statement_insert(a varchar(20), b timestamp);
create table table_after_statement_insert(a varchar(20), b timestamp);


create table table_before_row_delete(a int, b int, c timestamp);
create table table_after_row_delete(a int, b int, c timestamp);
create table table_before_statement_delete(a varchar(20), b timestamp);
create table table_after_statement_delete(a varchar(20), b timestamp);

create table table_before_row_update(a int, b int, c timestamp);
create table table_after_row_update(a int, b int, c timestamp);
create table table_before_statement_update(a varchar(20), b timestamp);
create table table_after_statement_update(a varchar(20), b timestamp);


create table table_before_statement_truncate(a varchar(20), b timestamp);
create table table_after_statement_truncate(a varchar(20), b timestamp);


--- before statement truncate

CREATE OR REPLACE FUNCTION function_before_statement_truncate()
  RETURNS trigger AS
$$
BEGIN
    INSERT INTO table_before_statement_truncate VALUES('before', now());
    return NULL;
END;
$$
LANGUAGE 'plpgsql';

CREATE  TRIGGER trigger_before_statement_truncate
before truncate
ON t
FOR EACH statement
EXECUTE PROCEDURE function_before_statement_truncate();

--- after statement truncate

CREATE OR REPLACE FUNCTION function_after_statement_truncate()
  RETURNS trigger AS
$$
BEGIN
    INSERT INTO table_after_statement_truncate VALUES('after', now());
    return NULL;
END;
$$
LANGUAGE 'plpgsql';

CREATE  TRIGGER trigger_after_statement_truncate
after truncate
ON t
FOR EACH statement
EXECUTE PROCEDURE function_after_statement_truncate();


--- before row insert

CREATE OR REPLACE FUNCTION function_before_row_insert()
  RETURNS trigger AS
$$
BEGIN
IF NEW.c IS NULL THEN
            NEW.c = 100;
        END IF;
IF NEW.d IS NULL THEN
            NEW.d = new.a + new.b;
        END IF;
    NEW.b = NEW.b * NEW.b;
    INSERT INTO table_before_row_insert VALUES(new.b, new.a);
    RETURN NEW;
END;
$$
LANGUAGE 'plpgsql';

CREATE  TRIGGER trigger_before_row_insert
before insert
ON t
FOR EACH ROW
EXECUTE PROCEDURE function_before_row_insert();

--- after row insert

CREATE OR REPLACE FUNCTION function_after_row_insert()
  RETURNS trigger AS
$$
BEGIN
IF NEW.c IS NULL THEN
            NEW.c = 100;
        END IF;
IF NEW.d IS NULL THEN
            NEW.d = new.a + new.b;
        END IF;
    NEW.b = NEW.b * NEW.b;
    INSERT INTO table_after_row_insert VALUES(new.c, new.d);
    RETURN NEW;
END;
$$
LANGUAGE 'plpgsql';

CREATE  TRIGGER trigger_after_row_insert
after insert
ON t
FOR EACH ROW
EXECUTE PROCEDURE function_after_row_insert();

--- before statement insert

CREATE OR REPLACE FUNCTION function_before_statement_insert()
  RETURNS trigger AS
$$
BEGIN
    INSERT INTO table_before_statement_insert VALUES('before', now());
    return NULL;
END;
$$
LANGUAGE 'plpgsql';

CREATE  TRIGGER trigger_before_statement_insert
before insert
ON t
FOR EACH statement
EXECUTE PROCEDURE function_before_statement_insert();

--- after statement insert

CREATE OR REPLACE FUNCTION function_after_statement_insert()
  RETURNS trigger AS
$$
BEGIN
    INSERT INTO table_after_statement_insert VALUES('after', now());
    return NULL;
END;
$$
LANGUAGE 'plpgsql';

CREATE  TRIGGER trigger_after_statement_insert
after insert
ON t
FOR EACH statement
EXECUTE PROCEDURE function_after_statement_insert();


--- before row update

CREATE OR REPLACE FUNCTION function_before_row_update()
  RETURNS trigger AS
$$
BEGIN
IF NEW.c IS NULL THEN
            NEW.c = 100;
        END IF;
IF NEW.d IS NULL THEN
            NEW.d = new.a + new.b;
        END IF;
    NEW.b = NEW.b * NEW.b;
    INSERT INTO table_before_row_update VALUES(old.a, new.b, now());
    RETURN NEW;
END;
$$
LANGUAGE 'plpgsql';

CREATE  TRIGGER trigger_before_row_update
before update
ON t
FOR EACH ROW
EXECUTE PROCEDURE function_before_row_update();


--- after row update

CREATE OR REPLACE FUNCTION function_after_row_update()
  RETURNS trigger AS
$$
BEGIN
IF NEW.c IS NULL THEN
            NEW.c = 100;
        END IF;
IF NEW.d IS NULL THEN
            NEW.d = new.a + new.b;
        END IF;
    NEW.b = NEW.b * NEW.b;
    INSERT INTO table_after_row_update VALUES(old.a, new.b, now());
    RETURN NEW;
END;
$$
LANGUAGE 'plpgsql';

CREATE  TRIGGER trigger_after_row_update
after update
ON t
FOR EACH ROW
EXECUTE PROCEDURE function_after_row_update();

--- before statement update

CREATE OR REPLACE FUNCTION function_before_statement_update()
  RETURNS trigger AS
$$
BEGIN
    INSERT INTO table_before_statement_update VALUES('before', now());
    return NULL;
END;
$$
LANGUAGE 'plpgsql';

CREATE  TRIGGER trigger_before_statement_update
before update
ON t
FOR EACH statement
EXECUTE PROCEDURE function_before_statement_update();

--- after statement update

CREATE OR REPLACE FUNCTION function_after_statement_update()
  RETURNS trigger AS
$$
BEGIN
    INSERT INTO table_after_statement_update VALUES('after', now());
    return NULL;
END;
$$
LANGUAGE 'plpgsql';

CREATE  TRIGGER trigger_after_statement_update
after update
ON t
FOR EACH statement
EXECUTE PROCEDURE function_after_statement_update();


--- before row delete

CREATE OR REPLACE FUNCTION function_before_row_delete()
  RETURNS trigger AS
$$
BEGIN
    INSERT INTO table_before_row_delete VALUES(old.a, old.b, now());
    RETURN old;
END;
$$
LANGUAGE 'plpgsql';

CREATE  TRIGGER trigger_before_row_delete
before delete
ON t
FOR EACH ROW
EXECUTE PROCEDURE function_before_row_delete();

--- after row delete

CREATE OR REPLACE FUNCTION function_after_row_delete()
  RETURNS trigger AS
$$
BEGIN
    INSERT INTO table_after_row_delete VALUES(old.a, old.c, now());
    RETURN NEW;
END;
$$
LANGUAGE 'plpgsql';

CREATE  TRIGGER trigger_after_row_delete
after delete
ON t
FOR EACH ROW
EXECUTE PROCEDURE function_after_row_delete();

--- before statement delete

CREATE OR REPLACE FUNCTION function_before_statement_delete()
  RETURNS trigger AS
$$
BEGIN
    INSERT INTO table_before_statement_delete VALUES('before', now());
    return NULL;
END;
$$
LANGUAGE 'plpgsql';

CREATE  TRIGGER trigger_before_statement_delete
before delete
ON t
FOR EACH statement
EXECUTE PROCEDURE function_before_statement_delete();

--- after statement delete

CREATE OR REPLACE FUNCTION function_after_statement_delete()
  RETURNS trigger AS
$$
BEGIN
    INSERT INTO table_after_statement_delete VALUES('after', now());
    return NULL;
END;
$$
LANGUAGE 'plpgsql';

CREATE  TRIGGER trigger_after_statement_delete
after delete
ON t
FOR EACH statement
EXECUTE PROCEDURE function_after_statement_delete();



insert into t values(1,1);
select * from t;
select * from table_before_row_insert;
select * from table_after_row_insert;
select * from table_before_statement_insert;
select * from table_after_statement_insert;

update t set a=2, b = 2;
select * from t;
select * from table_before_row_update;
select * from table_after_row_update;
select * from table_before_statement_update;
select * from table_after_statement_update;

delete from t;
select * from t;
select * from table_before_row_delete;
select * from table_after_row_delete;
select * from table_before_statement_delete;
select * from table_after_statement_delete;

truncate table t;

select * from table_before_statement_truncate;
select * from table_after_statement_truncate;

drop table t                               cascade;
drop table table_after_row_delete          cascade;
drop table table_after_row_insert          cascade;
drop table table_after_row_update          cascade;
drop table table_after_statement_delete    cascade;
drop table table_after_statement_insert    cascade;
drop table table_after_statement_truncate  cascade;
drop table table_after_statement_update    cascade;
drop table table_before_row_delete         cascade;
drop table table_before_row_insert         cascade;
drop table table_before_row_update         cascade;
drop table table_before_statement_delete   cascade;
drop table table_before_statement_insert   cascade;
drop table table_before_statement_truncate cascade;
drop table table_before_statement_update   cascade;