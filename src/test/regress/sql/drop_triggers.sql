CREATE SCHEMA drop_triggers_test;

SET search_path TO drop_triggers_test;

CREATE TABLE IF NOT EXISTS yx_channel (
    yx_id bigint NOT NULL,
    channel tinyint  NOT NULL,
    raw_data tinyint DEFAULT NULL,
    q bigint DEFAULT NULL,
    ld_alias varchar(64) DEFAULT NULL,
    order_no smallint DEFAULT '-1',
    gin bigint DEFAULT NULL,
    source_mode tinyint DEFAULT NULL,
    change_time bigint DEFAULT '0',
    PRIMARY KEY (yx_id,channel)
);

CREATE or replace FUNCTION "ti_yx_channel_trigger_function"() RETURNS "trigger"
    LANGUAGE "plpgsql" NOT SHIPPABLE
AS $$
DECLARE
    num1 INTEGER;
BEGIN
    RETURN NEW;
END$$;

CREATE TRIGGER "ti_yx_channel" BEFORE INSERT ON "yx_channel" FOR EACH ROW EXECUTE PROCEDURE "ti_yx_channel_trigger_function"();

-- ok
drop table yx_channel;
drop TRIGGER IF EXISTS  ti_yx_channel on yx_channel;


CREATE TABLE IF NOT EXISTS yx_channel (
    yx_id bigint NOT NULL,
    channel tinyint  NOT NULL,
    raw_data tinyint DEFAULT NULL,
    q bigint DEFAULT NULL,
    ld_alias varchar(64) DEFAULT NULL,
    order_no smallint DEFAULT '-1',
    gin bigint DEFAULT NULL,
    source_mode tinyint DEFAULT NULL,
    change_time bigint DEFAULT '0',
    PRIMARY KEY (yx_id,channel)
);

CREATE TRIGGER "ti_yx_channel" BEFORE INSERT ON "yx_channel" FOR EACH ROW EXECUTE PROCEDURE "ti_yx_channel_trigger_function"();

-- not ok
drop table yx_channel;
drop TRIGGER  ti_yx_channel on yx_channel;
drop TRIGGER IF EXISTS  ti_yx_channel on yx_channel;



CREATE TABLE IF NOT EXISTS yx_channel (
    yx_id bigint NOT NULL,
    channel tinyint  NOT NULL,
    raw_data tinyint DEFAULT NULL,
    q bigint DEFAULT NULL,
    ld_alias varchar(64) DEFAULT NULL,
    order_no smallint DEFAULT '-1',
    gin bigint DEFAULT NULL,
    source_mode tinyint DEFAULT NULL,
    change_time bigint DEFAULT '0',
    PRIMARY KEY (yx_id,channel)
);

CREATE TRIGGER "ti_yx_channel" BEFORE INSERT ON "yx_channel" FOR EACH ROW EXECUTE PROCEDURE "ti_yx_channel_trigger_function"();

drop TRIGGER ti_yx_channel on yx_channel;

drop TRIGGER IF EXISTS ti_yx_channel on yx_channel;

DROP SCHEMA drop_triggers_test CASCADE

