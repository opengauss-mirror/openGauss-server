drop schema if exists anydata_type;
create schema anydata_type;
set search_path=anydata_type;

set behavior_compat_options='proc_outparam_override';

/* anytype begincreate typecode */
declare
   v_anytype    anytype;
   prec         int;
   scale        int;
   len          int; 
   csid         int;
   csfrm        int;
   schema_name  VARCHAR2(20); 
   type_name    VARCHAR2(20); 
   version      varchar2(20);
   numelems     int;
   result       int;
begin
    anytype.BeginCreate(101, v_anytype);
    v_anytype.setinfo(255, 127, 2147483647, 65535, 33);
	anytype.endcreate(v_anytype);
    result := v_anytype.getinfo(prec,scale,len,csid,csfrm,schema_name,type_name,version,numelems);
    RAISE NOTICE 'Output values are: %, %, %, %, %, %', prec, scale, len, csid, csfrm, schema_name;
	RAISE NOTICE 'More output values are: %, %, %, %', type_name, version, numelems, result;

    anytype.BeginCreate(113, v_anytype);
    v_anytype.setinfo(255, 127, 2147483647, 65535, 33);
	anytype.endcreate(v_anytype);
    result := v_anytype.getinfo(prec,scale,len,csid,csfrm,schema_name,type_name,version,numelems);
    RAISE NOTICE 'Output values are: %, %, %, %, %, %', prec, scale, len, csid, csfrm, schema_name;
	RAISE NOTICE 'More output values are: %, %, %, %', type_name, version, numelems, result;

    anytype.BeginCreate(96, v_anytype);
    v_anytype.setinfo(255, 127, 2147483647, 65535, 33);
	anytype.endcreate(v_anytype);
    result := v_anytype.getinfo(prec,scale,len,csid,csfrm,schema_name,type_name,version,numelems);
    RAISE NOTICE 'Output values are: %, %, %, %, %, %', prec, scale, len, csid, csfrm, schema_name;
	RAISE NOTICE 'More output values are: %, %, %, %', type_name, version, numelems, result;

    anytype.BeginCreate(12, v_anytype);
    v_anytype.setinfo(255, 127, 2147483647, 65535, 33);
	anytype.endcreate(v_anytype);
    result := v_anytype.getinfo(prec,scale,len,csid,csfrm,schema_name,type_name,version,numelems);
    RAISE NOTICE 'Output values are: %, %, %, %, %, %', prec, scale, len, csid, csfrm, schema_name;
	RAISE NOTICE 'More output values are: %, %, %, %', type_name, version, numelems, result;

    anytype.BeginCreate(286, v_anytype);
    v_anytype.setinfo(255, 127, 2147483647, 65535, 33);
	anytype.endcreate(v_anytype);
    result := v_anytype.getinfo(prec,scale,len,csid,csfrm,schema_name,type_name,version,numelems);
    RAISE NOTICE 'Output values are: %, %, %, %, %, %', prec, scale, len, csid, csfrm, schema_name;
	RAISE NOTICE 'More output values are: %, %, %, %', type_name, version, numelems, result;

    anytype.BeginCreate(2, v_anytype);
    v_anytype.setinfo(255, 127, 2147483647, 65535, 33);
	anytype.endcreate(v_anytype);
    result := v_anytype.getinfo(prec,scale,len,csid,csfrm,schema_name,type_name,version,numelems);
    RAISE NOTICE 'Output values are: %, %, %, %, %, %', prec, scale, len, csid, csfrm, schema_name;
	RAISE NOTICE 'More output values are: %, %, %, %', type_name, version, numelems, result;

    anytype.BeginCreate(287, v_anytype);
    v_anytype.setinfo(255, 127, 2147483647, 65535, 33);
	anytype.endcreate(v_anytype);
    result := v_anytype.getinfo(prec,scale,len,csid,csfrm,schema_name,type_name,version,numelems);
    RAISE NOTICE 'Output values are: %, %, %, %, %, %', prec, scale, len, csid, csfrm, schema_name;
	RAISE NOTICE 'More output values are: %, %, %, %', type_name, version, numelems, result;

    anytype.BeginCreate(95, v_anytype);
    v_anytype.setinfo(255, 127, 2147483647, 65535, 33);
	anytype.endcreate(v_anytype);
    result := v_anytype.getinfo(prec,scale,len,csid,csfrm,schema_name,type_name,version,numelems);
    RAISE NOTICE 'Output values are: %, %, %, %, %, %', prec, scale, len, csid, csfrm, schema_name;
	RAISE NOTICE 'More output values are: %, %, %, %', type_name, version, numelems, result;

    anytype.BeginCreate(187, v_anytype);
    v_anytype.setinfo(255, 127, 2147483647, 65535, 33);
	anytype.endcreate(v_anytype);
    result := v_anytype.getinfo(prec,scale,len,csid,csfrm,schema_name,type_name,version,numelems);
    RAISE NOTICE 'Output values are: %, %, %, %, %, %', prec, scale, len, csid, csfrm, schema_name;
	RAISE NOTICE 'More output values are: %, %, %, %', type_name, version, numelems, result;

    anytype.BeginCreate(188, v_anytype);
    v_anytype.setinfo(255, 127, 2147483647, 65535, 33);
	anytype.endcreate(v_anytype);
    result := v_anytype.getinfo(prec,scale,len,csid,csfrm,schema_name,type_name,version,numelems);
    RAISE NOTICE 'Output values are: %, %, %, %, %, %', prec, scale, len, csid, csfrm, schema_name;
	RAISE NOTICE 'More output values are: %, %, %, %', type_name, version, numelems, result;

    anytype.BeginCreate(1, v_anytype);
    v_anytype.setinfo(255, 127, 2147483647, 65535, 33);
	anytype.endcreate(v_anytype);
    result := v_anytype.getinfo(prec,scale,len,csid,csfrm,schema_name,type_name,version,numelems);
    RAISE NOTICE 'Output values are: %, %, %, %, %, %', prec, scale, len, csid, csfrm, schema_name;
	RAISE NOTICE 'More output values are: %, %, %, %', type_name, version, numelems, result;

    anytype.BeginCreate(9, v_anytype);
    v_anytype.setinfo(255, 127, 2147483647, 65535, 33);
	anytype.endcreate(v_anytype);
    result := v_anytype.getinfo(prec,scale,len,csid,csfrm,schema_name,type_name,version,numelems);
    RAISE NOTICE 'Output values are: %, %, %, %, %, %', prec, scale, len, csid, csfrm, schema_name;
	RAISE NOTICE 'More output values are: %, %, %, %', type_name, version, numelems, result;

    anytype.BeginCreate(999, v_anytype);--error
    v_anytype.setinfo(255, 127, 2147483647, 65535, 33);
	anytype.endcreate(v_anytype);
    result := v_anytype.getinfo(prec,scale,len,csid,csfrm,schema_name,type_name,version,numelems);
    RAISE NOTICE 'Output values are: %, %, %, %, %, %', prec, scale, len, csid, csfrm, schema_name;
	RAISE NOTICE 'More output values are: %, %, %, %', type_name, version, numelems, result;
end;
/

declare
   v_anytype    anytype;
begin
    anytype.BeginCreate(101, NULL);
    v_anytype.setinfo(-1, 127, 2147483647, 65535, 33);
	anytype.endcreate(v_anytype);
end;
/

declare
   v_anytype    anytype;
begin
    anytype.BeginCreate(101, v_anytype);
    v_anytype.setinfo(-0, 127, 2147483647, 65535, 33);
	anytype.endcreate(v_anytype);
end;
/

declare
   v_anytype    anytype;
begin
    anytype.BeginCreate(101, v_anytype);
    v_anytype.setinfo(256, 127, 2147483647, 65535, 33);
	anytype.endcreate(v_anytype);
end;
/

declare
   v_anytype    anytype;
begin
    anytype.BeginCreate(101, v_anytype);
    v_anytype.setinfo(255, -128, 2147483647, 65535, 33);
	anytype.endcreate(v_anytype);
end;
/

declare
   v_anytype    anytype;
begin
    anytype.BeginCreate(101, v_anytype);
    v_anytype.setinfo(255, -127, 2147483647, 65535, 33);
	anytype.endcreate(v_anytype);
end;
/

declare
   v_anytype    anytype;
begin
    anytype.BeginCreate(101, v_anytype);
    v_anytype.setinfo(255, 128, 2147483647, 65535, 33);
	anytype.endcreate(v_anytype);
end;
/

declare
   v_anytype    anytype;
begin
    anytype.BeginCreate(101, v_anytype);
    v_anytype.setinfo(255, 127, -1, 65535, 33);
	anytype.endcreate(v_anytype);
end;
/

declare
   v_anytype    anytype;
begin
    anytype.BeginCreate(101, v_anytype);
    v_anytype.setinfo(255, 127, 0, 65535, 33);
	anytype.endcreate(v_anytype);
end;
/

declare
   v_anytype    anytype;
begin
    anytype.BeginCreate(101, v_anytype);
    v_anytype.setinfo(255, 127, 2147483648, 65535, 33);
	anytype.endcreate(v_anytype);
end;
/

declare
   v_anytype    anytype;
begin
    anytype.BeginCreate(101, v_anytype);
    v_anytype.setinfo(255, 127, 2147483647, -1, 33);
	anytype.endcreate(v_anytype);
end;
/

declare
   v_anytype    anytype;
begin
    anytype.BeginCreate(101, v_anytype);
    v_anytype.setinfo(255, 127, 2147483647, 0, 33);
	anytype.endcreate(v_anytype);
end;
/

declare
   v_anytype    anytype;
begin
    anytype.BeginCreate(101, v_anytype);
    v_anytype.setinfo(255, 127, 2147483647, 65536, 33);
	anytype.endcreate(v_anytype);
end;
/

declare
   v_anytype    anytype;
begin
    anytype.BeginCreate(101, v_anytype);
    v_anytype.setinfo(255, 127, 2147483647, 65535, -1);
	anytype.endcreate(v_anytype);
end;
/

declare
   v_anytype    anytype;
begin
    anytype.BeginCreate(101, v_anytype);
    v_anytype.setinfo(255, 127, 2147483647, 65535, 0);
	anytype.endcreate(v_anytype);
end;
/

declare
   v_anytype    anytype;
begin
    anytype.BeginCreate(101, v_anytype);
    v_anytype.setinfo(255, 127, 2147483647, 65535, 256);
	anytype.endcreate(v_anytype);
end;
/

declare
   v_anytype    anytype;
begin
    anytype.BeginCreate(101, v_anytype);
    v_anytype.setinfo(255, 127, 2147483647, 65535, 255);
	anytype.endcreate(v_anytype);
end;
/

declare
   v_anytype    anytype;
begin
    anytype.BeginCreate(101, v_anytype);
	anytype.endcreate(v_anytype);
end;
/

declare
   v_anytype    anytype;
begin
    anytype.BeginCreate(101, v_anytype);
    v_anytype.setinfo(255, 127, 2147483647, 65535, 255);
	anytype.endcreate(v_anytype);
    v_anytype.setinfo(255, 127, 2147483647, 65535, 255);
end;
/

declare
   v_anytype    anytype;
begin
	anytype.endcreate(v_anytype);
end;
/

declare
   v_anytype    anytype;
begin
    anytype.BeginCreate(101, v_anytype);
    v_anytype.setinfo(255, 127, 2147483647, 65535, 255);
	anytype.endcreate(v_anytype);
    anytype.endcreate(v_anytype);
end;
/

declare
    v_anytype    anytype;
    v_anydata anydata;
    typecode int;
    v_number float8;
    type_name    VARCHAR2(20);
begin
    v_anydata := anydata.convertBdouble(100.8);
    v_number := v_anydata.accessBdouble();
    IF v_anytype IS NULL THEN
        raise notice 'is NULL';
    ELSE
        raise notice 'not NULL';
    END IF;
    type_name = v_anydata.gettypename();
    raise notice '%, %', v_number, type_name;
end;
/

declare
    v_anytype    anytype;
    v_anydata anydata;
    typecode int;
    v_number float8;
    type_name    VARCHAR2(20);
begin
    v_anydata := anydata.convertBdouble(100.8);
    v_number := v_anydata.accessNumber();
    IF v_anytype IS NULL THEN
        raise notice 'is NULL';
    ELSE
        raise notice 'not NULL';
    END IF;
    type_name = v_anydata.gettypename();
    raise notice '%, %', v_number, type_name;
end;
/

declare
    v_anytype    anytype;
    v_anydata anydata;
    typecode int;
    v_number blob;
    type_name    VARCHAR2(20);
begin
    v_anydata := anydata.convertBlob('abc12');
    v_number := v_anydata.accessBlob();
    IF v_anytype IS NULL THEN
        raise notice 'is NULL';
    ELSE
        raise notice 'not NULL';
    END IF;
    type_name = v_anydata.gettypename();
    raise notice '%, %', v_number, type_name;
end;
/

declare
    v_anytype    anytype;
    v_anydata anydata;
    typecode int;
    v_number char(20);
    type_name    VARCHAR2(20);
begin
    v_anydata := anydata.convertchar('abc123,?');
    v_number := v_anydata.accesschar();
    IF v_anytype IS NULL THEN
        raise notice 'is NULL';
    ELSE
        raise notice 'not NULL';
    END IF;
    type_name = v_anydata.gettypename();
    raise notice '%, %', v_number, type_name;
end;
/

declare
    v_anytype    anytype;
    v_anydata anydata;
    typecode int;
    v_number date;
    type_name    VARCHAR2(20);
begin
    v_anydata := anydata.convertdate('20250330');
    v_number := v_anydata.accessdate();
    IF v_anytype IS NULL THEN
        raise notice 'is NULL';
    ELSE
        raise notice 'not NULL';
    END IF;
    type_name = v_anydata.gettypename();
    raise notice '%, %', v_number, type_name;
end;
/

declare
    v_anytype    anytype;
    v_anydata anydata;
    typecode int;
    v_number nchar(10);
    type_name    VARCHAR2(20);
begin
    v_anydata := anydata.convertnchar('abc123,?');
    v_number := v_anydata.accessnchar();
    IF v_anytype IS NULL THEN
        raise notice 'is NULL';
    ELSE
        raise notice 'not NULL';
    END IF;
    type_name = v_anydata.gettypename();
    raise notice '%, %', v_number, type_name;
end;
/

declare
    v_anytype    anytype;
    v_anydata anydata;
    typecode int;
    v_number number;
    type_name    VARCHAR2(20);
begin
    v_anydata := anydata.convertnumber('1.3e6');
    v_number := v_anydata.accessnumber();
    IF v_anytype IS NULL THEN
        raise notice 'is NULL';
    ELSE
        raise notice 'not NULL';
    END IF;
    type_name = v_anydata.gettypename();
    raise notice '%, %', v_number, type_name;
end;
/

declare
    v_anytype    anytype;
    v_anydata anydata;
    typecode int;
    v_number nvarchar2;
    type_name    VARCHAR2(20);
begin
    v_anydata := anydata.convertnvarchar2('测试1abc?');
    v_number := v_anydata.accessnvarchar2();
    IF v_anytype IS NULL THEN
        raise notice 'is NULL';
    ELSE
        raise notice 'not NULL';
    END IF;
    type_name = v_anydata.gettypename();
    raise notice '%, %', v_number, type_name;
end;
/

declare
    v_anytype    anytype;
    v_anydata anydata;
    typecode int;
    v_number raw;
    type_name    VARCHAR2(20);
begin
    v_anydata := anydata.convertraw('fa1');
    v_number := v_anydata.accessraw();
    IF v_anytype IS NULL THEN
        raise notice 'is NULL';
    ELSE
        raise notice 'not NULL';
    END IF;
    type_name = v_anydata.gettypename();
    raise notice '%, %', v_number, type_name;
end;
/

declare
    v_anytype    anytype;
    v_anydata anydata;
    typecode int;
    v_number timestamp;
    type_name    VARCHAR2(20);
begin
    v_anydata := anydata.converttimestamp('20250330');
    v_number := v_anydata.accesstimestamp();
    IF v_anytype IS NULL THEN
        raise notice 'is NULL';
    ELSE
        raise notice 'not NULL';
    END IF;
    type_name = v_anydata.gettypename();
    raise notice '%, %', v_number, type_name;
end;
/

declare
    v_anytype    anytype;
    v_anydata anydata;
    typecode int;
    v_number timestamptz;
    type_name    VARCHAR2(20);
begin
    v_anydata := anydata.converttimestamptz('20250330');
    v_number := v_anydata.accesstimestamptz();
    IF v_anytype IS NULL THEN
        raise notice 'is NULL';
    ELSE
        raise notice 'not NULL';
    END IF;
    type_name = v_anydata.gettypename();
    raise notice '%, %', v_number, type_name;
end;
/

declare
    v_anytype    anytype;
    v_anydata anydata;
    typecode int;
    v_number varchar2;
    type_name    VARCHAR2(20);
begin
    v_anydata := anydata.convertvarchar2('测试1abc?');
    v_number := v_anydata.accessvarchar2();
    IF v_anytype IS NULL THEN
        raise notice 'is NULL';
    ELSE
        raise notice 'not NULL';
    END IF;
    type_name = v_anydata.gettypename();
    raise notice '%, %', v_number, type_name;
end;
/

declare
    v_anytype    anytype;
    v_anydata anydata;
    typecode int;
    v_number varchar;
    type_name    VARCHAR2(20);
begin
    v_anydata := anydata.convertvarchar('测试1abc?');
    v_number := v_anydata.accessvarchar();
    IF v_anytype IS NULL THEN
        raise notice 'is NULL';
    ELSE
        raise notice 'not NULL';
    END IF;
    type_name = v_anydata.gettypename();
    raise notice '%, %', v_number, type_name;
end;
/

create table test_anydata(col1 anydata);
insert into test_anydata values(anydata.convertvarchar('测试1abc?'));
insert into test_anydata values(anydata.converttimestamptz('20250330'));

drop table test_anydata;

declare
    v_anytype    anytype;
    v_anydataset anydataset;
    v_string float8;
    v_type int;
    v_typname varchar2;
    v_count int;
begin
    anydataset.BeginCreate(1, v_anytype, v_anydataset);
    RAISE notice '%, %', array_length(v_anydataset.data, 1), v_anydataset.count;

    v_anydataset.addInstance();
    v_anydataset.SETvarchar('100.80');
    v_anydataset.addInstance();
    v_anydataset.SETvarchar('0.90');
    anydataset.EndCreate(v_anydataset);

    RAISE notice '%, %', array_length(v_anydataset.data, 1), v_anydataset.count;

    v_typname = v_anydataset.GETTYPENAME();
    v_count = v_anydataset.GetCount();

    RAISE notice 'name %, count %', v_typname, v_count;
    
    v_type := v_anydataset.getvarchar(v_string,1);
    raise notice '%, %', v_string, v_type;
    v_type := v_anydataset.getvarchar(v_string,2);
    raise notice '%, %', v_string, v_type;
    v_type := v_anydataset.getvarchar(v_string,3);
    raise notice '%, %', v_string, v_type;  
end;
/
declare
    v_anytype    anytype;
	v_anydataset anydataset;
	v_string     varchar(4000);
	v_type       int;
begin
    anydataset.beginCreate(1, v_anytype, v_anydataset);
    
    v_anydataset.addInstance();
    v_anydataset.setVarchar('hello');
    anydataset.endCreate(v_anydataset);
    v_type := v_anydataset.getVarchar(v_string,1);
    raise notice 'notice: %',v_string;
    v_type := v_anydataset.getVarchar(v_string,0);
    raise notice 'notice: %',v_string;
    v_type := v_anydataset.getVarchar(v_string,2);
    raise notice 'notice: %',v_string;
    v_type := v_anydataset.getVarchar(v_string,-1);
    raise notice 'notice: %',v_string;
	
end;
/

drop schema anydata_type cascade;