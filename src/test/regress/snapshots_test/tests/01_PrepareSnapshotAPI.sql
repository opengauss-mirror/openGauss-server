/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * 01_PrepareSnapshotAPI.sql
 *    DB4AI.Snapshot test case.
 *
 *
 * -------------------------------------------------------------------------
 */

CREATE OR REPLACE FUNCTION _db4ai_test.test(
)
RETURNS VOID LANGUAGE plpgsql SECURITY INVOKER SET client_min_messages TO NOTICE
AS $$
BEGIN

    PERFORM _db4ai_test.assert_exception('db4ai.prepare_snapshot(null,null,null)',
        'i_parent cannot be NULL or empty');
    PERFORM _db4ai_test.assert_exception('db4ai.prepare_snapshot(null, ''name'', null)',
        'i_parent must contain exactly one ''@'' character',
        'reference a snapshot using the format: snapshot_name@version');
    PERFORM _db4ai_test.assert_exception('db4ai.prepare_snapshot(null, ''name@org@com'', null)',
        'i_parent must contain exactly one ''@'' character',
        'reference a snapshot using the format: snapshot_name@version');
    PERFORM _db4ai_test.assert_exception('db4ai.prepare_snapshot(''_db4ai test'', ''name@version'',null)',
        'parent snapshot "_db4ai test"."name@version" does not exist');

    PERFORM db4ai.create_snapshot(null, 'test', '{SELECT 1 a, FROM DUAL}');

    PERFORM _db4ai_test.assert_exception('db4ai.prepare_snapshot(null, ''test@1.0.0'',null)',
        'i_commands array malformed',
        'pass SQL DML and DDL operations as TEXT[] literal, e.g. ''{ALTER, ADD a int, DROP c, DELETE, WHERE b=5, INSERT, FROM t, UPDATE, FROM t, SET x=y, SET z=f(z), WHERE t.u=v}''');
    PERFORM _db4ai_test.assert_exception('db4ai.prepare_snapshot(null, ''test@1.0.0'',''{}'')',
        'i_commands array malformed',
        'pass SQL DML and DDL operations as TEXT[] literal, e.g. ''{ALTER, ADD a int, DROP c, DELETE, WHERE b=5, INSERT, FROM t, UPDATE, FROM t, SET x=y, SET z=f(z), WHERE t.u=v}''');
    PERFORM _db4ai_test.assert_exception('db4ai.prepare_snapshot(null, ''test@1.0.0'', ''{{a},{a}}'')',
        'i_commands array malformed',
        'pass SQL DML and DDL operations as TEXT[] literal, e.g. ''{ALTER, ADD a int, DROP c, DELETE, WHERE b=5, INSERT, FROM t, UPDATE, FROM t, SET x=y, SET z=f(z), WHERE t.u=v}''');
    PERFORM _db4ai_test.assert_exception('db4ai.prepare_snapshot(null, ''test@1.0.0'',''{1, null, 2}'')',
        'i_commands array contains NULL values');

    PERFORM _db4ai_test.assert_exception('db4ai.prepare_snapshot(null, ''test@1.0.0'', ''{ WHERE ever , DELETE}'')',
        'missing INSERT / UPDATE / DELETE keyword before WHERE clause in i_commands at: ''WHERE ever''');
    PERFORM _db4ai_test.assert_exception('db4ai.prepare_snapshot(null, ''test@1.0.0'', ''{ALTER,  WHERE ever   }'')',
        'illegal WHERE clause in ALTER at: ''WHERE ever''');
    PERFORM _db4ai_test.assert_exception('db4ai.prepare_snapshot(null, ''test@1.0.0'', ''{DELETE, WHERE ever  , WHERE  also  }'')',
        'multiple WHERE clauses in % at: ''WHERE  also''');

    PERFORM _db4ai_test.assert_exception('db4ai.prepare_snapshot(null, ''test@1.0.0'', ''{ FROM  nowhere  }'')',
        'missing INSERT / UPDATE keyword before FROM clause in i_commands at: ''FROM  nowhere''');
    PERFORM _db4ai_test.assert_exception('db4ai.prepare_snapshot(null, ''test@1.0.0'', ''{AlTER,FrOM  nowhere }'')',
        'illegal FROM clause in ALTER at: ''FrOM  nowhere''');
    PERFORM _db4ai_test.assert_exception('db4ai.prepare_snapshot(null, ''test@1.0.0'', ''{DELeTE  ,  FRoM  nowhere }'')',
        'illegal FROM clause in DELETE at: ''FRoM  nowhere''');
    PERFORM _db4ai_test.assert_exception('db4ai.prepare_snapshot(null, ''test@1.0.0'', ''{  InSERT,  FrOM  nowhere , FROm  elsewhere }'')',
        'multiple FROM clauses in INSERT at: ''FROm  elsewhere''');
    PERFORM _db4ai_test.assert_exception('db4ai.prepare_snapshot(null, ''test@1.0.0'', ''{ upDatE ,  FrOM  nowhere , FROm  elsewhere }'')',
        'multiple FROM clauses in UPDATE at: ''FROm  elsewhere''');

    PERFORM _db4ai_test.assert_exception('db4ai.prepare_snapshot(null, ''test@1.0.0'', ''{ UsinG  something  }'')',
        'missing DELETE keyword before USING clause in i_commands at: ''UsinG  something''');
    PERFORM _db4ai_test.assert_exception('db4ai.prepare_snapshot(null, ''test@1.0.0'', ''{ Alter  , UsinG  something  }'')',
        'illegal USING clause in ALTER at: ''UsinG  something''');
    PERFORM _db4ai_test.assert_exception('db4ai.prepare_snapshot(null, ''test@1.0.0'', ''{ INSerT  , UsinG  something  }'')',
        'illegal USING clause in INSERT at: ''UsinG  something''');
    PERFORM _db4ai_test.assert_exception('db4ai.prepare_snapshot(null, ''test@1.0.0'', ''{  upDatE  , UsinG  something  }'')',
        'illegal USING clause in UPDATE at: ''UsinG  something''');
    PERFORM _db4ai_test.assert_exception('db4ai.prepare_snapshot(null, ''test@1.0.0'', ''{DeLETe,  USinG  this , USing  that }'')',
        'multiple USING clauses in DELETE at: ''USing  that''');

    PERFORM _db4ai_test.assert_exception('db4ai.prepare_snapshot(null, ''test@1.0.0'', ''{ Set  nothing  }'')',
        'missing UPDATE keyword before SET clause in i_commands at: ''Set  nothing''');
    PERFORM _db4ai_test.assert_exception('db4ai.prepare_snapshot(null, ''test@1.0.0'', ''{ AlTeR , Set  nothing  }'')',
        'illegal SET clause in ALTER at: ''Set  nothing''');
    PERFORM _db4ai_test.assert_exception('db4ai.prepare_snapshot(null, ''test@1.0.0'', ''{ inserT ,Set  nothing}'')',
        'illegal SET clause in INSERT at: ''Set  nothing''');
    PERFORM _db4ai_test.assert_exception('db4ai.prepare_snapshot(null, ''test@1.0.0'', ''{ DelETE ,Set  nothing}'')',
        'illegal SET clause in DELETE at: ''Set  nothing''');

    PERFORM _db4ai_test.assert_exception('db4ai.prepare_snapshot(null, ''test@1.0.0'', ''{ AS  alias  }'')',
        'missing UPDATE / DELETE keyword before AS clause in i_commands at: ''AS  alias''');
    PERFORM _db4ai_test.assert_exception('db4ai.prepare_snapshot(null, ''test@1.0.0'', ''{ AlTeR , As  AliaS  }'')',
        'illegal AS clause in ALTER at: ''As  AliaS''');
    PERFORM _db4ai_test.assert_exception('db4ai.prepare_snapshot(null, ''test@1.0.0'', ''{ inserT ,aS  aliAs  }'')',
        'illegal AS clause in INSERT at: ''aS  aliAs''');
    PERFORM _db4ai_test.assert_exception('db4ai.prepare_snapshot(null, ''test@1.0.0'', ''{DeLETe,  AS this , as  thAt }'')',
        'multiple AS clauses in DELETE at: ''as  thAt''');

    PERFORM _db4ai_test.assert_exception('db4ai.prepare_snapshot(null, ''test@1.0.0'', ''{ ( a b) }'')',
        'missing ALTER / INSERT / UPDATE / DELETE keyword before SQL clause: ''( a b)''');
    PERFORM _db4ai_test.assert_exception('db4ai.prepare_snapshot(null, ''test@1.0.0'', ''{ AlTer, Values( a b) }'')',
        'illegal SQL clause in ALTER at: ''Values( a b)''');
    PERFORM _db4ai_test.assert_exception('db4ai.prepare_snapshot(null, ''test@1.0.0'', ''{ uPDaTe, Values( a b) }'')',
        'illegal SQL clause in UPDATE at: ''Values( a b)''');
    PERFORM _db4ai_test.assert_exception('db4ai.prepare_snapshot(null, ''test@1.0.0'', ''{ delETE , Values( a b) }'')',
        'illegal SQL clause in DELETE at: ''Values( a b)''');

    PERFORM _db4ai_test.assert_exception('db4ai.prepare_snapshot(null, ''test@1.0.0'', ''{ alTeR  }'')',
        'missing auxiliary clauses in ALTER');
    PERFORM _db4ai_test.assert_exception('db4ai.prepare_snapshot(null, ''test@1.0.0'', ''{ inSerT  }'')',
        'missing auxiliary clauses in INSERT');
    PERFORM _db4ai_test.assert_exception('db4ai.prepare_snapshot(null, ''test@1.0.0'', ''{ UPdate  }'')',
        'missing auxiliary clauses in UPDATE');
    PERFORM _db4ai_test.assert_exception('db4ai.prepare_snapshot(null, ''test@1.0.0'', ''{ alter , AlTer  }'')',
        'missing auxiliary clauses in ALTER before: ''AlTer''');
    PERFORM _db4ai_test.assert_exception('db4ai.prepare_snapshot(null, ''test@1.0.0'', ''{ inSert , upDate  }'')',
        'missing auxiliary clauses in INSERT before: ''upDate''');
    PERFORM _db4ai_test.assert_exception('db4ai.prepare_snapshot(null, ''test@1.0.0'', ''{ upDatE , DeletE  }'')',
        'missing auxiliary clauses in UPDATE before: ''DeletE''');

    PERFORM _db4ai_test.assert_exception('db4ai.prepare_snapshot(null, ''test@1.0.0'', ''{ "Add column, ADD b"}'')',
        'unable to extract column name in ADD operation: ''Add column, ADD b''');
    PERFORM _db4ai_test.assert_exception('db4ai.prepare_snapshot(null, ''test@1.0.0'', ''{ "DROP ColuMn iF eXistS, ADD b"}'')',
        'unable to extract column name in DROP operation: ''DROP ColuMn iF eXistS, ADD b''');
    PERFORM _db4ai_test.assert_exception('db4ai.prepare_snapshot(null, ''test@1.0.0'', ''{ "Add b, c"}'')',
        'expected ADD or DROP keyword before ''c'' in: ''Add b, c''',
        'currently only ADD and DROP supported');

    PERFORM _db4ai_test.assert_exception(
        'db4ai.prepare_snapshot(null, ''test@1.0.0'', ARRAY['' UPDATE '', '' SET foo'' ,''  WHERE FALSE '', '' DroP "Bb" int'' ])',
        'unable to map field "Bb" to backing table in DROP operation: ''DroP "Bb" int''');
    PERFORM _db4ai_test.assert_exception(
        'db4ai.prepare_snapshot(null, ''test@1.0.0'', ''{ UPDATE , SET foo ,  WHERE FALSE , DroP \"Bb\" int }'')',
        'unable to map field "Bb" to backing table in DROP operation: ''DroP "Bb" int''');
    PERFORM _db4ai_test.assert_exception(
        'db4ai.prepare_snapshot(null, ''test@1.0.0'', ARRAY['' UPDATE '', '' SET foo'' ,''  WHERE FALSE '', '' ADD "s int '' ])',
        'unterminated quoted identifier ''"s int'' at or near: ''ADD "s int''');

    PERFORM _db4ai_test.assert_exception('db4ai.prepare_snapshot(null, ''test@1.0.0'', ''{ INSERT, WHERE FALSE, ADD s int}'')',
        'missing SELECT or VALUES clause in INSERT operation');
    PERFORM _db4ai_test.assert_exception('db4ai.prepare_snapshot(null, ''test@1.0.0'', ''{ UPDATE, WHERE FALSE, ADD s int}'')',
        'missing SET clause in UPDATE operation');


    PERFORM db4ai.create_snapshot(null, 'test', '{SELECT 1 a, FROM DUAL}', 'zzz');

    PERFORM _db4ai_test.assert_exception('db4ai.prepare_snapshot(null, ''test@zzz'', ''{ DELETE, Add b}'')',
        'parent has nonstandard version %. i_vers cannot be null or empty',
        'provide custom version using i_vers parameter for new snapshot');

    PERFORM _db4ai_test.assert_exception('db4ai.prepare_snapshot(null, ''test@1.0.0'', ''{ DELETE, Add b}'',''@'')',
        'illegal i_vers: ''@''');
    PERFORM _db4ai_test.assert_exception('db4ai.prepare_snapshot(null, ''test@1.0.0'', ''{ DELETE, Add b}'',''@@34'')',
        'i_vers may contain only one single, leading ''@'' character',
        'specify snapshot version as [@]x.y.z or [@]label with optional, leading ''@''');
    PERFORM _db4ai_test.assert_exception('db4ai.prepare_snapshot(null, ''test@1.0.0'', ''{ DELETE, Add b}'',''x@34'')',
        'i_vers may contain only one single, leading ''@'' character',
        'specify snapshot version as [@]x.y.z or [@]label with optional, leading ''@''');

    PERFORM _db4ai_test.assert_exception('db4ai.prepare_snapshot(null, ''test@1.0.0'', ''{ DELETE }'',''123456789_123456789_123456789_123456789_123456789_123456789'')',
        'snapshot name too long: ''test@123456789_123456789_123456789_123456789_123456789_123456789''');

    PERFORM db4ai.create_snapshot('_db4ai test', ' test  ', '{SELECT 1 a, FROM DUAL}');

    PERFORM _db4ai_test.assert_equal(
        'db4ai.prepare_snapshot(''_db4ai test'', '' test  @1.0.0'', ''{DELETE}'')',
        '(''_db4ai test''::NAME,'' test  @1.1.0''::NAME)');

    PERFORM _db4ai_test.assert_equal(
        'SELECT (schema, name) FROM '
            'db4ai.prepare_snapshot(null, ''test@1.0.0'', ''{ADD xx int}'')',
        '(''_db4ai_test''::NAME, ''test@2.0.0''::NAME)');

    PERFORM _db4ai_test.assert_exception('db4ai.prepare_snapshot(''_db4ai test'', '' test  @1.0.0'', ''{DELETE}'')',
        'snapshot "_db4ai test"." test  @1.1.0" already exists');
    PERFORM _db4ai_test.assert_exception('db4ai.prepare_snapshot(''_db4ai test'', '' test  @1.0.0'', ''{ DELETE }'',''1.0.0'')',
        'snapshot "_db4ai test"." test  @1.0.0" already exists');

    PERFORM _db4ai_test.assert_exception('db4ai.prepare_snapshot_internal(null, null, null, null, null, null, null, null, null, null, null)',
        'direct call to db4ai.prepare_snapshot_internal(bigint,bigint,bigint,bigint,name,name,text[],text,name,int,text[],name[]) is not allowed',
        'call public interface db4ai.prepare_snapshot instead');

    --PERFORM _db4ai_test.assert_exception('db4ai.prepare_snapshot(null, ''test@2.0.0'', ''{ DELETE, Add abc@d int}'')',
    --    'syntax error at or near "@"');
    PERFORM _db4ai_test.assert_exception('db4ai.prepare_snapshot(null, ''test@2.0.0'', ''{ DELETE, Add \"abc\"@ int}'')',
        'syntax error at or near "@"');
    PERFORM _db4ai_test.assert_exception('db4ai.prepare_snapshot(null, ''test@2.0.0'', ''{ DELETE, Add \"abc\"d int}'')',
        'syntax error at or near "int"');


    SET db4ai_snapshot_mode = 8;
    PERFORM _db4ai_test.assert_exception('db4ai.prepare_snapshot(null, null, null)',
        'invalid snapshot mode: ''8''');

END;
$$;
