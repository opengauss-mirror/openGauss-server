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
 * 04_SampleSnapshotAPI.sql
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

    PERFORM _db4ai_test.assert_exception('db4ai.sample_snapshot(null,null,null,null)',
        'i_parent cannot be NULL or empty');
    PERFORM _db4ai_test.assert_exception('db4ai.sample_snapshot(null, ''name'',null,null)',
        'i_parent must contain exactly one ''@'' character',
        'reference a snapshot using the format: snapshot_name@version');
    PERFORM _db4ai_test.assert_exception('db4ai.sample_snapshot(null, ''name@org@com'',null,null)',
        'i_parent must contain exactly one ''@'' character',
        'reference a snapshot using the format: snapshot_name@version');
    PERFORM _db4ai_test.assert_exception('db4ai.sample_snapshot(''_db4ai test'', ''name@version'',null,null)',
        'parent snapshot "_db4ai test"."name@version" does not exist');

    PERFORM db4ai.create_snapshot(null, 'test', '{SELECT 1 a, FROM DUAL, DISTRIBUTE BY  RePLICATION}');

    PERFORM _db4ai_test.assert_exception('db4ai.sample_snapshot(null, ''test@1.0.0'',null,null)',
        'i_sample_infixes array malformed',
        'pass sample infixes as NAME[] literal, e.g. ''{_train, _test}''');
    PERFORM _db4ai_test.assert_exception('db4ai.sample_snapshot(null, ''test@1.0.0'',''{}'',null)',
        'i_sample_infixes array malformed',
        'pass sample infixes as NAME[] literal, e.g. ''{_train, _test}''');
    PERFORM _db4ai_test.assert_exception('db4ai.sample_snapshot(null, ''test@1.0.0'',''{{a},{a}}'',null)',
        'i_sample_infixes array malformed',
        'pass sample infixes as NAME[] literal, e.g. ''{_train, _test}''');

    PERFORM _db4ai_test.assert_exception('db4ai.sample_snapshot(null, ''test@1.0.0'',''{a,b}'',null)',
        'i_sample_ratios array malformed',
        'pass sample percentages as NUMBER[] literal, e.g. ''{.8, .2}''');
    PERFORM _db4ai_test.assert_exception('db4ai.sample_snapshot(null, ''test@1.0.0'',''{a,b}'',''{}'')',
        'i_sample_ratios array malformed',
        'pass sample percentages as NUMBER[] literal, e.g. ''{.8, .2}''');
    PERFORM _db4ai_test.assert_exception('db4ai.sample_snapshot(null, ''test@1.0.0'',''{a,b}'',''{{.1},{.2}}'')',
        'i_sample_ratios array malformed',
        'pass sample percentages as NUMBER[] literal, e.g. ''{.8, .2}''');
    PERFORM _db4ai_test.assert_exception('db4ai.sample_snapshot(null, ''test@1.0.0'',''{a,b}'',''{.1}'')',
        'i_sample_infixes and i_sample_ratios array length mismatch');

    PERFORM _db4ai_test.assert_exception('db4ai.sample_snapshot(null, ''test@1.0.0'',''{a,b}'',''{.1,.2}'',''{}'')',
        'i_stratify array malformed',
        'pass stratification field names as NAME[] literal, e.g. ''{color, size}''');
    PERFORM _db4ai_test.assert_exception('db4ai.sample_snapshot(null, ''test@1.0.0'',''{a,b}'',''{.1,.2}'',''{{a},{a}}'')',
        'i_stratify array malformed',
        'pass stratification field names as NAME[] literal, e.g. ''{color, size}''');

    PERFORM _db4ai_test.assert_exception('db4ai.sample_snapshot(null, ''test@1.0.0'',''{a,b}'',''{.1,.2}'',null,''{}'')',
        'i_sample_comments array malformed',
        'pass sample comments as TEXT[] literal, e.g. ''{comment 1, comment 2}''');
    PERFORM _db4ai_test.assert_exception('db4ai.sample_snapshot(null, ''test@1.0.0'',''{a,b}'',''{.1,.2}'',null,''{{a},{a}}'')',
        'i_sample_comments array malformed',
        'pass sample comments as TEXT[] literal, e.g. ''{comment 1, comment 2}''');
    PERFORM _db4ai_test.assert_exception('db4ai.sample_snapshot(null, ''test@1.0.0'',''{a,b}'',''{.1,.2}'',null,''{a,a,a}'')',
        'i_sample_infixes and i_sample_comments array length mismatch');

    PERFORM _db4ai_test.assert_exception('db4ai.sample_snapshot(null, ''test@1.0.0'',''{_a,null}'',''{.1,.2}'')',
        'i_sample_infixes array contains NULL values');
    PERFORM _db4ai_test.assert_exception('db4ai.sample_snapshot(null, ''test@1.0.0'',''{_a,_b}'',''{.1,null}'')',
        'i_sample_ratios array contains NULL values');
    PERFORM _db4ai_test.assert_exception('db4ai.sample_snapshot(null, ''test@1.0.0'',''{_a,123456789_123456789_123456789_123456789_123456789_1234}'',''{.1,.2}'')',
        'sample snapshot name too long: ''test123456789_123456789_123456789_123456789_123456789_1234@1.0.0''');
    PERFORM _db4ai_test.assert_exception('db4ai.sample_snapshot(null, ''test@1.0.0'',''{_a,_b}'',''{2,.2}'')',
        'sample ratio must be between 0 and 1');
    PERFORM _db4ai_test.assert_exception('db4ai.sample_snapshot(null, ''test@1.0.0'',''{_a,_b}'',''{.1,-.2}'')',
        'sample ratio must be between 0 and 1');

    PERFORM db4ai.create_snapshot('_db4ai test', 'test', '{SELECT 1 a, FROM DUAL}');
    PERFORM db4ai.create_snapshot('_db4ai test', 'test_a', '{SELECT 1 a, FROM DUAL}');
    PERFORM _db4ai_test.assert_exception('db4ai.sample_snapshot(''_db4ai test'', ''test@1.0.0'',''{_a,_ b}'',''{.1,-.2}'')',
        'snapshot "_db4ai test"."test_a@1.0.0" already exists');

    PERFORM _db4ai_test.assert_equal(
            'SELECT array_agg(schema || '','' || name) FROM db4ai.sample_snapshot(null, ''test@1.0.0'', ''{_1, " _ 2 "}'', ''{.1, .2}'')',
        '''{"_db4ai_test,test_1@1.0.0","_db4ai_test,test _ 2 @1.0.0"}''');

    PERFORM db4ai.create_snapshot('_db4ai test', ' test  ', '{SELECT 1 \"x <> y\", FROM DUAL}');
    PERFORM _db4ai_test.assert_equal(
        'SELECT array_agg(schema || '','' || name) FROM '
            'db4ai.sample_snapshot(''_db4ai test'', '' test  @1.0.0'', ''{_1, " _ 2 "}'', ''{.1, .2}'')',
        '''{"_db4ai test, test  _1@1.0.0","_db4ai test, test   _ 2 @1.0.0"}''');

    PERFORM _db4ai_test.assert_exception('db4ai.prepare_snapshot_internal(null, null, null, null, null, null, null, null, null, null, null)',
        'direct call to db4ai.prepare_snapshot_internal(bigint,bigint,bigint,bigint,name,name,text[],text,name,int,text[],name[]) is not allowed',
        'call public interface db4ai.prepare_snapshot instead');

    SET db4ai_snapshot_mode = 8;
    PERFORM _db4ai_test.assert_exception('db4ai.sample_snapshot(null, null, null, null)',
        'invalid snapshot mode: ''8''');

END;
$$;
