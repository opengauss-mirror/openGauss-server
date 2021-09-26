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
 * 09_SQLTokenizer.sql
 *    DB4AI.Snapshot test case.
 *
 *
 * -------------------------------------------------------------------------
 */

CREATE OR REPLACE FUNCTION _db4ai_test.tokenize(
    IN i_tokens TEXT                         -- input for tokenization
)
RETURNS TEXT[] LANGUAGE plpgsql SECURITY INVOKER SET client_min_messages TO NOTICE
AS $$
DECLARE
    pattern TEXT;
    res_arr TEXT[];

    quoted BOOLEAN := FALSE;  -- inside quoted identifier
    cur_ch VARCHAR;           -- current character in tokenizer
    idx INTEGER := 0;         -- loop counter, cannot use FOR .. iterator
    tokens TEXT := i_tokens || ' ';
BEGIN

-- BEGIN tokenizer code for testing

    pattern := '';

    LOOP
        idx := idx + 1;
        cur_ch := substr(tokens, idx, 1);
        EXIT WHEN cur_ch IS NULL OR cur_ch = '';

        CASE cur_ch
        WHEN '"' THEN
            IF quoted AND substr(tokens, idx + 1, 1) = '"' THEN
                pattern := pattern || '"';
                idx := idx + 1;
            ELSE
                quoted := NOT quoted;
            END IF;
            IF quoted THEN
                CONTINUE;
            END IF;
        WHEN ',' THEN
            IF quoted THEN
                pattern := pattern || cur_ch;
                CONTINUE;
            ELSIF pattern IS NULL OR length(pattern) = 0 THEN
                pattern := ',';
            ELSE
                idx := idx - 1; -- reset on comma for next loop
            END IF;
        WHEN ' ', E'\n', E'\t' THEN
            IF quoted THEN
                pattern := pattern || cur_ch;
                CONTINUE;
            ELSIF pattern IS NULL OR length(pattern) = 0 THEN
                CONTINUE;
            END IF;
        ELSE
            pattern := pattern || CASE WHEN quoted THEN cur_ch ELSE lower(cur_ch) END;
            CONTINUE;
        END CASE;

-- END tokenizer code for testing

        res_arr := res_arr || pattern;
        pattern := '';
    END LOOP;

    IF quoted THEN
        RAISE EXCEPTION 'unterminated quoted identifier at or near: ''%''', i_tokens;
    END IF;

    RETURN res_arr;

END;
$$;

CREATE OR REPLACE FUNCTION _db4ai_test.test(
)
RETURNS VOID LANGUAGE plpgsql SECURITY INVOKER SET client_min_messages TO NOTICE
AS $$
BEGIN

    -- map of legal characters in SQL identifiers
    -- all:         & * @ \ { } ^ : , # $ " = ! / < > ( ) ´ ` - % . + ? ; ' ~ _ | [ ]
    -- plain:                         # $             ( ) ´                   _
    -- quoted:      & * @   { } ^ : ,       = ! / < >       ` - % . + ? ;   ~   | [ ]
    -- escaped:                           "                               '

    PERFORM _db4ai_test.assert_equal('_db4ai_test.tokenize('' What is this'')', '''{what,is,this}''');
    PERFORM _db4ai_test.assert_equal('_db4ai_test.tokenize(''  What   is  this   '')', '''{what,is,this}''');
    PERFORM _db4ai_test.assert_equal('_db4ai_test.tokenize(''  "What" is  this   '')', '''{What,is,this}''');
    PERFORM _db4ai_test.assert_equal('_db4ai_test.tokenize(''  "What " is  this   '')', '''{"What ",is,this}''');
    PERFORM _db4ai_test.assert_equal('_db4ai_test.tokenize(''  "What, " is  this   '')', '''{"What, ",is,this}''');
    PERFORM _db4ai_test.assert_equal('_db4ai_test.tokenize(''  Wh#$at,  i()s, t´h_is '')', '''{wh#$at,",",i()s,",",t´h_is}''');

    PERFORM _db4ai_test.assert_equal('_db4ai_test.tokenize(''  " Wh& * @   { } ^ : ,at  "   ,   " i= ! / < >s "  ,  " th` - % . + ? ;   ~   | [ ]is " '')',
                                                           '''{" Wh& * @   { } ^ : ,at  ", ",", " i= ! / < >s ",","," th` - % . + ? ;   ~   | [ ]is " }''');

    PERFORM _db4ai_test.assert_equal('_db4ai_test.tokenize(''  "W'''' ""ha "" t", ", ,is", ", th""is"   ''''?'''''')',
                                                             '''{"W'''' \"ha \" t",",",", ,is",",",", th\"is",''''?''''}''');

    PERFORM _db4ai_test.assert_equal('_db4ai_test.tokenize(''  "Wh at"is,  "this"? '')', '''{"Wh at",is,",",this,?}''');

    PERFORM _db4ai_test.assert_equal('_db4ai_test.tokenize(''  "Wh ""at"is,  "th""is"? '')', '''{"Wh \"at",is,",","th\"is",?}''');

    PERFORM _db4ai_test.assert_equal('_db4ai_test.tokenize(''& * @ \ ( ) , ? !'')', '''{&,*,@,\\,(,),\,,?,!}''');

    PERFORM _db4ai_test.assert_equal('_db4ai_test.tokenize(''& * @ \   {  } ^ ::  , # $ """" = ! / < > ( ) ´ ` - % . + ? ; '''' ~ _ | [ ]'')',
                                                         '''{&,*,@,\\,\{,\},^,::,\,,#,$,"\"",=,!,/,<,>,(,),´,`,-,%,.,+,?,;,'''',~,_,|,[,]}''');

    PERFORM _db4ai_test.assert_exception('_db4ai_test.tokenize(''What is " this  '')',
                                                                'unterminated quoted identifier at or near: ''What is " this  ''');

END;
$$;
