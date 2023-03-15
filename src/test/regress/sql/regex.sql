set enable_bitmapscan = off;
--
-- Regular expression tests
--

-- Don't want to have to double backslashes in regexes
set standard_conforming_strings = on;

-- Test simple quantified backrefs
select 'bbbbb' ~ '^([bc])\1*$' as t;
select 'ccc' ~ '^([bc])\1*$' as t;
select 'xxx' ~ '^([bc])\1*$' as f;
select 'bbc' ~ '^([bc])\1*$' as f;
select 'b' ~ '^([bc])\1*$' as t;

-- Test quantified backref within a larger expression
select 'abc abc abc' ~ '^(\w+)( \1)+$' as t;
select 'abc abd abc' ~ '^(\w+)( \1)+$' as f;
select 'abc abc abd' ~ '^(\w+)( \1)+$' as f;
select 'abc abc abc' ~ '^(.+)( \1)+$' as t;
select 'abc abd abc' ~ '^(.+)( \1)+$' as f;
select 'abc abc abd' ~ '^(.+)( \1)+$' as f;

-- Test some cases that crashed in 9.2beta1 due to pmatch[] array overrun
select substring('asd TO foo' from ' TO (([a-z0-9._]+|"([^"]+|"")+")+)');
select substring('a' from '((a))+');
select substring('a' from '((a)+)');

-- Test conversion of regex patterns to indexable conditions
explain (costs off) select * from pg_proc where proname ~ 'abc';
explain (costs off) select * from pg_proc where proname ~ '^abc';
explain (costs off) select * from pg_proc where proname ~ '^abc$';
explain (costs off) select * from pg_proc where proname ~ '^abcd*e';
explain (costs off) select * from pg_proc where proname ~ '^abc+d';
explain (costs off) select * from pg_proc where proname ~ '^(abc)(def)';
explain (costs off) select * from pg_proc where proname ~ '^(abc)$';
explain (costs off) select * from pg_proc where proname ~ '^(abc)?d';

-- Test for infinite loop in pullback() (CVE-2007-4772)
select 'a' ~ '($|^)*';

-- These cases expose a bug in the original fix for CVE-2007-4772
select 'a' ~ '(^)+^';
select 'a' ~ '$($$)+';

-- More cases of infinite loop in pullback(), not fixed by CVE-2007-4772 fix
select 'a' ~ '($^)+';
select 'a' ~ '(^$)*';
select 'aa bb cc' ~ '(^(?!aa))+';
select 'aa x' ~ '(^(?!aa)(?!bb)(?!cc))+';
select 'bb x' ~ '(^(?!aa)(?!bb)(?!cc))+';
select 'cc x' ~ '(^(?!aa)(?!bb)(?!cc))+';
select 'dd x' ~ '(^(?!aa)(?!bb)(?!cc))+';
select 'x' ~ 'abcd(\m)+xyz';
select 'x' ~ 'a^(^)bcd*xy(((((($a+|)+|)+|)+$|)+|)+|)^$';

-- Test for infinite loop in fixempties() (Tcl bugs 3604074, 3606683)
select 'a' ~ '((((((a)*)*)*)*)*)*';
select 'a' ~ '((((((a+|)+|)+|)+|)+|)+|)';

-- invalid chr code
select 'a' ~ '\x7fffffff';  

-- Test for infinite loop in cfindloop with zero-length possible match
-- but no actual match (can only happen in the presence of backrefs)
select 'a' ~ '$()|^\1';
select 'a' ~ '.. ()|\1';
select 'a' ~ '()*\1';
select 'a' ~ '()+\1';

-- test {0}
select 'xyz' ~ '((.)){0}(\2){0}' as t;

-- test similar with regex
SELECT 'abc' SIMILAR TO '我%(b|d)%' escape '我' AS RESULT;
SELECT '%abc' SIMILAR TO '我%abc' escape '我' AS RESULT;
SELECT 'abc' SIMILAR TO '你%(b|d)%' escape '你' AS RESULT;
SELECT '%abc' SIMILAR TO '你%abc' escape '你' AS RESULT;
SELECT '%abc' SIMILAR TO '\%abc' escape '\' AS RESULT;
