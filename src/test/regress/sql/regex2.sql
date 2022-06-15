-- These cases used to give too-many-states failures
select 'x' ~ 'abcd(\m)+xyz';
select 'a' ~ '^abcd*(((((^(a c(e?d)a+|)+|)+|)+|)+|a)+|)';
select 'x' ~ 'a^(^)bcd*xy(((((($a+|)+|)+|)+$|)+|)+|)^$';
select 'x' ~ 'xyz(\Y\Y)+';
select 'x' ~ 'x|(?:\M)+';

-- This generates O(N) states but O(N^2) arcs, so it causes problems
-- if arc count is not constrained
select 'x' ~ repeat('x*y*z*', 1000);