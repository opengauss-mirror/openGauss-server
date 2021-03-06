-- test leaky-function protections in selfuncs
-- regress_user1 will own a table and provide a view for it.
grant all on schema public to public;
create user regress_user1 password 'gauss@123';
create user regress_user2 password 'gauss@123';
SET SESSION AUTHORIZATION regress_user1 password 'gauss@123';

CREATE TABLE public.atest12 as SELECT x AS a, 10001 - x AS b FROM generate_series(1,10000) x;
CREATE INDEX ON public.atest12 (a);
CREATE INDEX ON public.atest12 (abs(a));
VACUUM ANALYZE public.atest12;

-- Check if regress_user2 can break security.
SET SESSION AUTHORIZATION regress_user2 password 'gauss@123';

CREATE FUNCTION public.leak20(integer,integer) RETURNS boolean AS $$begin raise notice 'leak % %', $1, $2; return $1 > $2; end$$ LANGUAGE plpgsql immutable;
CREATE OPERATOR >>>> (procedure = public.leak20, leftarg = integer, rightarg = integer,restrict = scalargtsel);

-- This should not show any "leak" notices before failing.
EXPLAIN (COSTS OFF) SELECT * FROM public.atest12 ;
-- This should not show any "leak" notices before failing.(After Patch)
EXPLAIN (COSTS OFF) SELECT * FROM public.atest12 WHERE a >>>> 99;
