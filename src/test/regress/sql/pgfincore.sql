--
-- test SYSCONF
--
select true from pgsysconf();
select true from pgsysconf_pretty();

--
-- make a temp table to use below
--
CREATE TEMP TABLE test AS SELECT generate_series(1,256) as a;

--
-- this is not perfect testing but it is hard to predict what the OS will do
-- for *sure*
--

--
-- test fadvise_loader
--
select true from pgfadvise_loader('test', 'r', NULL, 0, true, true, B'1010');
select true from pgfadvise_loader('test', 'r', NULL, 0, true, false, B'1010');
select true from pgfadvise_loader('test', 'r', NULL, 0, false, true, B'1010');
select true from pgfadvise_loader('test', 'r', NULL, 0, false, false, B'1010');
-- must not fail on empty databit input
select true from pgfadvise_loader('test', 'r', NULL, 0, false, false, B'');
-- ERROR on NULL databit input
select true from pgfadvise_loader('test', 'r', NULL, 0, false, false, NULL);

--
-- test pgfincore
--
select true from pgfincore('test', true);
select true from pgfincore('test');

--
-- test DONTNEED, WILLNEED
--
select true from pgfadvise_willneed('test');
select true from pgfadvise_dontneed('test');

--
-- test PGFADVISE flags
--
select true from pgfadvise_sequential('test');
select true from pgfadvise_random('test');
select true from pgfadvise_normal('test');

--
-- tests drawers
--
select true from pgfincore('test','main',true);
