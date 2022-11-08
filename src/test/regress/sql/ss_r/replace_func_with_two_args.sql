--
-- replace function with two arguments
--

select replace('string', '');
select replace('string', 'i');
select replace('string', 'in');
select replace('string', 'ing');

select replace('', 'ing');
select replace(NULL, 'ing');
select replace('ing', '');
select replace('ing', NULL);
select replace('', '');
select replace(NULL, NULL);

select replace(123, '1');
select replace('123', 1);
select replace(123, 1);

select replace('abc\nabc', '\n');
select replace('abc\nabc', E'\n');
select replace(E'abc\nabc', E'\n');

select replace('~!@#$%^&*()', '!@');

select replace('高斯', '高');