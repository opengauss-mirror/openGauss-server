create schema extra_float_digits;
set search_path = extra_float_digits;

set extra_float_digits to 1;

-- Special values
select 0::float4;
select 'NaN'::float4;
select 'Infinity'::float4;
select '-Infinity'::float4;
select 0::float8;
select 'NaN'::float8;
select 'Infinity'::float8;
select '-Infinity'::float8;

-- Non-scientific notation for float4
select 123456::float4;
select -123456::float4;
select 123456.789::float4;
select -123456.789::float4;
select 0.000123456789::float4;
select -0.000123456789::float4;

-- Scientific notation for float4
select 1234567::float4;
select -1234567::float4;
select 1234567.89::float4;
select -1234567.89::float4;
select 0.0000123456789::float4;
select -0.0000123456789::float4;

-- Non-scientific notation for float8
select 123456789012345::float8;
select -123456789012345::float8;
select 123456789012345.6789::float8;
select -123456789012345.6789::float8;
select 0.0001234567890123456789::float8;
select -0.0001234567890123456789::float8;

-- Scientific notation for float8
select 1234567890123456::float8;
select -1234567890123456::float8;
select 1234567890123456.789::float8;
select -1234567890123456.789::float8;
select 0.00001234567890123456789::float8;
select -0.00001234567890123456789::float8;
