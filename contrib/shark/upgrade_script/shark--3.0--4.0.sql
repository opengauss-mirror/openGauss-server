SET LOCAL d_format_behavior_compat_options = '';

-- This function uses PostgreSQL array declarations and subscripts. Temporarily
-- remove enable_sbr_identifier while preserving the default collation option;
-- the caller's GUC value is restored automatically after each function call.
ALTER FUNCTION sys.shark_conv_string_to_datetime2(TEXT, TEXT, NUMERIC)
    SET d_format_behavior_compat_options TO 'default_collation';

-- dateadd: remove STRICT so that a null datepart raises an error,
-- while a null number/date still returns null (handled in datefuncs.cpp).
CREATE OR REPLACE FUNCTION sys.dateadd(cstring,integer,date)
RETURNS timestamp without time zone
language c
immutable NOT FENCED NOT SHIPPABLE
AS '$libdir/shark', $function$dateadddate$function$;

CREATE OR REPLACE FUNCTION sys.dateadd(cstring,integer,timestamp without time zone)
RETURNS timestamp without time zone
language c
immutable NOT FENCED NOT SHIPPABLE
AS '$libdir/shark', $function$dateaddtimestamp$function$;

CREATE OR REPLACE FUNCTION sys.dateadd(cstring,integer,timestamp with time zone)
RETURNS timestamp with time zone
language c
immutable NOT FENCED NOT SHIPPABLE
AS '$libdir/shark', $function$dateaddtimestamptz$function$;

CREATE OR REPLACE FUNCTION sys.dateadd(cstring,integer,time without time zone)
RETURNS timestamp without time zone
language c
immutable NOT FENCED NOT SHIPPABLE
AS '$libdir/shark', $function$dateaddtime$function$;

CREATE OR REPLACE FUNCTION sys.dateadd(cstring,integer,time with time zone)
RETURNS timestamp with time zone
language c
immutable NOT FENCED NOT SHIPPABLE
AS '$libdir/shark', $function$dateaddtimetz$function$;