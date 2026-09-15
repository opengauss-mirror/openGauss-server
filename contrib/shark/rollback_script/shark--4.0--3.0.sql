SET LOCAL d_format_behavior_compat_options = '';

-- rollback: restore STRICT on dateadd functions (null datepart returns null)
CREATE OR REPLACE FUNCTION sys.dateadd(cstring,integer,date)
RETURNS timestamp without time zone
language c
immutable strict NOT FENCED NOT SHIPPABLE
AS '$libdir/shark', $function$dateadddate$function$;

CREATE OR REPLACE FUNCTION sys.dateadd(cstring,integer,timestamp without time zone)
RETURNS timestamp without time zone
language c
immutable strict NOT FENCED NOT SHIPPABLE
AS '$libdir/shark', $function$dateaddtimestamp$function$;

CREATE OR REPLACE FUNCTION sys.dateadd(cstring,integer,timestamp with time zone)
RETURNS timestamp with time zone
language c
immutable strict NOT FENCED NOT SHIPPABLE
AS '$libdir/shark', $function$dateaddtimestamptz$function$;

CREATE OR REPLACE FUNCTION sys.dateadd(cstring,integer,time without time zone)
RETURNS timestamp without time zone
language c
immutable strict NOT FENCED NOT SHIPPABLE
AS '$libdir/shark', $function$dateaddtime$function$;

CREATE OR REPLACE FUNCTION sys.dateadd(cstring,integer,time with time zone)
RETURNS timestamp with time zone
language c
immutable strict NOT FENCED NOT SHIPPABLE
AS '$libdir/shark', $function$dateaddtimetz$function$;

ALTER FUNCTION sys.shark_conv_string_to_datetime2(TEXT, TEXT, NUMERIC)
    RESET d_format_behavior_compat_options;
