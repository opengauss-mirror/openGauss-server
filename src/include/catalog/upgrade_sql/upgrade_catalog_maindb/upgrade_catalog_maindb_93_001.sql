SET search_path TO information_schema;

CREATE OR REPLACE FUNCTION information_schema._pg_char_max_length(typid oid, typmod int4) RETURNS integer
    LANGUAGE sql
    IMMUTABLE
    NOT FENCED
    RETURNS NULL ON NULL INPUT
    AS
$$SELECT
  CASE WHEN $2 = -1 /* default typmod */
       THEN null::integer
       WHEN $1 IN (1042, 1043, 3969) /* char, varchar ,nvarchar2*/
       THEN CASE WHEN $2 > 1073741828
                 THEN $2 - 1073741828
                 ELSE $2 - 4
END
WHEN $1 IN (1560, 1562) /* bit, varbit */
       THEN $2
       ELSE null::integer
END$$;

RESET search_path;
