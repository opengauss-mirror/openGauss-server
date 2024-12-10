SET search_path TO information_schema;

UPDATE pg_class
set reloptions = (CASE WHEN array_length(array_remove(reloptions, 'segment=on'), 1) = 0
                     then NULL
                     else array_remove(reloptions, 'segment=on')
                     END
                )
WHERE relkind = 'v';

-- update function pg_char_max_length
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
       THEN $2 - 4
       WHEN $1 IN (1560, 1562) /* bit, varbit */
       THEN $2
       ELSE null::integer
  END$$;

RESET search_path;
