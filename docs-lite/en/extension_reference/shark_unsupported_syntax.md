# Unsupported Syntax

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-30T06:28:58.015Z pushedAt=2026-09-21T08:21:36.627Z -->

Due to certain new features introduced in the shark plugin, some syntax features are disabled. This chapter primarily lists the syntax that is no longer supported by default when the shark plugin is in use.

## Postfix Operators

### Syntax Change Description

- The only built-in postfix operator in openGauss is the factorial `!`. The shark plugin removes the postfix operator syntax, so the factorial operator `!` is no longer supported. Use the factorial function instead for related functionality.

```sql
-- The previously supported syntax will no longer be supported.
select 10!;
-- Available alternative syntax.
select factorial(10);
```

## ROWNUM

### Syntax Change Description

- ROWNUM is no longer used as a pseudocolumn keyword.

```sql
-- ROWNUM is no longer supported as a pseudocolumn keyword.
openGauss=# select * from test1 where ROWNUM < 2;
ERROR:  column "rownum" does not exist
LINE 1: select * from test1 where ROWNUM < 2;
                                  ^

--Supported syntax
--For example, LIMIT can be used as an alternative to the ROWNUM less-than syntax.
openGauss=# select * from test1 limit 1;
 id
----
  1
(1 row)

```