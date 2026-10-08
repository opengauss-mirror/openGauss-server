# Constraints

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-30T06:27:49.411Z pushedAt=2026-09-21T03:31:54.258Z -->

A constraint clause is used to declare constraints that newly inserted or updated rows must satisfy for the operation to succeed. If any data behavior violates a constraint, the operation is terminated by the constraint.

Constraints can be specified when a table is created (via the CREATE TABLE statement) or after the table is created (via the ALTER TABLE statement).

Constraints can be column-level or table-level. A column-level constraint applies only to a column, while a table-level constraint is applied to the entire table.

The constraints in the shark plugin in openGauss are as follows:

- This section contains only the syntax newly added by shark. The original openGauss syntax has not been deleted or modified. For the original openGauss syntax, see [Constraints](../sql_reference/constraints.md).
- IDENTITY: Used to create an identity column in a table. It takes effect only in D mode under the shark plugin.

## IDENTITY Constraint<a name="section11621339171820"></a>

IDENTITY constraint syntax: IDENTITY [ (seed , increment) ]

Parameter
seed
The value used for the first row loaded into the table.

increment
The incremental value added to the identity value of the previously loaded row.

Both seed and increment must be specified, or neither. If neither is specified, the default value (1,1) is used.

The IDENTITY attribute can be assigned to a tinyint, smallint, int, bigint, decimal(p, 0), or numeric(p, 0) column. Only one identity column can be created per table.

The behavior of explicitly inserting values into an IDENTITY column is controlled by the GUC parameter IDENTITY_INSERT. The default value OFF means that explicit insertion of IDENTITY column values is not allowed.

```
openGauss=# CREATE TABLE book
(
    bookId int NOT NULL PRIMARY KEY IDENTITY,
    bookname NVARCHAR(50),
    author NVARCHAR(50)
);
NOTICE:  CREATE TABLE will create implicit sequence "book_bookid_seq_identity" for serial column "book.bookid"
NOTICE:  CREATE TABLE / PRIMARY KEY will create implicit index "book_pkey" for table "book"
CREATE TABLE
```

In an INSERT statement, the current value of the IDENTITY column is not updated in the following error scenarios: the table does not exist or there is no permission for the INSERT operation on the table; the values in the VALUES clause or the values in a subquery do not match the types of the inserted columns (such as incompatible types or exceeding type precision); the number of values is not equal to the number of inserted columns; a trigger on the target table is violated or the insert operation is modified by a trigger to return NULL (such as a BEFORE ROW INSERT trigger or an INSTEAD OF ROW INSERT trigger); unsupported syntax exists in the VALUES clause, SELECT clause, or RETURNING clause; or there is a syntax-level error in the INSERT statement. However, the current value of the IDENTITY column is updated when the INSERT operation violates a column constraint, because the insert data is fully prepared before constraint checking and the IDENTITY column has already obtained the next value.

## Related Links<a name="section156744489391"></a>

[Constraints](../sql_reference/constraints.md)