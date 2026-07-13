# An Error Occurs During Integer Conversion<a name="EN-US_TOPIC_0291615107"></a>

## Symptom<a name="section24920212218"></a>

The following error is reported during integer conversion:

```
Invalid input syntax for integer: "13."
```

## Cause Analysis<a name="section1071410284216"></a>

Some data types cannot be converted to the target data type.

## Procedure<a name="section2142103410214"></a>

Gradually narrow down the range of SQL statements to determine the data types that cannot be converted.
