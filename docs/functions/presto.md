<!-- DO NOT MODIFY THIS FILE DIRECTORY, IT IS AUTO GENERATED -->

# presto

## char2hexint
- **Usage**
```scala
char2hexint(expr) - Returns the hexadecimal representation of the UTF-16BE encoding of the string.
```
- **Arguments**
```scala

```
- **Examples**
```sql

    Examples:
      > SELECT char2hexint('Spark SQL');
        0053007000610072006B002000530051004C
  
```
- **Class**
```scala
org.apache.spark.sql.catalyst.expressions.teradata.Char2HexInt
```
- **Note**

- **Since**
0.1.0
## cosine_similarity
- **Usage**
```scala
N/A.
```
- **Arguments**
```scala

```
- **Examples**
```sql

```
- **Class**
```scala
CosineSimilarity
```
- **Note**

- **Since**

## EDITDISTANCE
- **Usage**
```scala
EDITDISTANCE(str1, str2) - Returns the Levenshtein distance between the two given strings.
```
- **Arguments**
```scala

```
- **Examples**
```sql

    Examples:
      > SELECT EDITDISTANCE('kitten', 'sitting');
       3
  
```
- **Class**
```scala
org.apache.spark.sql.catalyst.expressions.Levenshtein
```
- **Note**

- **Since**
1.5.0
## from_base
- **Usage**
```scala
from_base(num, from_base, to_base) - Convert `num` from `from_base` to `to_base`.
```
- **Arguments**
```scala

```
- **Examples**
```sql

    Examples:
      > SELECT from_base('100', 2, 10);
       4
      > SELECT from_base(-10, 16, -10);
       -16
  
```
- **Class**
```scala
org.apache.spark.sql.catalyst.expressions.Conv
```
- **Note**

- **Since**
1.5.0
## index
- **Usage**
```scala

    index(substr, str[, pos]) - Returns the position of the first occurrence of `substr` in `str` after position `pos`.
      The given `pos` and return value are 1-based.
  
```
- **Arguments**
```scala

```
- **Examples**
```sql

    Examples:
      > SELECT index('bar', 'foobarbar');
       4
      > SELECT index('bar', 'foobarbar', 5);
       7
      > SELECT POSITION('bar' IN 'foobarbar');
       4
  
```
- **Class**
```scala
org.apache.spark.sql.catalyst.expressions.StringLocate
```
- **Note**

- **Since**
1.5.0
## infinity
- **Usage**
```scala
N/A.
```
- **Arguments**
```scala

```
- **Examples**
```sql

```
- **Class**
```scala
Infinity
```
- **Note**

- **Since**

## is_finite
- **Usage**
```scala
N/A.
```
- **Arguments**
```scala

```
- **Examples**
```sql

```
- **Class**
```scala
IsFinite
```
- **Note**

- **Since**

## is_infinite
- **Usage**
```scala
N/A.
```
- **Arguments**
```scala

```
- **Examples**
```sql

```
- **Class**
```scala
IsInfinite
```
- **Note**

- **Since**

## nan
- **Usage**
```scala
N/A.
```
- **Arguments**
```scala

```
- **Examples**
```sql

```
- **Class**
```scala
NaN
```
- **Note**

- **Since**

## to_base
- **Usage**
```scala
to_base(num, from_base, to_base) - Convert `num` from `from_base` to `to_base`.
```
- **Arguments**
```scala

```
- **Examples**
```sql

    Examples:
      > SELECT to_base('100', 2, 10);
       4
      > SELECT to_base(-10, 16, -10);
       -16
  
```
- **Class**
```scala
org.apache.spark.sql.catalyst.expressions.Conv
```
- **Note**

- **Since**
1.5.0
## try
- **Usage**
```scala

    try(expr) - Evaluate an expression and handle certain types of runtime exceptions by returning NULL.
    In cases where it is preferable that queries produce NULL instead of failing when corrupt or invalid data is encountered, the TRY function may be useful, especially when ANSI mode is on and the users need null-tolerant on certain columns or outputs.
    AnalysisExceptions will not be handled by this, typically runtime exceptions handled by try function are:

      * ArightmeticException - e.g. division by zero, numeric value out of range,
      * NumberFormatException - e.g. invalid casting,
      * IllegalArgumentException - e.g. invalid datetime pattern, missing format argument for string formatting,
      * DateTimeException - e.g. invalid datetime values
      * UnsupportedEncodingException - e.g. encode or decode string with invalid charset
  
```
- **Arguments**
```scala

```
- **Examples**
```sql

      Examples:
      > SELECT try(1 / 0);
       NULL
      > SELECT try(date_format(timestamp '2019-10-06', 'yyyy-MM-dd uucc'));
       NULL
      > SELECT try((5e36BD + 0.1) + 5e36BD);
       NULL
      > SELECT try(regexp_extract('1a 2b 14m', '\\d+', 1));
       NULL
      > SELECT try(encode('abc', 'utf-88'));
       NULL
  
```
- **Class**
```scala
org.apache.spark.sql.catalyst.expressions.teradata.TryExpression
```
- **Note**

- **Since**
0.1.0
