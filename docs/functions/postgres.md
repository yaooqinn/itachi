<!-- DO NOT MODIFY THIS FILE DIRECTORY, IT IS AUTO GENERATED -->

# postgres

## age
- **Usage**
```scala

    age(expr1, expr2) - Subtract arguments, producing a "symbolic" result that uses years and months
    age(expr) - Subtract from current_date (at midnight)
  
```
- **Arguments**
```scala

```
- **Examples**
```sql

    > SELECT age(timestamp '1957-06-13');
     43 years 9 months 27 days
    > SELECT age(timestamp '2001-04-10', timestamp '1957-06-13');
     43 years 9 months 27 days
  
```
- **Class**
```scala
org.apache.spark.sql.catalyst.expressions.postgresql.Age
```
- **Note**

- **Since**
0.1.0
## array_append
- **Usage**
```scala

  array_append(array, element) - Returns an array of appending an element to the end of an array
  
```
- **Arguments**
```scala

```
- **Examples**
```sql

    Examples:
      > SELECT array_append(array(1, 2, 3), 3);
       [1,2,3,3]
      > SELECT array_append(array(1, 2, 3), null);
       [1,2,3,null]
      > SELECT array_append(a, e) FROM VALUES (array(1,2), 3), (array(3, 4), null), (null, 5) tbl(a, e);
       [1,2,3]
       [3,4,null]
       [5]
  
```
- **Class**
```scala
org.apache.spark.sql.catalyst.expressions.postgresql.ArrayAppend
```
- **Note**

- **Since**
0.1.0
## array_cat
- **Usage**
```scala
array_cat(col1, col2, ..., colN) - Returns the concatenation of col1, col2, ..., colN.
```
- **Arguments**
```scala

```
- **Examples**
```sql

    Examples:
      > SELECT array_cat('Spark', 'SQL');
       SparkSQL
      > SELECT array_cat(array(1, 2, 3), array(4, 5), array(6));
       [1,2,3,4,5,6]
  
```
- **Class**
```scala
org.apache.spark.sql.catalyst.expressions.Concat
```
- **Note**

    Concat logic for arrays is available since 2.4.0.
  
- **Since**
1.5.0
## array_length
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
org.apache.spark.sql.catalyst.expressions.postgresql.ArrayLength
```
- **Note**

- **Since**

## justifyDays
- **Usage**
```scala
justifyDays(expr) - Adjust interval so 30-day time periods are represented as months
```
- **Arguments**
```scala

```
- **Examples**
```sql

    Examples:
      > SELECT justifyDays(interval '1 month -59 day 25 hour');
       -29 days 25 hours
  
```
- **Class**
```scala
org.apache.spark.sql.catalyst.expressions.postgresql.JustifyDays
```
- **Note**

- **Since**
0.1.0
## justifyHours
- **Usage**
```scala
justifyHours(expr) - Adjust interval so 30-day time periods are represented as months
```
- **Arguments**
```scala

```
- **Examples**
```sql

    Examples:
      > SELECT justifyHours(interval '1 month -59 day 25 hour');
       -29 days 25 hours
  
```
- **Class**
```scala
org.apache.spark.sql.catalyst.expressions.postgresql.JustifyDays
```
- **Note**

- **Since**
0.1.0
## justifyInterval
- **Usage**
```scala
justifyInterval(expr) - Adjust interval so 30-day time periods are represented as months
```
- **Arguments**
```scala

```
- **Examples**
```sql

    Examples:
      > SELECT justifyInterval(interval '1 month -59 day 25 hour');
       -29 days 25 hours
  
```
- **Class**
```scala
org.apache.spark.sql.catalyst.expressions.postgresql.JustifyDays
```
- **Note**

- **Since**
0.1.0
## scale
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
Scale
```
- **Note**

- **Since**

## split_part
- **Usage**
```scala
split_part(text, delimiter, field) - Split string on delimiter and return the given field (counting from one).
```
- **Arguments**
```scala

```
- **Examples**
```sql

    Examples:
      > SELECT split_part('abc~@~def~@~ghi', '~@~', 2);
       def
  
```
- **Class**
```scala
org.apache.spark.sql.catalyst.expressions.postgresql.SplitPart
```
- **Note**

- **Since**
0.1.0
## string_to_array
- **Usage**
```scala
string_to_array(text, delimiter [, replaced]) - splits string into array elements using supplied delimiter and optional null string
```
- **Arguments**
```scala

```
- **Examples**
```sql

    Examples:
      > SELECT string_to_array('xx~^~yy~^~zz~^~', '~^~', 'yy');
       ["xx",null,"zz",""]
  
```
- **Class**
```scala
org.apache.spark.sql.catalyst.expressions.postgresql.StringToArray
```
- **Note**

- **Since**
0.1.0
## unnest
- **Usage**
```scala
unnest(expr) - Separates the elements of array `expr` into multiple rows recursively.
```
- **Arguments**
```scala

```
- **Examples**
```sql

    Examples:
      > SELECT unnest(array(10, 20));
       10
       20
      > SELECT unnest(a) FROM VALUES (array(1,2)), (array(3,4)) AS v1(a);
       1
       2
       3
       4
      > SELECT unnest(a) FROM VALUES (array(array(1,2), array(3,4))) AS v1(a);
       1
       2
       3
       4
  
```
- **Class**
```scala
org.apache.spark.sql.catalyst.expressions.postgresql.UnNest
```
- **Note**

- **Since**
0.1.0
