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
## regr_count
- **Usage**
```scala

    regr_count(expr1, expr2) - Returns the count of all rows in an expression pair. The function eliminates expression pairs where either expression in the pair is NULL. If no rows remain, the function returns 0.
  
```
- **Arguments**
```scala

      expr1	The dependent DOUBLE PRECISION expression
      expr2	The independent DOUBLE PRECISION expression
      
```
- **Examples**
```sql

    > SELECT regr_count(1, 2);
     1
    > SELECT regr_count(1, null);
     0
  
```
- **Class**
```scala
org.apache.spark.sql.catalyst.expressions.ansi.RegrCount
```
- **Note**

- **Since**
0.2.0
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
## stage_attempt_num
- **Usage**
```scala
stage_attempt_num() - Get stage attemptNumber, How many times the stage that this task belongs to has been attempted.
```
- **Arguments**
```scala

```
- **Examples**
```sql

```
- **Class**
```scala
org.apache.spark.sql.catalyst.expressions.debug.StageAttemptNumber
```
- **Note**

- **Since**
0.3.0
## stage_id
- **Usage**
```scala
stage_id() - Get the stage id which the current task belong to
```
- **Arguments**
```scala

```
- **Examples**
```sql

```
- **Class**
```scala
org.apache.spark.sql.catalyst.expressions.debug.StageId
```
- **Note**

- **Since**
0.3.0
## stage_id_with_retry
- **Usage**
```scala
stage_id_with_retry(stageId) - Get task attemptNumber, and will throw FetchFailedException in the `stageId` Stage and make it retry.
```
- **Arguments**
```scala

```
- **Examples**
```sql

```
- **Class**
```scala
org.apache.spark.sql.catalyst.expressions.debug.StageIdWithRetry
```
- **Note**

- **Since**
3.3.0
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
## task_attempt_id
- **Usage**
```scala
task_attempt_id() - Get an ID that is unique to this task attempt within SparkContext
```
- **Arguments**
```scala

```
- **Examples**
```sql

```
- **Class**
```scala
org.apache.spark.sql.catalyst.expressions.debug.TaskAttemptId
```
- **Note**

- **Since**
0.3.0
## task_attempt_num
- **Usage**
```scala
task_attempt_num() - Get task attemptNumber, how many times this task has been attempted
```
- **Arguments**
```scala

```
- **Examples**
```sql

```
- **Class**
```scala
org.apache.spark.sql.catalyst.expressions.debug.TaskAttemptNumber
```
- **Note**

- **Since**
0.3.0
## task_metrics_result_size
- **Usage**
```scala
task_metrics_result_size() - Meaningless
```
- **Arguments**
```scala

```
- **Examples**
```sql

```
- **Class**
```scala
org.apache.spark.sql.catalyst.expressions.debug.TaskMetricsResultSize
```
- **Note**

- **Since**
0.3.0
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
