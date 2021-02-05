# itachi

itachi brings useful functions from modern database management systems to Apache Spark :)

For example, you can import the Postgres extensions and write Spark code that looks just like Postgres.

The functions are implemented as native Spark functions, so they're performant.

In general, only those functions that difficult for the Apache Spark Community to maintain in the master branch will be added to this library.

## Spark SQL Compliance

This is a Spark SQL extension supplying add-on or aliased functions to the Apache Spark SQL builtin standard functions.

The functions in this library take precedence over the native Spark functions in the even of a name conflict.

## Prerequisites

- Apache Spark 3.0.0 and above.

## Quick Start

config your spark applications with `spark.sql.extensions`, e.g. `spark.sql.extensions=org.apache.spark.sql.extra.PostgreSQLExtensions`

- org.apache.spark.sql.extra.PostgreSQLExtensions
- org.apache.spark.sql.extra.TeradataExtensions

## Functions from Teradata/Presto

- char2HexInt
- cosine_similarity
- editdistance
- from_base
- index
- infinity
- is_finite
- is_infinite
- NaN
- try
- to_base


## Functions from PostgreSQL

- age
- array_append
- array_cat
- array_length
- justifyDays
- justifyHours
- justifyInterval
- scale
- split_part
- string_to_array
- unnest


** More popular modern dbms system function can be added with your help **