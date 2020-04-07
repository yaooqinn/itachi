# Spark SQL Function Extensions

A library that brings excellent and useful functions from various modern database management systems to Apache Spark, maybe few now:).

Functions are roughly classified to different extensions by where they came from, e.g. PostgreSQL.

In general, only those functions that difficult for the Apache Spark Community to maintain in the master branch will be involved here.

## Spark SQL Compliance

This is a Spark SQL extension supplying add-on or aliased functions to the Apache Spark SQL builtin standard functions.

If the functions added in this library is conflicts, the one in this library will take precedence of spark one.

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

- array_append
- array_cat
- justifyDays
- justifyHours
- justifyInterval
- scale
- split_part
- string_to_array
- unnest


** More popular modern dbms system function can be added with your help **