itachi
======

itachi brings useful functions from modern database management systems to Apache Spark :)

For example, you can import the Postgres extensions and write Spark code that looks just like Postgres.

The functions are implemented as native Spark functions, so they're performant.

In general, only those functions that difficult for the Apache Spark Community to maintain in the master branch will be added to this library.

Installation
------------

Fetch the JAR file from Maven.

    libraryDependencies += "com.github.yaooqinn" %% "itachi" % "0.1.0"

Here's `the Maven link <https://repo1.maven.org/maven2/com/github/yaooqinn/itachi_2.12/>`_ where the JAR files are stored.

itachi requires Spark 3+.

Simple function registration
--------------

Access the Postgres / Teradata functions with these commands:::

    org.apache.itachi.registerPostgresFunctions
    org.apache.itachi.registerTeradataFunctions

Simple example
--------------

Suppose you have the following data table and would like to join the two arrays, with the familiar `array_cat <https://w3resource.com/PostgreSQL/postgresql_array_cat-function.php>`_ function from Postgres.::

    +------+------+
    |  arr1|  arr2|
    +------+------+
    |[1, 2]|    []|
    |[1, 2]|[1, 3]|
    +------+------+

Concatenate the two arrays:::

    spark
      .sql("select array_cat(arr1, arr2) as both_arrays from some_data")
      .show()

    +------------+
    | both_arrays|
    +------------+
    |      [1, 2]|
    |[1, 2, 1, 3]|
    +------------+

itachi lets you write Spark SQL code that looks just like Postgres SQL!

Spark SQL extensions installation
--------------

Config your spark applications with `spark.sql.extensions`, e.g. `spark.sql.extensions=org.apache.spark.sql.extra.PostgreSQLExtensions`

- org.apache.spark.sql.extra.PostgreSQLExtensions
- org.apache.spark.sql.extra.TeradataExtensions

Databricks Installation
--------------

Create an `init script <https://docs.databricks.com/clusters/init-scripts.html>`_ in DBFS:::

    dbutils.fs.mkdirs("dbfs:/databricks/scripts/")

    dbutils.fs.put("/databricks/scripts/itachi-install.sh","""
    #!/bin/bash
    wget --quiet -O /mnt/driver-daemon/jars/itachi_2.12-0.1.0.jar https://repo1.maven.org/maven2/com/github/yaooqinn/itachi_2.12/0.1.0/itachi_2.12-0.1.0.jar""", true)

Before starting the cluster, set the Spark Config:::

    spark.sql.extensions org.apache.spark.sql.extra.PostgreSQLExtensions

Also set the DBFS file path before starting the cluster:::

    dbfs:/databricks/scripts/itachi-install.sh

You can now attach a notebook to the cluster using Postgres SQL syntax.

Spark SQL Compliance
--------------------

This is a Spark SQL extension supplying add-on or aliased functions to the Apache Spark SQL builtin standard functions.

The functions in this library take precedence over the native Spark functions in the even of a name conflict.

Contributing
------------

**More popular modern dbms system function can be added with your help**