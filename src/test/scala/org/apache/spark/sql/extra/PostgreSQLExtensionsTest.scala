/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.extra

import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.Decimal
import org.apache.spark.unsafe.types.CalendarInterval

class PostgreSQLExtensionsTest extends QueryTest with SharedSparkSession {

  override def beforeAll(): Unit = {
    super.beforeAll()
    new PostgreSQLExtensions().apply(spark.extensions)
  }

  def checkResult(df: DataFrame, expect: DataFrame): Unit = {
    assert(df.collect() === expect.collect())
  }

  def checkAnswer(df: DataFrame, expect: Seq[Row]): Unit = {
    assert(df.collect() === expect)
  }

  test("array_append") {
    val sql = spark.sql _
    checkResult(sql("select array_append(array(1,2), 3)"), sql("select array(1, 2, 3)"))
    checkResult(sql("select array_append(array(1,2), null)"), sql("select array(1, 2, null)"))
    checkResult(sql("select array_append(array('3', '2'), '3')"),
      sql("select array('3', '2', '3')"))
    checkResult(sql("select array_append(array(null), null)"), sql("select array(null, null)"))
    checkResult(sql("select array_append(null, 3)"), sql("select array(3)"))
  }

  test("array_cat") {
    val sql = spark.sql _
    checkResult(sql("select array_cat(array(1,2), array(1,3))"), sql("select array(1, 2, 1, 3)"))
    checkResult(sql("select array_cat(array(1,2), array(null))"), sql("select array(1, 2, null)"))
    checkResult(sql("select array_cat(array('3', '2'), array('3'))"),
      sql("select array('3', '2', '3')"))
  }

  test("array_length") {
    val sql = spark.sql _
    checkResult(sql("select array_length(array(1,2), 0)"), sql("select null"))
    checkResult(sql("select array_length(array(1,2), null)"), sql("select null"))
    checkResult(sql("select array_length(array('3', '2'), 1)"), sql("select 2"))
    checkResult(sql("select array_length(array('3', '2'), 2)"), sql("select null"))
    checkResult(sql("select array_length(array(array(1, 2, 3), array(3, 4, 5)), 2)"),
      sql("select 3"))

    checkResult(sql("select array_append(array(null), null)"), sql("select array(null, null)"))
    checkResult(sql("select array_append(null, 3)"), sql("select array(3)"))
  }

  test("sting split_part") {
    val s = spark
    import s.implicits._
    val df = Seq("abc~@~def~@~ghi").toDF("a")
    checkAnswer(df.selectExpr("split_part(a, '~@~', 2)"), Seq(Row("def")))
    checkAnswer(df.selectExpr("split_part(a, '~@~', -1)"), Seq(Row(null)))
    checkAnswer(df.selectExpr("split_part(a, '~@~', 4)"), Seq(Row(null)))

    checkAnswer(df.selectExpr("split_part(null, '~@~', 2)"), Seq(Row(null)))
    checkAnswer(df.selectExpr("split_part(a, null, 2)"), Seq(Row(null)))
    checkAnswer(df.selectExpr("split_part(a, '~@~', null)"), Seq(Row(null)))

    val df2 = Seq("abc~.~def~@~ghi").toDF("a")
    checkAnswer(df2.selectExpr("split_part(a, '~.~', 2)"), Seq(Row("def~@~ghi")))
    checkAnswer(df2.selectExpr("split_part(a, '', 1)"), Seq(Row("abc~.~def~@~ghi")))
  }

  test("string_to_array function") {
    val s = spark
    import s.implicits._
    val df1 = Seq("xx~^~yy~^~zz~^~").toDF("a")

    checkAnswer(
      df1.selectExpr("string_to_array(a, '~^~', 'yy')"),
      Seq(Row(Seq("xx", null, "zz", "")))
    )
    checkAnswer(
      df1.selectExpr("string_to_array(a, '~^~')"),
      Seq(Row(Seq("xx", "yy", "zz", "")))
    )

    checkAnswer(
      df1.selectExpr("string_to_array(a, '~^~', '.*')"),
      Seq(Row(Seq("xx", "yy", "zz", "")))
    )

    checkAnswer(
      df1.selectExpr("string_to_array(a, null, 'y')"),
      Seq(Row(Seq("x", "x", "~", "^", "~", null, null, "~", "^", "~", "z", "z", "~", "^", "~")))
    )

    val df2 = Seq(null.asInstanceOf[String]).toDF("a")
    checkAnswer(df2.selectExpr("string_to_array(a, ',')"), Seq(Row(null)))

    val df3 = Seq("").toDF("a")
    checkAnswer(df3.selectExpr("string_to_array(a, ',')"), Seq(Row(Seq(""))))
  }

  test("scale") {

    checkAnswer(
      sql("select scale('1.1D')"),
      Seq(Row(1))
    )
  }

  test("age") {
    checkAnswer(
      sql("select age(timestamp '2001-04-10',  timestamp '1957-06-13')"),
      Seq(Row(new CalendarInterval(525, 28, 0)))
    )

    checkAnswer(
      sql(
        """
          |SELECT EXTRACT(YEAR FROM age) year_part,
          |EXTRACT(MONTH FROM age) month_part,
          |EXTRACT(DAY FROM age) day_part,
          |EXTRACT(hour FROM age) hour_part,
          |EXTRACT(minute FROM age) minute_part,
          |EXTRACT(second FROM age) second_part
          |FROM values (age(TIMESTAMP '2021-03-16 10:09:07', TIMESTAMP '2020-01-15 00:00:00')) AS t(age)
          |""".stripMargin),
        Seq(Row(1, 2, 1, 10, 9, Decimal(7000000, 8, 6).toJavaBigDecimal))
    )

    checkAnswer(
      sql("select age(date 'today')"),
      Seq(Row(new CalendarInterval(0, 0, 0)))
    )
  }
}
