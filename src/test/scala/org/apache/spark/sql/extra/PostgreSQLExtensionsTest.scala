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

import org.scalatest.{BeforeAndAfterAll, FunSuite}

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.hive.test.TestHive

class PostgreSQLExtensionsTest extends FunSuite with BeforeAndAfterAll{

  private val spark = TestHive.sparkSession

  var sql = spark.sql _

  import spark.implicits._

  override def beforeAll(): Unit = {
    new PostgreSQLExtensions().apply(spark.extensions)
  }

  override def afterAll(): Unit = {
    spark.reset()
  }

  def checkResult(df: DataFrame, expect: DataFrame): Unit = {
    assert(df.collect() === expect.collect())
  }

  def checkAnswer(df: DataFrame, expect: Seq[Row]): Unit = {
    assert(df.collect() === expect)
  }

  test("array_append") {
    checkResult(sql("select array_append(array(1,2), 3)"), sql("select array(1, 2, 3)"))
    checkResult(sql("select array_append(array(1,2), null)"), sql("select array(1, 2, null)"))
    checkResult(sql("select array_append(array('3', '2'), '3')"),
      sql("select array('3', '2', '3')"))
    checkResult(sql("select array_append(array(null), null)"), sql("select array(null, null)"))
    checkResult(sql("select array_append(null, 3)"), sql("select array(3)"))
  }

  test("sting split_part") {
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
}
