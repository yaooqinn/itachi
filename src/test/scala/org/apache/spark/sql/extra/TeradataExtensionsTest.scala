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

import org.apache.spark.SparkException
import org.apache.spark.sql.{AnalysisException, DataFrame, Row}

class TeradataExtensionsTest extends SparkSessionHelper {

  override def beforeAll(): Unit = {
    super.beforeAll()
    new TeradataExtensions().apply(spark.extensions)
  }

  def checkResult(df: DataFrame, expect: DataFrame): Unit = {
    assert(df.collect() === expect.collect())
  }

  def checkAnswer(df: DataFrame, expect: Seq[Row]): Unit = {
    assert(df.collect() === expect)
  }

  test("char2hexint") {
    val res1 = spark.sql("select char2hexint('Spark SQL')")
      .head().getString(0)
    assert(res1 === "0053007000610072006B002000530051004C")
    val res2 = spark.sql("select char2hexint(v) from values('Spark SQL') t(v)")
      .head().getString(0)
    assert(res2 === "0053007000610072006B002000530051004C")
  }


  test("EDITDISTANCE") {
    val res1 = spark.sql("select EDITDISTANCE('Spark SQL', 'Hive QL')")
      .head().getInt(0)
    assert(res1 === 6)
    val res2 = spark.sql("select EDITDISTANCE(a, b) from values('Spark SQL', 'Hive QL') t(a, b)")
      .head().getInt(0)
    assert(res2 === 6)
  }

  test("index") {
    val res1 = spark.sql("select index('foobarbar', 'bar')")
      .head().getInt(0)
    assert(res1 === 4)
    val res2 = spark.sql("select index(b, a) from values('bar', 'foobarbar') t(a, b)")
      .head().getInt(0)
    assert(res2 === 4)
  }

  test("try") {
    val res0 = spark.sql("select try(date_part('year', timestamp '2020-04-01'))").head().getInt(0)
    assert(res0 === 2020)
    // TODO: handle child expression fail in analysis?
    intercept[AnalysisException](spark.sql("select try(date_part('aha', timestamp '2020-04-01'))"))
    spark.sql("set spark.sql.ansi.enabled=true")
    intercept[SparkException](spark.sql("select assert_true(1<0)").collect())

    intercept[SparkException](spark.sql("select try(assert_true(1<0))").collect())

    spark.sql(
      "create table abcde using parquet as select cast(a as string)," +
        " b from values (interval 1 day , 2)," +
        " (interval 2 day, 3), (interval 6 month, 0) t(a, b)")
    assert(spark.sql("select try(cast(a as interval) / b) from abcde where b = 0")
      .head().isNullAt(0))
  }

  test("cosine_similarity") {
    val frame = spark.sql("SELECT cosine_similarity(MAP('a', 1.0), MAP('a', 2.0))")
    checkAnswer(frame, Seq(Row(1.0)))
  }

  test("from_base") {
    val frame = spark.sql("SELECT from_base('10', 2)")
    checkAnswer(frame, Seq(Row("2")))
  }

  test("to_base") {
    val frame = spark.sql("SELECT to_base('10', 2)")
    checkAnswer(frame, Seq(Row("1010")))
  }

  test("infinity") {
    val frame = spark.sql("SELECT infinity()")
    checkAnswer(frame, Seq(Row(Double.PositiveInfinity)))
    val frame2 = spark.sql("SELECT -infinity()")
    checkAnswer(frame2, Seq(Row(Double.NegativeInfinity)))
  }

  test("is_infinite") {
    val frame = spark.sql("SELECT is_infinite(1)")
    checkAnswer(frame, Seq(Row(false)))
    val frame2 = spark.sql("SELECT is_infinite(infinity())")
    checkAnswer(frame2, Seq(Row(true)))
    val frame3 = spark.sql("SELECT is_infinite(-infinity())")
    checkAnswer(frame3, Seq(Row(true)))
  }

  test("is_finite") {
    val frame = spark.sql("SELECT is_finite(1)")
    checkAnswer(frame, Seq(Row(true)))
    val frame2 = spark.sql("SELECT is_finite(infinity())")
    checkAnswer(frame2, Seq(Row(false)))
    val frame3 = spark.sql("SELECT is_finite(-infinity())")
    checkAnswer(frame3, Seq(Row(false)))
    val frame4 = spark.sql("SELECT is_finite(null)")
    checkAnswer(frame4, Seq(Row(true)))
  }

  test("nan") {
    val frame = spark.sql("SELECT nan()")
    checkAnswer(frame, Seq(Row(Double.NaN)))
    val frame2 = spark.sql("SELECT -nan()")
    checkAnswer(frame2, Seq(Row(Double.NaN)))
  }

  test("isnan") {
    val frame = spark.sql("SELECT isnan(1)")
    checkAnswer(frame, Seq(Row(false)))
    val frame2 = spark.sql("SELECT isnan(nan() + 0)")
    checkAnswer(frame2, Seq(Row(true)))
  }

  test("generate function doc") {
    ItachiTestUtils.generateFunctionDocument(spark, "presto")
  }
}
