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

package org.apache.itachi

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.extra.SparkSessionHelper
import org.apache.spark.unsafe.types.CalendarInterval

class ItachiTest extends SparkSessionHelper {

  override def beforeAll(): Unit = {
    super.beforeAll()
    org.apache.itachi.registerPostgresFunctions
    org.apache.itachi.registerTeradataFunctions
  }

  def checkAnswer(df: DataFrame, expect: Seq[Row]): Unit = {
    assert(df.collect() === expect)
  }

  test("age") {
    checkAnswer(
      spark.sql("select age(timestamp '2001-04-10',  timestamp '1957-06-13')"),
      Seq(Row(new CalendarInterval(525, 28, 0)))
    )
  }

  test("regr_count") {
    val query = "select k, count(*), regr_count(v, v2)" +
      " from values(1, 10, null), (2, 10, 11), (2, 20, 22), (2, 25,null), (2, 30, 35) t(k, v, v2)" +
      " group by k"
    checkAnswer(sql(query), Seq(Row(1, 1, 0), Row(2, 4, 3)))
    checkAnswer(sql("SELECT REGR_COUNT(1, 2)"), Seq(Row(1)))
    checkAnswer(sql("SELECT REGR_COUNT(1, null)"), Seq(Row(0)))
  }

  test("cosine_similarity") {
    val frame = spark.sql("SELECT cosine_similarity(MAP('a', 1.0), MAP('a', 2.0))")
    checkAnswer(frame, Seq(Row(1.0)))
  }

}
