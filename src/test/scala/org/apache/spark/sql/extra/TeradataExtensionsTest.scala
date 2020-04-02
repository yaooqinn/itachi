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

import org.apache.spark.sql.hive.test.TestHive
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class TeradataExtensionsTest extends FunSuite with BeforeAndAfterAll {

  private val spark = TestHive.sparkSession.newSession()

  override def beforeAll(): Unit = {
    new TeradataExtensions().apply(spark.extensions)
  }

  override def afterAll(): Unit = {
    spark.reset()
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

}
