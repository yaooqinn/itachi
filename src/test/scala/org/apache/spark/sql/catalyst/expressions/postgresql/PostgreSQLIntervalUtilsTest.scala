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

package org.apache.spark.sql.catalyst.expressions.postgresql

import org.scalatest.FunSuite

import org.apache.spark.sql.catalyst.util.DateTimeConstants._
import org.apache.spark.sql.catalyst.util.IntervalUtils._
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

class PostgreSQLIntervalUtilsTest extends FunSuite {

  implicit def ss(s: String): UTF8String = UTF8String.fromString(s)
  test("justify days") {
    assert(PostgreSQLIntervalUtils.justifyDays(
      safeStringToInterval("1 month 35 day")) ===
      new CalendarInterval(2, 5, 0))
    assert(PostgreSQLIntervalUtils.justifyDays(
      safeStringToInterval("-1 month 35 day")) ===
      new CalendarInterval(0, 5, 0))
    assert(PostgreSQLIntervalUtils.justifyDays(
      safeStringToInterval("1 month -35 day")) ===
      new CalendarInterval(0, -5, 0))
    assert(PostgreSQLIntervalUtils.justifyDays(
      safeStringToInterval("-1 month -35 day")) ===
      new CalendarInterval(-2, -5, 0))
    assert(PostgreSQLIntervalUtils.justifyDays(safeStringToInterval("-1 month 2 day")) ===
      new CalendarInterval(0, -28, 0))
  }

  test("justify hours") {
    assert(PostgreSQLIntervalUtils.justifyHours(safeStringToInterval("29 day 25 hour")) ===
      new CalendarInterval(0, 30, 1 * MICROS_PER_HOUR))
    assert(PostgreSQLIntervalUtils.justifyHours(safeStringToInterval("29 day -25 hour")) ===
      new CalendarInterval(0, 27, 23 * MICROS_PER_HOUR))
    assert(PostgreSQLIntervalUtils.justifyHours(safeStringToInterval("-29 day 25 hour")) ===
      new CalendarInterval(0, -27, -23 * MICROS_PER_HOUR))
    assert(PostgreSQLIntervalUtils.justifyHours(safeStringToInterval("-29 day -25 hour")) ===
      new CalendarInterval(0, -30, -1 * MICROS_PER_HOUR))
  }

  test("justify interval") {
    assert(
      PostgreSQLIntervalUtils.justifyInterval(safeStringToInterval("1 month 29 day 25 hour")) ===
      new CalendarInterval(2, 0, 1 * MICROS_PER_HOUR))
    assert(
      PostgreSQLIntervalUtils.justifyInterval(safeStringToInterval("-1 month 29 day -25 hour")) ===
      new CalendarInterval(0, -2, -1 * MICROS_PER_HOUR))
    assert(
      PostgreSQLIntervalUtils.justifyInterval(safeStringToInterval("1 month -29 day -25 hour")) ===
      new CalendarInterval(0, 0, -1 * MICROS_PER_HOUR))
    assert(
      PostgreSQLIntervalUtils.justifyInterval(safeStringToInterval("-1 month -29 day -25 hour")) ===
      new CalendarInterval(-2, 0, -1 * MICROS_PER_HOUR))
    intercept[ArithmeticException] {
      PostgreSQLIntervalUtils.justifyInterval(new CalendarInterval(2, 0, Long.MaxValue))
    }
  }
}
