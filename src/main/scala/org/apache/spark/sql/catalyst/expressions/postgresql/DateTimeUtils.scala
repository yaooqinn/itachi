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

import java.time.{Instant, LocalDateTime, ZoneId}
import java.time.temporal.ChronoUnit

import org.apache.spark.sql.catalyst.util.DateTimeConstants._
import org.apache.spark.unsafe.types.CalendarInterval

object DateTimeUtils {

  /**
   * Adopted from Apache Spark
   */
  def microsToInstant(us: Long): Instant = {
    val secs = Math.floorDiv(us, MICROS_PER_SECOND)
    // Unfolded Math.floorMod(us, MICROS_PER_SECOND) to reuse the result of
    // the above calculation of `secs` via `floorDiv`.
    val mos = us - secs * MICROS_PER_SECOND
    Instant.ofEpochSecond(secs, mos * NANOS_PER_MICROS)
  }

  final val zone = ZoneId.systemDefault()

  def age(end: Long, start: Long): CalendarInterval = {
    val endInstant = microsToInstant(end)
    val startInstant = microsToInstant(start)
    val endLocalDateTime = LocalDateTime.ofInstant(endInstant, zone)
    val startLocalDateTime = LocalDateTime.ofInstant(startInstant, zone)
    val endLocalDate = endLocalDateTime.toLocalDate
    val startLocalDate = startLocalDateTime.toLocalDate
    val endLocalTime = endLocalDateTime.toLocalTime
    val startLocalTime = startLocalDateTime.toLocalTime
    val period = startLocalDate.until(endLocalDate)
    val micros = startLocalTime.until(endLocalTime, ChronoUnit.MICROS)
    new CalendarInterval(period.getYears * 12 + period.getMonths, period.getDays, micros)
  }

}
