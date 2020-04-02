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

import org.apache.spark.unsafe.types.CalendarInterval
import org.apache.spark.sql.catalyst.util.DateTimeConstants._

object PostgreSQLIntervalUtils {

  /**
   * Adjust interval so 30-day time periods are represented as months.
   */
  def justifyDays(interval: CalendarInterval): CalendarInterval = {
    val monthToDays = interval.months * DAYS_PER_MONTH
    val totalDays = monthToDays + interval.days
    val months = Math.toIntExact(totalDays / DAYS_PER_MONTH)
    val days = totalDays % DAYS_PER_MONTH
    new CalendarInterval(months, days.toInt, interval.microseconds)
  }

  /**
   * Adjust interval so 24-hour time periods are represented as days.
   */
  def justifyHours(interval: CalendarInterval): CalendarInterval = {
    val dayToUs = MICROS_PER_DAY * interval.days
    val totalUs = Math.addExact(interval.microseconds, dayToUs)
    val days = totalUs / MICROS_PER_DAY
    val microseconds = totalUs % MICROS_PER_DAY
    new CalendarInterval(interval.months, days.toInt, microseconds)
  }

  /**
   * Adjust interval using justifyHours and justifyDays, with additional sign adjustments.
   */
  def justifyInterval(interval: CalendarInterval): CalendarInterval = {
    val monthToDays = DAYS_PER_MONTH * interval.months
    val dayToUs = Math.multiplyExact(monthToDays + interval.days, MICROS_PER_DAY)
    val totalUs = Math.addExact(interval.microseconds, dayToUs)
    val microseconds = totalUs % MICROS_PER_DAY
    val totalDays = totalUs / MICROS_PER_DAY
    val days = totalDays % DAYS_PER_MONTH
    val months = totalDays / DAYS_PER_MONTH
    new CalendarInterval(months.toInt, days.toInt, microseconds)
  }
}

