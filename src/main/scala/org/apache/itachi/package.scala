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

package org.apache

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.ansi.RegrCount
import org.apache.spark.sql.catalyst.expressions.postgresql.{Age, ArrayAppend, ArrayLength, IntervalJustifyLike, Scale, SplitPart, StringToArray, UnNest}
import org.apache.spark.sql.extra.{FunctionAliases, FunctionDescription}

package object itachi {

  private def registerFunction(function: FunctionDescription): Unit = {
    SparkSession.active.sessionState
      .functionRegistry
      .registerFunction(function._1, function._2, function._3)
  }

  def registerPostgresFunctions: Unit = {
    registerFunction(Age.fd)
    registerFunction(ArrayAppend.fd)
    registerFunction(ArrayLength.fd)
    registerFunction(IntervalJustifyLike.justifyDays)
    registerFunction(IntervalJustifyLike.justifyHours)
    registerFunction(IntervalJustifyLike.justifyInterval)

    registerFunction(Scale.fd)
    registerFunction(SplitPart.fd)
    registerFunction(StringToArray.fd)
    registerFunction(UnNest.fd)

    registerFunction(RegrCount.fd)
  }
}
