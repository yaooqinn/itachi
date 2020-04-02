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

import scala.util.control.NonFatal

import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, ExpressionDescription, ExpressionInfo, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.postgresql.PostgreSQLIntervalUtils._
import org.apache.spark.sql.catalyst.expressions.teradata.Char2HexInt
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.extra.FunctionDescription
import org.apache.spark.sql.types.{AbstractDataType, CalendarIntervalType, DataType}
import org.apache.spark.unsafe.types.CalendarInterval

abstract class IntervalJustifyLike(
    child: Expression,
    justify: CalendarInterval => CalendarInterval,
    justifyFuncName: String) extends UnaryExpression with ExpectsInputTypes {
  override def inputTypes: Seq[AbstractDataType] = Seq(CalendarIntervalType)

  override def dataType: DataType = CalendarIntervalType

  override def nullSafeEval(input: Any): Any = {
    try {
      justify(input.asInstanceOf[CalendarInterval])
    } catch {
      case NonFatal(_) => null
    }
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, child => {
      val iu = PostgreSQLIntervalUtils.getClass.getCanonicalName.stripSuffix("$")
      s"""
         |try {
         |  ${ev.value} = $iu.$justifyFuncName($child);
         |} catch (java.lang.ArithmeticException e) {
         |  ${ev.isNull} = true;
         |}
         |""".stripMargin
    })
  }

  override def prettyName: String = justifyFuncName
}

@ExpressionDescription(
  usage = "_FUNC_(expr) - Adjust interval so 30-day time periods are represented as months",
  examples = """
    Examples:
      > SELECT _FUNC_(interval '1 month -59 day 25 hour');
       -29 days 25 hours
  """,
  since = "3.0.0")
case class JustifyDays(child: Expression)
  extends IntervalJustifyLike(child, justifyDays, "justifyDays")

@ExpressionDescription(
  usage = "_FUNC_(expr) - Adjust interval so 24-hour time periods are represented as days",
  examples = """
    Examples:
      > SELECT _FUNC_(interval '1 month -59 day 25 hour');
       1 months -57 days -23 hours
  """,
  since = "3.0.0")
case class JustifyHours(child: Expression)
  extends IntervalJustifyLike(child, justifyHours, "justifyHours")

@ExpressionDescription(
  usage = "_FUNC_(expr) - Adjust interval using justifyHours and justifyDays, with additional" +
    " sign adjustments",
  examples = """
    Examples:
      > SELECT _FUNC_(interval '1 month -59 day 25 hour');
       -27 days -23 hours
  """,
  since = "3.0.0")
case class JustifyInterval(child: Expression)
  extends IntervalJustifyLike(child, justifyInterval, "justifyInterval")


object IntervalJustifyLike {

  val justifyDays: FunctionDescription = (
    new FunctionIdentifier("justifyDays"),
    new ExpressionInfo(classOf[JustifyDays].getCanonicalName, "justifyDays"),
    (children: Seq[Expression]) => JustifyDays(children.head))

  val justifyHours: FunctionDescription = (
    new FunctionIdentifier("justifyHours"),
    new ExpressionInfo(classOf[JustifyDays].getCanonicalName, "justifyHours"),
    (children: Seq[Expression]) => JustifyHours(children.head))

  val justifyInterval: FunctionDescription = (
    new FunctionIdentifier("justifyInterval"),
    new ExpressionInfo(classOf[JustifyDays].getCanonicalName, "justifyInterval"),
    (children: Seq[Expression]) => JustifyInterval(children.head))
}