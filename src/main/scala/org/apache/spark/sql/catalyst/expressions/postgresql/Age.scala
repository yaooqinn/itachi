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

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, CurrentDate, Expression, ExpressionDescription, ExpressionInfo, ImplicitCastInputTypes}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.extra.{ExpressionUtils, FunctionDescription}
import org.apache.spark.sql.types.{AbstractDataType, CalendarIntervalType, DataType, TimestampType}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(expr1, expr2) - Subtract arguments, producing a "symbolic" result that uses years and months
    _FUNC_(expr) - Subtract from current_date (at midnight)
  """,
  examples = """
    > SELECT _FUNC_(timestamp '1957-06-13');
     43 years 9 months 27 days
    > SELECT _FUNC_(timestamp '2001-04-10', timestamp '1957-06-13');
     43 years 9 months 27 days
  """,
  since = "0.1.0")
// scalastyle:on line.size.limit
case class Age(end: Expression, start: Expression)
  extends BinaryExpression with ImplicitCastInputTypes {
  override def left: Expression = end

  override def right: Expression = start

  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType, TimestampType)

  override def nullSafeEval(e: Any, s: Any): Any = {
    DateTimeUtils.age(e.asInstanceOf[Long], s.asInstanceOf[Long])
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val dtu = DateTimeUtils.getClass.getCanonicalName.stripSuffix("$")
    defineCodeGen(ctx, ev, (ed, st) => s"$dtu.age($ed, $st)")
  }

  override def dataType: DataType = CalendarIntervalType

  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): Expression = {
    copy(end = newLeft, start = newRight)
  }
}

object Age {

  val fd: FunctionDescription = (
    new FunctionIdentifier("age"),
    ExpressionUtils.getExpressionInfo(classOf[Age], "age"),
    (children: Seq[Expression]) => if ( children.size == 1) {
      Age(CurrentDate(), children.head)
    } else {
      Age(children.head, children.last)
    })
}
