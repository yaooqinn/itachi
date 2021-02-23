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

package org.apache.spark.sql.catalyst.expressions.teradata

import java.io.UnsupportedEncodingException
import java.time.DateTimeException

import org.apache.spark.sql.catalyst.{FunctionIdentifier, InternalRow}
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, ExpressionInfo, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator, ExprCode}
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.extra.{ExpressionUtils, FunctionDescription}
import org.apache.spark.sql.types.DataType

// scalastyle:off line.size.limit
// [SPARK-31335][SQL] Add try function support https://github.com/apache/spark/pull/28106
@ExpressionDescription(
  usage = """
    _FUNC_(expr) - Evaluate an expression and handle certain types of runtime exceptions by returning NULL.
    In cases where it is preferable that queries produce NULL instead of failing when corrupt or invalid data is encountered, the TRY function may be useful, especially when ANSI mode is on and the users need null-tolerant on certain columns or outputs.
    AnalysisExceptions will not be handled by this, typically runtime exceptions handled by _FUNC_ function are:

      * ArightmeticException - e.g. division by zero, numeric value out of range,
      * NumberFormatException - e.g. invalid casting,
      * IllegalArgumentException - e.g. invalid datetime pattern, missing format argument for string formatting,
      * DateTimeException - e.g. invalid datetime values
      * UnsupportedEncodingException - e.g. encode or decode string with invalid charset
  """,
  examples = """
      Examples:
      > SELECT _FUNC_(1 / 0);
       NULL
      > SELECT _FUNC_(date_format(timestamp '2019-10-06', 'yyyy-MM-dd uucc'));
       NULL
      > SELECT _FUNC_((5e36BD + 0.1) + 5e36BD);
       NULL
      > SELECT _FUNC_(regexp_extract('1a 2b 14m', '\\d+', 1));
       NULL
      > SELECT _FUNC_(encode('abc', 'utf-88'));
       NULL
  """,
  since = "0.1.0")
// scalastyle:on line.size.limit
case class TryExpression(child: Expression) extends UnaryExpression {

  override def nullable: Boolean = true
  override def dataType: DataType = child.dataType
  override def prettyName: String = "try"

  override def eval(input: InternalRow): Any = {
    try {
      child.eval(input)
    } catch {
      case e if TryExpression.canSuppress(e) => null
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val eval = child.genCode(ctx)
    val javaType = CodeGenerator.javaType(dataType)
    ev.copy(code =
      code"""
        |boolean ${ev.isNull} = false;
        |$javaType ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        |try {
        |  ${eval.code}
        |  ${ev.isNull} = ${eval.isNull};
        |  ${ev.value} = ${eval.value};
        |} catch (java.lang.Exception e) {
        |  if (org.apache.spark.sql.catalyst.expressions.teradata.TryExpression.canSuppress(e)) {
        |    ${ev.isNull} = true;
        |  } else {
        |    throw e;
        |  }
        |}
        |""".stripMargin)
  }
}

object TryExpression {

  /**
   * Certain runtime exceptions that can be suppressed by [[TryExpression]], those not listed here
   * are not handled by try function, e.g. exceptions that related access controls or critical ones
   * like [[InterruptedException]] etc, or superclass like [[RuntimeException]] that can suppress
   * all of its subclasses.
   */
  def canSuppress(e: Throwable): Boolean = e match {
    case _: IllegalArgumentException |
         _: ArithmeticException |
         _: DateTimeException |
         _: NumberFormatException |
         _: UnsupportedEncodingException => true
    case _ => false
  }

  val fd: FunctionDescription = (
    new FunctionIdentifier("try"),
    ExpressionUtils.getExpressionInfo(classOf[TryExpression], "try"),
    (children: Seq[Expression]) => TryExpression(children.head))
}
