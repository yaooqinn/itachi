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

import scala.util.control.NonFatal

import org.apache.spark.sql.catalyst.{FunctionIdentifier, InternalRow}
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, ExpressionInfo, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator, ExprCode}
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.extra.FunctionDescription
import org.apache.spark.sql.types.DataType

// TODO: This Function should be updated to the community approved but not merged status
@ExpressionDescription(
  usage = "_FUNC_(expr) - Evaluate an expression and handle certain types of execution errors by" +
    " returning NULL.\nIn cases where it is preferable that queries produce NULL instead of" +
    " failing when corrupt or invalid data is encountered, the TRY function may be useful," +
    " especially when ANSI mode is on and the users need null-tolerant on certain columns or" +
    " outputs.\nAnalysisExceptions will not handle by this, typically errors handled by TRY" +
    " function are: " +
    """
      * Division by zero,
      * Invalid casting,
      * Numeric value out of range,
      * e.t.c
      """,
  examples = """
      Examples:
      > SELECT _FUNC_(1 / 0);
       NULL
  """,
  since = "3.0.0")
case class Try(child: Expression) extends UnaryExpression {

  override def nullable: Boolean = true

  override def eval(input: InternalRow): Any = {
    try {
      child.eval(input)
    } catch {
      case NonFatal(_) => null
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val eval = child.genCode(ctx)
    val javaType = CodeGenerator.javaType(dataType)
    ev.copy(code =
      code"""
        boolean ${ev.isNull} = false;
        $javaType ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        try {
          ${eval.code}
          ${ev.isNull} = ${eval.isNull};
          ${ev.value} = ${eval.value};
        } catch (java.lang.Exception e) {
          if (scala.util.control.NonFatal.apply(e)) {
            ${ev.isNull} = true;
          } else {
            throw e;
          }
        }
        """)
  }

  override def dataType: DataType = child.dataType

}

object Try {
  val fd: FunctionDescription = (
    new FunctionIdentifier("try"),
    new ExpressionInfo(classOf[Try].getCanonicalName, "try"),
    (children: Seq[Expression]) => Try(children.head))
}
