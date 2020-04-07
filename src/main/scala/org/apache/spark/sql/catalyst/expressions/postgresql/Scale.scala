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

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo, ImplicitCastInputTypes, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.extra.FunctionDescription
import org.apache.spark.sql.types._

case class Scale(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {
  override def dataType: DataType = IntegerType

  override def nullSafeEval(input: Any): Any = {
    child.dataType match {
      case _: DecimalType => input.asInstanceOf[Decimal].scale
      case _: IntegralType => 0
      case _ =>
        input.asInstanceOf[Double] match {
          case Double.NaN => null
          case a if a.isInfinite => null
          case _ => input.toString.drop(input.toString.indexOf('.')).length - 1
        }
    }
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, c =>
      child.dataType match {
        case _: DecimalType => s"${ev.value} = $c.scale();"
        case _: IntegralType => s"${ev.value} = 0;"
        case _ =>
          val sep = "\\."
          s"""
             | if (Double.isNaN($c) || Double.isInfinite($c)) {
             |   ${ev.isNull} = true;
             | } else{
             |   String tmp = Double.toString($c);
             |   int begin = tmp.indexOf((int) '.');
             |   ${ev.value} = tmp.substring(begin + 1).length();
             | }
             |""".stripMargin
      })
  }

  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(DoubleType, DecimalType, IntegralType))
}

object Scale {
  val fd: FunctionDescription = (
    new FunctionIdentifier("scale"),
    new ExpressionInfo(classOf[Scale].getCanonicalName, "scale"),
    (children: Seq[Expression]) => Scale(children.head))

}
