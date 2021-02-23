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

import org.apache.spark.sql.catalyst.{FunctionIdentifier, InternalRow}
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo, ImplicitCastInputTypes, Predicate, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, FalseLiteral}
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.extra.{ExpressionUtils, FunctionDescription}
import org.apache.spark.sql.types.{AbstractDataType, DoubleType, FloatType, TypeCollection}

case class IsInfinite(child: Expression) extends UnaryExpression
  with Predicate with ImplicitCastInputTypes {
  override def nullable: Boolean = false

  override def inputTypes: Seq[AbstractDataType] = Seq(TypeCollection(DoubleType, FloatType))

  override def eval(input: InternalRow): Any = {
    val value = child.eval(input)
    if (value == null) {
      false
    } else {
      child.dataType match {
        case FloatType => value.asInstanceOf[Float].isInfinite
        case DoubleType => value.asInstanceOf[Double].isInfinite
      }
    }
  }
  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val eval = child.genCode(ctx)
    val isInfinite = child.dataType match {
      case FloatType => s"java.lang.Float.isInfinite(${eval.value})"
      case DoubleType => s"java.lang.Double.isInfinite(${eval.value})"
    }
    ev.copy(code =
      code"""
            |${eval.code}
            |boolean ${ev.value} = false;
            |${ev.value} = !${eval.isNull} && $isInfinite;
            |""".stripMargin, isNull = FalseLiteral)
  }

  override def prettyName: String = "is_infinite"
}

object IsInfinite {
  val fd: FunctionDescription = (
    new FunctionIdentifier("is_infinite"),
    ExpressionUtils.getExpressionInfo(classOf[IsInfinite], "is_infinite"),
    (children: Seq[Expression]) => IsInfinite(children.head))
}