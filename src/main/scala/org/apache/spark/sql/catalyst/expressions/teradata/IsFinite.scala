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
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes, Predicate, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, FalseLiteral}
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.extra.{ExpressionUtils, FunctionDescription}
import org.apache.spark.sql.types.{AbstractDataType, DoubleType, FloatType, TypeCollection}

case class IsFinite(child: Expression) extends UnaryExpression
  with Predicate with ImplicitCastInputTypes {
  override def nullable: Boolean = false

  override def inputTypes: Seq[AbstractDataType] = Seq(TypeCollection(DoubleType, FloatType))

  override def eval(input: InternalRow): Any = {
    val value = child.eval(input)
    if (value == null) {
      true
    } else {
      child.dataType match {
        case FloatType => java.lang.Float.isFinite(value.asInstanceOf[Float])
        case DoubleType => java.lang.Double.isFinite(value.asInstanceOf[Double])
      }
    }
  }
  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val eval = child.genCode(ctx)
    val isFinite = child.dataType match {
      case FloatType => s"java.lang.Float.isFinite(${eval.value})"
      case DoubleType => s"java.lang.Double.isFinite(${eval.value})"
    }
    ev.copy(code =
      code"""
            |${eval.code}
            |boolean ${ev.value} = false;
            |${ev.value} = ${eval.isNull} || $isFinite;
            |""".stripMargin, isNull = FalseLiteral)
  }

  override def prettyName: String = "is_infinite"
}

object IsFinite {

  val fd: FunctionDescription = (
    new FunctionIdentifier("is_finite"),
    ExpressionUtils.getExpressionInfo(classOf[IsFinite], "is_finite"),
    (children: Seq[Expression]) => IsFinite(children.head))
}
