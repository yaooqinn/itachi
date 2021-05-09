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

package org.apache.spark.sql.catalyst.expressions.ansi

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, ExpressionDescription, If, ImplicitCastInputTypes, IsNull, Literal, Or}
import org.apache.spark.sql.catalyst.expressions.aggregate.DeclarativeAggregate
import org.apache.spark.sql.extra.{ExpressionUtils, FunctionDescription}
import org.apache.spark.sql.types.{AbstractDataType, DataType, DoubleType, LongType}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(expr1, expr2) - Returns the count of all rows in an expression pair. The function eliminates expression pairs where either expression in the pair is NULL. If no rows remain, the function returns 0.
  """,
  arguments =
    """
      expr1	The dependent DOUBLE PRECISION expression
      expr2	The independent DOUBLE PRECISION expression
      """,
  examples = """
    > SELECT _FUNC_(1, 2);
     1
    > SELECT _FUNC_(1, null);
     0
  """,
  since = "0.2.0",
  note = "",
  group = "agg_funcs")
// scalastyle:on line.size.limit
case class RegrCount(y: Expression, x: Expression)
    extends DeclarativeAggregate with ImplicitCastInputTypes {
  override def prettyName: String = "regr_count"
  private lazy val regrCount = AttributeReference(prettyName, LongType, nullable = false)()

  override lazy val initialValues = Seq(Literal(0L))
  override lazy val updateExpressions: Seq[Expression] = {
    val nullableChildren = children.filter(_.nullable)
    if (nullableChildren.isEmpty) {
      Seq(regrCount + 1L)
    } else {
      Seq(If(nullableChildren.map(IsNull).reduce(Or), regrCount, regrCount + 1L))
    }
  }

  override lazy val mergeExpressions = Seq(regrCount.left + regrCount.right)

  override lazy val evaluateExpression = regrCount

  override lazy val aggBufferAttributes: Seq[AttributeReference] = regrCount :: Nil

  override def inputTypes: Seq[AbstractDataType] = Seq(DoubleType, DoubleType)

  override def nullable: Boolean = false

  override def dataType: DataType = LongType

  override def children: Seq[Expression] = Seq(y, x)
}


object RegrCount {

  val fd: FunctionDescription = (
    new FunctionIdentifier("regr_count"),
    ExpressionUtils.getExpressionInfo(classOf[RegrCount], "regr_count"),
    (children: Seq[Expression]) => RegrCount(children.head, children.last))

}
