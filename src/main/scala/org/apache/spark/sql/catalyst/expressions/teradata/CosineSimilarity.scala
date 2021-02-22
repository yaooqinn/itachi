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

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo, GetMapValueUtil}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.util.MapData
import org.apache.spark.sql.extra.{ExpressionUtils, FunctionDescription}
import org.apache.spark.sql.types._

case class CosineSimilarity(
    left: Expression,
    right: Expression) extends GetMapValueUtil {

  override def inputTypes: Seq[AbstractDataType] = {
    Seq(MapType(StringType, DoubleType), MapType(StringType, DoubleType))
  }

  override def nullSafeEval(l: Any, r: Any): Any = {
    val left = l.asInstanceOf[MapData]
    val right = r.asInstanceOf[MapData]
    val leftKeys = left.keyArray()
    val rightKeys = right.keyArray()
    val leftValues = left.valueArray().toDoubleArray()
    val rightValues = right.valueArray().toDoubleArray()

    var dotProduct = 0.0D
    var i = 0
    while (i < leftKeys.numElements()) {
      var j = 0
      var found = false
      while (j < rightKeys.numElements() && !found) {
        if (leftKeys.getUTF8String(i) == rightKeys.getUTF8String(j)) {
          dotProduct += leftValues(i) * rightValues(j)
          found = true
        }
        j += 1
      }
      i += 1
    }

    val d1 = leftValues.map(Math.pow(_, 2)).sum
    val d2 = rightValues.map(Math.pow(_, 2)).sum
    if (d1 <= 0 || d2 <=0) {
      0.0D
    } else {
      dotProduct / (Math.sqrt(d1) * Math.sqrt(d2))
    }
  }
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {

    val leftKeys = ctx.freshName("leftKeys")
    val rightKeys = ctx.freshName("rightKeys")
    val leftValues = ctx.freshName("leftValues")
    val rightValues = ctx.freshName("rightValues")
    val dotProduct = ctx.freshName("dotProduct")
    val d1 = ctx.freshName("d1")
    val d2 = ctx.freshName("d2")
    val i = ctx.freshName("i")
    val j = ctx.freshName("j")
    nullSafeCodeGen(ctx, ev, (l, r) =>
      s"""
         |ArrayData $leftKeys = $l.keyArray();
         |ArrayData $rightKeys = $r.keyArray();
         |double[] $leftValues = $l.valueArray().toDoubleArray();
         |double[] $rightValues = $r.valueArray().toDoubleArray();
         |double $dotProduct = 0;
         |for (int $i = 0; $i < $leftKeys.numElements(); $i++) {
         |  for (int $j= 0; $j < $rightKeys.numElements(); $j++) {
         |    if ($leftKeys.getUTF8String($i) == $rightKeys.getUTF8String($j)) {
         |      $dotProduct += $leftValues[$i] * $rightValues[$j];
         |      $j += $rightKeys.numElements();
         |    }
         |  }
         |}
         |double $d1 = 0.0D;
         |double $d2 = 0.0D;
         |for(final double i: $leftValues) {
         |  $d1 += java.lang.Math.pow(i, 2);
         |}
         |for(final double i: $rightValues) {
         |  $d2 += java.lang.Math.pow(i, 2);
         |}
         |if ($d1 <= 0 || $d2 <=0) {
         |  ${ev.value} = 0.0D;
         |} else {
         |  ${ev.value} = $dotProduct / (java.lang.Math.sqrt($d1) * java.lang.Math.sqrt($d2));
         |}
         |""".stripMargin)
  }

  override def dataType: DataType = DoubleType

  override def prettyName: String = "cosine_similarity"
}

object CosineSimilarity {
  val fd: FunctionDescription = (
    new FunctionIdentifier("cosine_similarity"),
    ExpressionUtils.getExpressionInfo(classOf[CosineSimilarity], "cosine_similarity"),
    (children: Seq[Expression]) => CosineSimilarity(children.head, children.last))

}