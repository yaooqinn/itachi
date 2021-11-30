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
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, ExpressionDescription, ExpressionInfo, ImplicitCastInputTypes}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.extra.FunctionDescription
import org.apache.spark.sql.types.{AbstractDataType, ArrayType, DataType, IntegerType}

@ExpressionDescription(
  usage = """
  _FUNC_(anyarray, int) - returns the length of the requested array dimension
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(array(1, 2, 3), 1);
       3
  """,
  since = "0.1.0")
case class ArrayLength(left: Expression, right: Expression)
  extends BinaryExpression with ImplicitCastInputTypes with CodegenFallback {

  override def nullable: Boolean = true

  override def nullSafeEval(array: Any, dimension: Any): Any = {
    val dim = dimension.asInstanceOf[Int]
    if (dim <= 0) {
      null
    } else {
      var arrayData = array.asInstanceOf[ArrayData]
      var currentType = left.dataType
      var i = 1
      while (i < dim) {
        currentType = getCurrentElementType(currentType)
        if (currentType == null) {
          return null
        } else {
          arrayData = arrayData.toArray[ArrayData](currentType).head
        }
        i += 1
      }
      arrayData.numElements()
    }

  }

  private def getCurrentElementType(dt: DataType): DataType = {
    dt match {
      case ArrayType(et, _) => et match {
        case ArrayType(_, _) => et
        case _ => null
      }
      case _ => null
    }
  }

  override def dataType: DataType = IntegerType

  override def inputTypes: Seq[AbstractDataType] = Seq(ArrayType, IntegerType)

  override def prettyName: String = "array_length"

  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): Expression =
    copy(left = newLeft, right = newRight)
}


object ArrayLength {
  val fd: FunctionDescription = (
    new FunctionIdentifier("array_length"),
    new ExpressionInfo(classOf[ArrayLength].getCanonicalName, "array_length"),
    (children: Seq[Expression]) => ArrayLength(children.head, children.last))

}