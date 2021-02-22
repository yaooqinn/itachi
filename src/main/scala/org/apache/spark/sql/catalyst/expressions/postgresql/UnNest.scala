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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.{FunctionIdentifier, InternalRow}
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, ExpressionInfo, Generator, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData, TypeUtils}
import org.apache.spark.sql.extra.{ExpressionUtils, FunctionDescription}
import org.apache.spark.sql.types.{ArrayType, AtomicType, DataType, NullType, StructType}

@ExpressionDescription(
  usage = "_FUNC_(expr) - Separates the elements of array `expr` into multiple rows recursively.",
  examples = """
    Examples:
      > SELECT _FUNC_(array(10, 20));
       10
       20
      > SELECT _FUNC_(a) FROM VALUES (array(1,2)), (array(3,4)) AS v1(a);
       1
       2
       3
       4
      > SELECT _FUNC_(a) FROM VALUES (array(array(1,2), array(3,4))) AS v1(a);
       1
       2
       3
       4
  """,
  since = "0.1.0")
case class UnNest(child: Expression) extends UnaryExpression with Generator with CodegenFallback {

  override def elementSchema: StructType = {
    new StructType().add(prettyName, getLeafDataType(child.dataType), true)
  }

  override def checkInputDataTypes(): TypeCheckResult = child.dataType match {
    case ArrayType(_: NullType, _) => TypeCheckResult.TypeCheckSuccess
    case _: ArrayType =>
      var currentType = child.dataType
      while(currentType.isInstanceOf[ArrayType]) {
        val arrayType = currentType.asInstanceOf[ArrayType]
        if (arrayType.containsNull && !arrayType.elementType.isInstanceOf[AtomicType]) {
          return TypeCheckResult.TypeCheckFailure("multidimensional arrays must not contains nulls")
        }
        currentType = getElementTypeOrItself(currentType)
      }
      TypeUtils.checkForSameTypeInputExpr(child.children.map(_.dataType), s"function $prettyName")

    case _ =>
      TypeCheckResult.TypeCheckFailure(
        s"input to function unnest should be array type not ${child.dataType.catalogString}")
  }

  @scala.annotation.tailrec
  private def getLeafDataType(typ: DataType): DataType = typ match {
    case ArrayType(et, _) => getLeafDataType(et)
    case at => at
  }

  private def getElementTypeOrItself(typ: DataType): DataType = typ match {
    case ArrayType(et, _) => et
    case _ => typ
  }

  override def eval(input: InternalRow): TraversableOnce[InternalRow] = {
    var inputArray = child.eval(input).asInstanceOf[ArrayData]
    if (inputArray == null) {
      Nil
    } else {
      val rows = ArrayBuffer[InternalRow]()
      var currentType = child.dataType
      while (currentType.isInstanceOf[ArrayType]) {
        val currentElementType = getElementTypeOrItself(currentType)
        if (!currentElementType.isInstanceOf[ArrayType]) {
          inputArray.foreach(currentElementType, (_, e) => rows += InternalRow(e))
        } else {
          val array = inputArray.toObjectArray(currentElementType).flatMap {
            _.asInstanceOf[ArrayData].toObjectArray(getElementTypeOrItself(currentElementType))
          }
          inputArray = new GenericArrayData(array)
        }
        currentType = currentElementType
      }
      rows
    }
  }
}

object UnNest {

  val fd: FunctionDescription = (
    new FunctionIdentifier("unnest"),
    ExpressionUtils.getExpressionInfo(classOf[UnNest], "unnest"),
    (children: Seq[Expression]) => UnNest(children.head))

}
