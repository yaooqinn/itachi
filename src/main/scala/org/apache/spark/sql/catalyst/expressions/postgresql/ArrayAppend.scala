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

import org.apache.spark.sql.catalyst.{FunctionIdentifier, InternalRow}
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, ExpressionDescription, ExpressionInfo, GenArrayData}
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData, TypeUtils}
import org.apache.spark.sql.extra.{ExpressionUtils, FunctionDescription}
import org.apache.spark.sql.types.{ArrayType, AtomicType, DataType, NullType}

@ExpressionDescription(
  usage = """
  _FUNC_(array, element) - Returns an array of appending an element to the end of an array
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(array(1, 2, 3), 3);
       [1,2,3,3]
      > SELECT _FUNC_(array(1, 2, 3), null);
       [1,2,3,null]
      > SELECT _FUNC_(a, e) FROM VALUES (array(1,2), 3), (array(3, 4), null), (null, 5) tbl(a, e);
       [1,2,3]
       [3,4,null]
       [5]
  """,
  since = "0.1.0")
case class ArrayAppend(left: Expression, right: Expression) extends BinaryExpression {

  private lazy val elementType: DataType = left.dataType match {
    case ArrayType(elementType, containsNull) => elementType
    case _ => right.dataType
  }

  override def nullable: Boolean = left.dataType match {
    case  _: ArrayType => left.nullable
    case _ => false
  }

  override def checkInputDataTypes(): TypeCheckResult = left.dataType match {
    case ArrayType(et, _) => right.dataType match {
      case NullType =>
        TypeCheckResult.TypeCheckSuccess
      case _: AtomicType =>
        TypeUtils.checkForSameTypeInputExpr(Seq(et, right.dataType), s"function $prettyName")
      case o => TypeCheckResult.TypeCheckFailure(
        s"function $prettyName not support append ${o.typeName} to array[${et.typeName}]")
    }
    case NullType => right.dataType match {
      case _: AtomicType =>
        TypeCheckResult.TypeCheckSuccess
      case o => TypeCheckResult.TypeCheckFailure(
        s"function $prettyName not support append ${o.typeName} to array")
    }
    case o => TypeCheckResult.TypeCheckFailure(
      s"function $prettyName not support append to ${o.typeName} type")
  }

  override def eval(input: InternalRow): Any = left.dataType match {
    case NullType =>
      val elem = right.eval(input)
      if (elem == null) {
        null
      } else {
        new GenericArrayData(Array(elem))
      }
    case ArrayType(elementType, containsNull) =>
      val array = left.eval(input)
      val elem = right.eval(input)
      if (array == null) {
        if (elem == null) {
          null
        } else {
          new GenericArrayData(Array(elem))
        }
      } else {
        val arrayData = array.asInstanceOf[ArrayData]
        val numElements = arrayData.numElements() + 1
        val finalData = new Array[AnyRef](numElements)
        val srcData = arrayData.toObjectArray(elementType)
        Array.copy(srcData, 0, finalData, 0, srcData.length)
        finalData.update(numElements - 1, elem.asInstanceOf[AnyRef])
        new GenericArrayData(finalData)
      }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = left.dataType match {
    case NullType =>
      val (allocation, assigns, arrayData) =
        GenArrayData.genCodeToCreateArrayData(ctx, right.dataType, Seq(right), prettyName)
      ev.copy(
        code = code"""$allocation$assigns""",
        value = JavaCode.variable(arrayData, dataType),
        isNull = FalseLiteral)
    case ArrayType(et, _) =>
      val leftGen = left.genCode(ctx)
      val rightGen = right.genCode(ctx)
      val inputArray = leftGen.value
      val elementToAppend = rightGen.value
      val newArray = ctx.freshName("newArray")
      val oldArraySize = ctx.freshName("oldArraySize")
      val newArraySize = ctx.freshName("newArraySize")
      val allocation1 =
        CodeGenerator.createArrayData(newArray, et, newArraySize, s"$prettyName failed")

      val allocation2 =
        CodeGenerator.createArrayData(newArray, et, "1", s"$prettyName failed")

      val i = ctx.freshName("i")
      val assignment =
        CodeGenerator.createArrayAssignment(newArray, et, inputArray, i, i, true)

      val setNewValue = CodeGenerator.setArrayElement(
        newArray, et, oldArraySize, elementToAppend, Some(s"${rightGen.isNull}"))

      val resultCode =
        s"""
           |int $oldArraySize = $inputArray.numElements();
           |int $newArraySize = $oldArraySize + 1;
           |$allocation1
           |for (int $i = 0; $i < $inputArray.numElements(); $i ++) {
           |  $assignment
           |}
           |$setNewValue
           |${ev.value} = $newArray;
           |""".stripMargin

      if (nullable) {
        ev.copy(code =
          code"""
          boolean ${ev.isNull} = true;
          ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
          ${leftGen.code}
          ${rightGen.code}
          if (${leftGen.isNull}) {
            if (!${rightGen.isNull}) {
               ${ev.isNull} = false; // resultCode could change nullability.
               int $oldArraySize = 0;
               $allocation2
               $setNewValue
               ${ev.value} = $newArray;
             }
          } else {
            ${ev.isNull} = false; // resultCode could change nullability.
            $resultCode
          }""")
      } else {
        ev.copy(code = code"""
        ${leftGen.code}
        ${rightGen.code}
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        $resultCode""", isNull = FalseLiteral)
      }
  }

  override def dataType: DataType = left.dataType match {
    case NullType => ArrayType(right.dataType, containsNull = false)
    case ArrayType(elementType, containsNull) =>
      ArrayType(elementType, containsNull || right.nullable)
  }

  override def prettyName: String = "array_append"
}

object ArrayAppend {

  val fd: FunctionDescription = (
    new FunctionIdentifier("array_append"),
    ExpressionUtils.getExpressionInfo(classOf[ArrayAppend], "array_append"),
    (children: Seq[Expression]) => ArrayAppend(children.head, children.last))

}