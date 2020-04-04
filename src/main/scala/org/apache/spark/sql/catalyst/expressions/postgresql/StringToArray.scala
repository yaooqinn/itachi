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

import java.util.regex.Pattern

import org.apache.spark.sql.catalyst.{FunctionIdentifier, InternalRow}
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, ExpressionDescription, ExpressionInfo, Literal, TernaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.extra.FunctionDescription
import org.apache.spark.sql.types.{AbstractDataType, ArrayType, DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String

@ExpressionDescription(
  usage = "_FUNC_(text, delimiter [, replaced]) - splits string into array elements using" +
    " supplied delimiter and optional null string",
  examples = """
    Examples:
      > SELECT _FUNC_('xx~^~yy~^~zz~^~', '~^~', 'yy');
       ["xx",null,"zz",""]
  """, since = "3.0.0")
case class StringToArray(text: Expression, delimiter: Expression, replaced: Expression)
  extends TernaryExpression with CodegenFallback with ExpectsInputTypes {

  def this(text: Expression, delimiter: Expression) =
    this(text, delimiter, Literal.create(null, StringType))

  override def prettyName: String = "string_to_array"

  override def children: Seq[Expression] = Seq(text, delimiter, replaced)

  override def inputTypes: Seq[AbstractDataType] = Seq(StringType, StringType, StringType)

  override def dataType: DataType = ArrayType(StringType, containsNull = true)

  override def eval(input: InternalRow): Any = {
    val exprs = children
    val value1 = exprs.head.eval(input)
    if (value1 != null) {
      val tempValue2 = exprs(1).eval(input)
      val value2 = if (tempValue2 == null) UTF8String.EMPTY_UTF8 else tempValue2
      nullSafeEval(value1, value2, exprs(2).eval(input))
    } else {
      null
    }
  }

  override def nullSafeEval(string: Any, deli: Any, replaced: Any): Any = {
    val originalStr = string.asInstanceOf[UTF8String]
    val quotedDelimiter = Pattern.quote(deli.asInstanceOf[UTF8String].toString)
    val strings = if (replaced == null) {
      originalStr.split(UTF8String.fromString(quotedDelimiter), -1)
    } else {
      originalStr.toString.split(quotedDelimiter, -1).map {
        case s if UTF8String.fromString(s).equals(replaced) => null
        case s => UTF8String.fromString(s)
      }
    }
    val inited = if (UTF8String.EMPTY_UTF8.equals(deli)) strings.init else strings
    new GenericArrayData(inited.asInstanceOf[Array[Any]])
  }
}

object StringToArray {
  val fd: FunctionDescription = (
    new FunctionIdentifier("string_to_array"),
    new ExpressionInfo(classOf[StringToArray].getCanonicalName, "string_to_array"),
    (children: Seq[Expression]) => StringToArray(children.head, children(1), children.last))

}