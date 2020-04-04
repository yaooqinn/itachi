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

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, ExpressionInfo, ImplicitCastInputTypes, TernaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.extra.FunctionDescription
import org.apache.spark.sql.types.{AbstractDataType, DataType, IntegerType, StringType}
import org.apache.spark.unsafe.types.UTF8String

@ExpressionDescription(
  usage = "_FUNC_(text, delimiter, field) - Split string on delimiter and return the given field" +
    " (counting from one).",
  examples = """
    Examples:
      > SELECT _FUNC_('abc~@~def~@~ghi', '~@~', 2);
       def
  """,
  since = "3.0.0")
case class SplitPart(text: Expression, delimiter: Expression, field: Expression)
  extends TernaryExpression with ImplicitCastInputTypes with CodegenFallback {

  override def prettyName: String = "split_part"

  override def children: Seq[Expression] = text :: delimiter :: field :: Nil

  override def inputTypes: Seq[AbstractDataType] = StringType :: StringType :: IntegerType :: Nil

  override def dataType: DataType = StringType

  override def nullSafeEval(text: Any, deli: Any, idx: Any): Any = {
    val string = text.asInstanceOf[UTF8String]
    val delimiterLit = Pattern.quote(deli.asInstanceOf[UTF8String].toString)
    val index = idx.asInstanceOf[Int]
    val strings: Array[UTF8String] = if (UTF8String.EMPTY_UTF8.equals(deli)) {
      Array(string)
    } else {
      string.split(UTF8String.fromString(delimiterLit), -1)
    }

    if (index > strings.size || index <= 0) {
      null
    } else {
      strings(index - 1)
    }
  }
}

object SplitPart {

  val fd: FunctionDescription = (
    new FunctionIdentifier("split_part"),
    new ExpressionInfo(classOf[SplitPart].getCanonicalName, "split_part"),
    (children: Seq[Expression]) => SplitPart(children.head, children(1), children.last))

}
