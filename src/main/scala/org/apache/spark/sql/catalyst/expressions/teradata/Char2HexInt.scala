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
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, ExpressionDescription, ExpressionInfo, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.extra.FunctionDescription
import org.apache.spark.sql.types.{AbstractDataType, DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String

@ExpressionDescription(
  usage = "_FUNC_(expr) - Returns the hexadecimal representation of the UTF-16BE encoding" +
    " of the string.",
  examples = """
    Examples:
      > SELECT _FUNC_('Spark SQL');
        0053007000610072006B002000530051004C
  """)
case class Char2HexInt(
    child: Expression) extends UnaryExpression with ExpectsInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(StringType)

  override def nullSafeEval(input: Any): Any = {
    Char2HexInt.toHexIntString(input.asInstanceOf[UTF8String].getBytes)
  }
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val hex = Char2HexInt.getClass.getCanonicalName.stripSuffix("$")
    defineCodeGen(ctx, ev, str => s"$hex.toHexIntString($str.getBytes())")
  }

  override def dataType: DataType = StringType

  override def prettyName: String = "CHAR2HEXINT"
}

object Char2HexInt {
  private val hexDigits: Seq[Byte] = "0123456789ABCDEF".map(_.toByte)

  def toHexIntString(bytes: Array[Byte]): UTF8String = {
    val length = bytes.length
    val res = new Array[Byte](length * 4)
    var i = 0
    while(i < length) {
      res(4 * i) = hexDigits.head
      res(4 * i + 1) = hexDigits.head
      res(4 * i + 2) = hexDigits((bytes(i) & 0xF0) >> 4)
      res(4 * i + 3) = hexDigits(bytes(i) & 0x0F)
      i += 1
    }
    UTF8String.fromBytes(res)
  }

  val fd: FunctionDescription = (
    new FunctionIdentifier("char2hexint"),
    new ExpressionInfo(classOf[Char2HexInt].getCanonicalName, "char2hexint"),
    (children: Seq[Expression]) => Char2HexInt(children.head))

}