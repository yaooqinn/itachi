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

package org.apache.spark.sql.catalyst.expressions.debug

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription}
import org.apache.spark.sql.extra.{ExpressionUtils, FunctionDescription}
import org.apache.spark.sql.types.{DataType, IntegerType}

@ExpressionDescription(
  usage = """_FUNC_() - Get the stage id which the current task belong to""",
  examples = "",
  since = "0.3.0",
  group = "misc_funcs",
  source = "built-in")
case class StageId() extends TaskContextHelper {
  override def prettyName: String = "stage_id"
  override protected def func: () => Any = () => ctx.stageId()
  override protected def funcName: String = "stageId"
  override def dataType: DataType = IntegerType
}

object StageId {
  val fd: FunctionDescription = (
    new FunctionIdentifier("stage_id"),
    ExpressionUtils.getExpressionInfo(classOf[StageId], "stage_id"),
    (_: Seq[Expression]) => StageId())
}