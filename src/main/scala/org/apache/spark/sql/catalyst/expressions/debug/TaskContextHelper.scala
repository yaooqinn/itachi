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

import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{LeafExpression, Nondeterministic}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator, ExprCode, FalseLiteral}
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper

abstract class TaskContextHelper extends LeafExpression with Nondeterministic {

  protected val ctx: TaskContext = TaskContext.get()

  protected def func: () => Any

  protected def funcName: String

  override protected def initializeInternal(partitionIndex: Int): Unit = {}

  override protected def evalInternal(input: InternalRow): Any = func

  override def nullable: Boolean = false

  // scalastyle:off line.size.limit
  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val className = classOf[TaskContext].getName.stripSuffix("$")
    ev.copy(
      code = code"final ${CodeGenerator.javaType(dataType)} ${ev.value} = $className.get().$funcName();",
      isNull = FalseLiteral)
  }
  // scalastyle:on line.size.limit
}
