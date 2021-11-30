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

import org.apache.spark.{SparkEnv, TaskContext}

import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.{FunctionIdentifier, InternalRow}
import org.apache.spark.sql.extra.{ExpressionUtils, FunctionDescription}
import org.apache.spark.sql.types.{DataType, IntegerType}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """_FUNC_(stageId) - Get task attemptNumber, and will throw FetchFailedException in the `stageId` Stage and make it retry.""",
  examples = "",
  since = "3.3.0",
  group = "misc_funcs")
// scalastyle:on line.size.limit
case class StageIdWithRetry(child: Expression)
  extends UnaryExpression with CodegenFallback {
  override def foldable: Boolean = false
  override def nullable: Boolean = false

  override def dataType: DataType = IntegerType
  override def eval(input: InternalRow): Any = {
    val id = child.eval(input).asInstanceOf[Int]
    val tc = TaskContext.get()
    if (tc.stageAttemptNumber() == 0 &&
      tc.stageId() == id &&
      tc.attemptNumber() == 0 &&
      tc.partitionId() == 0) {
      val blockManagerId = SparkEnv.get.blockManager.shuffleServerId
      throw new FetchFailedException(
        blockManagerId, 0, 0L, 0, 0, "illusion: don't worry to see this")
    } else {
      TaskContext.get().stageId()
    }
  }

  override def prettyName: String = "stage_id_with_retry"

  override protected def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}


object StageIdWithRetry {
  val fd: FunctionDescription = (
    new FunctionIdentifier("stage_id_with_retry"),
    ExpressionUtils.getExpressionInfo(classOf[StageIdWithRetry], "stage_id_with_retry"),
    (c: Seq[Expression]) => StageIdWithRetry(c.head))
}