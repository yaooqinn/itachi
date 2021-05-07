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

package org.apache.itachi.postgres

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction
import org.apache.spark.sql.catalyst.expressions.ansi.RegrCount

// scalastyle:off
object functions {
// scalastyle:on

  private def withAggregateFunction(
      func: AggregateFunction,
      isDistinct: Boolean = false): Column = {
    new Column(func.toAggregateExpression(isDistinct))
  }

  /**
   * Returns the count of all rows in an expression pair.
   * The function eliminates expression pairs where either expression in the pair is NULL.
   * If no rows remain, the function returns 0.
   *
   * @group agg_funcs
   * @since 0.2.0
   */
  def regr_count(y: Column, x: Column): Column = withAggregateFunction {
    RegrCount(y.expr, x.expr)
  }

  /**
   * Returns the count of all rows in an expression pair.
   * The function eliminates expression pairs where either expression in the pair is NULL.
   * If no rows remain, the function returns 0.
   *
   * @group agg_funcs
   * @since 0.2.0
   */
  def regr_count(y: String, x: String): Column = {
    regr_count(new Column(y), new Column(x))
  }
}
