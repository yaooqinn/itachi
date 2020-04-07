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

package org.apache.spark.sql.extra

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.{Concat, Conv, Expression, ExpressionInfo, Levenshtein, Literal, StringLocate}

object FunctionAliases {

  /**
   * Returns the minimum number of edit operations
   * (insertions, deletions, substitutions and transpositions) required to
   * transform string1 into string2.
   *
   * @note only accepts two string arguments but not ci/cd/cs/ct
   *       for the relative cost of a edit operation.
   */
  val editDistance: FunctionDescription = {
    (new FunctionIdentifier("EDITDISTANCE"),
      new ExpressionInfo(classOf[Levenshtein].getCanonicalName, "EDITDISTANCE"),
      (children: Seq[Expression]) => Levenshtein(children.head, children.last))
  }

  /**
   * Returns the position in string_expression_1 where string_expression_2 starts.
   *
   * @note the argument order is opposite to spark's `position` function
   */
  val index: FunctionDescription = {
    (new FunctionIdentifier("index"),
      new ExpressionInfo(classOf[StringLocate].getCanonicalName, "index"),
      (children: Seq[Expression]) => StringLocate(children(1), children.head, Literal(1)))
  }

  val from_base: FunctionDescription = (
    new FunctionIdentifier("from_base"),
    new ExpressionInfo(classOf[Conv].getCanonicalName, "from_base"),
    (children: Seq[Expression]) => Conv(children.head, children.last, Literal(10)))

  val to_base: FunctionDescription = (
    new FunctionIdentifier("to_base"),
    new ExpressionInfo(classOf[Conv].getCanonicalName, "to_base"),
    (children: Seq[Expression]) => Conv(children.head, Literal(10), children.last))

  // array_cat
  val array_cat: FunctionDescription = (
    new FunctionIdentifier("array_cat"),
    new ExpressionInfo(classOf[Concat].getCanonicalName, "array_cat"),
    (children: Seq[Expression]) => Concat(children))
}
