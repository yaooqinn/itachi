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

import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.expressions.teradata._

class TeradataExtensions extends Extensions {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectFunction(Char2HexInt.fd)
    extensions.injectFunction(CosineSimilarity.fd)
    extensions.injectFunction(FunctionAliases.editDistance)
    extensions.injectFunction(FunctionAliases.from_base)
    extensions.injectFunction(FunctionAliases.index)
    extensions.injectFunction(FunctionAliases.to_base)
    extensions.injectFunction(Infinity.fd)
    extensions.injectFunction(IsFinite.fd)
    extensions.injectFunction(IsInfinite.fd)
    extensions.injectFunction(NaN.fd)
    extensions.injectFunction(TryExpression.fd)
  }
}

