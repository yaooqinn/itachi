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

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths, StandardOpenOption}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.ExpressionInfo

object ItachiTestUtils {

  def listExtraFunctionInfos(spark: SparkSession): Seq[ExpressionInfo] = {
    val allExprs = spark.sessionState.catalog.listFunctions("default")
    val extras = allExprs.filter(_._2 == "USER")
    extras.map {
      case (f, _) => spark.sessionState.catalog.lookupFunctionInfo(f)
    }
  }

  def generateFunctionDocument(spark: SparkSession, genFile: String) : Unit = {
    val markdown = Paths.get("docs", "functions", s"$genFile.md")
      .toAbsolutePath

    val writer = Files.newBufferedWriter(
      markdown, StandardCharsets.UTF_8,
      StandardOpenOption.TRUNCATE_EXISTING,
      StandardOpenOption.CREATE)

    def writeWithNewLine(content: String): Unit = {
      writer.write(content)
      writer.newLine()
      writer.flush()
    }

    def writeWith2Line(content: String): Unit = {
      writeWithNewLine(content)
      writer.newLine()
    }

    writeWith2Line("<!-- DO NOT MODIFY THIS FILE DIRECTORY, IT IS AUTO GENERATED -->")
    writeWith2Line(s"# $genFile")

    val infoes = listExtraFunctionInfos(spark)
    infoes.foreach { info =>
      info.getDeprecated
      writeWithNewLine("## " + info.getName)

      writeWithNewLine("- **Usage**")
      writeWithNewLine("```scala")
      writeWithNewLine(info.getUsage)
      writeWithNewLine("```")

      writeWithNewLine("- **Arguments**")
      writeWithNewLine("```scala")
      writeWithNewLine(info.getArguments)
      writeWithNewLine("```")

      writeWithNewLine("- **Examples**")
      writeWithNewLine("```sql")
      writeWithNewLine(info.getExamples)
      writeWithNewLine("```")

      writeWithNewLine("- **Class**")
      writeWithNewLine("```scala")
      writeWithNewLine(info.getClassName)
      writeWithNewLine("```")

      writeWithNewLine("- **Note**")
      writeWithNewLine(info.getNote)

      writeWithNewLine(s"- **Since**")
      writeWithNewLine(info.getSince)

    }
  }

}
