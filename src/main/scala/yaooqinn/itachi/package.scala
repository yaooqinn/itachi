package yaooqinn

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.postgresql._
import org.apache.spark.sql.extra.FunctionAliases

package object itachi {

  def registerPostgresFunctions: Unit = {
    val spark = SparkSession.getActiveSession.get
    spark.sessionState.functionRegistry.registerFunction(Age.fd._1, Age.fd._2, Age.fd._3)
    spark.sessionState.functionRegistry.registerFunction(FunctionAliases.array_cat._1, FunctionAliases.array_cat._2, FunctionAliases.array_cat._3)
    spark.sessionState.functionRegistry.registerFunction(ArrayAppend.fd._1, ArrayAppend.fd._2, ArrayAppend.fd._3)
    spark.sessionState.functionRegistry.registerFunction(ArrayLength.fd._1, ArrayLength.fd._2, ArrayLength.fd._3)
    spark.sessionState.functionRegistry.registerFunction(IntervalJustifyLike.justifyDays._1, IntervalJustifyLike.justifyDays._2, IntervalJustifyLike.justifyDays._3)
    spark.sessionState.functionRegistry.registerFunction(IntervalJustifyLike.justifyHours._1, IntervalJustifyLike.justifyHours._2, IntervalJustifyLike.justifyHours._3)
    spark.sessionState.functionRegistry.registerFunction(IntervalJustifyLike.justifyInterval._1, IntervalJustifyLike.justifyInterval._2, IntervalJustifyLike.justifyInterval._3)
    spark.sessionState.functionRegistry.registerFunction(Scale.fd._1, Scale.fd._2, Scale.fd._3)
    spark.sessionState.functionRegistry.registerFunction(SplitPart.fd._1, SplitPart.fd._2, SplitPart.fd._3)
    spark.sessionState.functionRegistry.registerFunction(StringToArray.fd._1, StringToArray.fd._2, StringToArray.fd._3)
    spark.sessionState.functionRegistry.registerFunction(UnNest.fd._1, UnNest.fd._2, UnNest.fd._3)
  }

}
