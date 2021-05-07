package yaooqinn

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.extra.SparkSessionHelper
import org.apache.spark.unsafe.types.CalendarInterval

class ItachiTest extends SparkSessionHelper {

  def checkAnswer(df: DataFrame, expect: Seq[Row]): Unit = {
    assert(df.collect() === expect)
  }

  test("age") {
    yaooqinn.itachi.registerPostgresFunctions
    checkAnswer(
      spark.sql("select age(timestamp '2001-04-10',  timestamp '1957-06-13')"),
      Seq(Row(new CalendarInterval(525, 28, 0)))
    )
  }

}
