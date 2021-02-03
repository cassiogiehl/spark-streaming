package Streaming

import org.apache.spark.sql._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

object Stream {
  def main(args: Array[String]): Unit = {
    val ssc: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("FutureTestSpark")
      .getOrCreate()

    val stream_in = "/home/cassiogiehl/code/FutureScala/src/main/scala/Files/stream_in"
    val stream_out = "/home/cassiogiehl/code/FutureScala/src/main/scala/Files/stream_out"

    val df = ssc.readStream
      .format("csv")
      .option("header", true)
      .option("delimiter",",")
      .option("encoding", "iso-8859-1")
      .schema(Schema.getSchema)
      .load(s"$stream_in/*.csv")

    df.createOrReplaceTempView("municipios_ibge")

    val transformation = ssc.sql(
      s"""SELECT
                    municipio,
                    sum(replace(outros, ".000", "")) AS quantidade
                  FROM municipios_ibge
                  GROUP BY municipio""")

    val consoleOutput = transformation.writeStream
      .format("console")
      .option("truncate", false)
      .outputMode(OutputMode.Complete())
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()

    consoleOutput.awaitTermination()
  }
}
