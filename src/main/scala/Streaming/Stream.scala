package Streaming

import org.apache.spark.sql._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

object Stream {
  def main(args: Array[String]): Unit = {
    val ssc: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("SparkStreaming")
      .getOrCreate()

//    val stream_in = "/home/cassiogiehl/code/FutureScala/src/main/scala/Files/stream_in"
    val stream_in = "hdfs://127.0.0.1:9000/streaming/input"
//    val stream_out = "/home/cassiogiehl/code/FutureScala/src/main/scala/Files/stream_out/"
    val stream_out = "hdfs://127.0.0.1:9000/streaming/output/"
//    val check_location = "/home/cassiogiehl/code/FutureScala/src/main/scala/Files/checkpoint_location/"
    val check_location = "hdfs://127.0.0.1:9000/streaming/checkpoint_location/"

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
                    cast(replace(outros, ".000", "") as float) AS quantidade
                  FROM municipios_ibge
                  """)

    val consoleOutput = transformation.writeStream
      .format("parquet")
      .option("path", stream_out)
      .option("truncate", false)
      .option("checkpointLocation", check_location)
      .outputMode(OutputMode.Append())
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()

    consoleOutput.awaitTermination()
  }
}
