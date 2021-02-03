package Streaming

import org.apache.spark.sql.types.{StructField, StructType, StringType}

object Schema {
  val getSchema = StructType(Array(
    StructField("municipio", StringType, true),
    StructField("ibge", StringType, true),
    StructField("latitude", StringType, true),
    StructField("longitude", StringType, true),
    StructField("outros", StringType, true),
  ))
}
