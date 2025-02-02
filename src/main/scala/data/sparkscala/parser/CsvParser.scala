package data.sparkscala.parser

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

object CsvParser {

  def parse(spark:SparkSession,file_path:String,delimiter:String,header:Boolean,infer_schema:Boolean):DataFrame = {

    val df = spark.read.format("csv")
    .option("header", header)
    .option("delimiter", delimiter)
    .option("inferSchema", infer_schema)
    .load(file_path)

    df
  }

}
