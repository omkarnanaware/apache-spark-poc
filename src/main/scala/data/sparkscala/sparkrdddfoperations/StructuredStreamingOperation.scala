package data.sparkscala.sparkrdddfoperations

import data.sparkscala.config.SparkEnvConfig
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object StructuredStreamingOperation extends App{

  val env = new SparkEnvConfig()
  val conf = env.sparkConfig()
  val sc = env.createsparkContext(conf)
  val spark = env.createsparkSession(sc)

  val schemawithCorruptRec = new StructType()
    .add("column1", StringType, true)
    .add("column2", IntegerType, true)
    .add("_corrupt_record", StringType, true)


  val streamingDf = spark.readStream
    .schema(schemawithCorruptRec)
    .option("header", "true")
    .option("mode", "PERMISSIVE")
    .option("columnNameOfCorruptRecord", "_corrupt_record")
    .csv("C:\\Users\\omkar\\Downloads\\dump")

  streamingDf.cache()

  // Filter out corrupt records in the streaming DataFrame
  val validDf = streamingDf.filter("_corrupt_record IS NULL").drop("_corrupt_record")

  val query = validDf.writeStream
    .outputMode("append")
    .format("console")
    .start()

  query.awaitTermination()


}
