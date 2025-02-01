package data.sparkscala.driver

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
object kafkaDriver {

  def main(args: Array[String]): Unit = {


    System.setProperty ("hadoop.home.dir", "C:/hadoop/" )
   // System.load ("C:/hadoop/bin/hadoop.dll")

    // Step 1: Create SparkSession
    val spark = SparkSession.builder()
      .appName("KafkaSparkConsumer")
      .master("local[*]") // Run locally with all available cores
      .config("spark.driver.memory", "1g") // Allocate 1GB to the driver
      .getOrCreate()

    // Step 2: Read from Kafka topic
    val kafkaStreamDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.56.103:9092") // Kafka Broker
      .option("subscribe", "mytopic") // Kafka topic to subscribe
      .option("startingOffsets", "earliest") // Read from the beginning
      .load()

    // Step 3: Convert the Kafka data from binary to string
    val stringDF = kafkaStreamDF.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    // Step 4: Write the streaming data to console for debugging
    val query = stringDF.writeStream
      .outputMode("append")
      .format("console") // Output to the console
      .option("truncate", "false")
      //.trigger(Trigger.ProcessingTime("10 seconds")) // Trigger every 10 seconds
      .start()

    // Step 5: Await termination

  }

}
