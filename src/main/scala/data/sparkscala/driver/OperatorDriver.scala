package data.sparkscala.driver

import data.sparkscala.config.SparkEnvConfig
import data.sparkscala.sparkrdddfoperations.Operators
object OperatorDriver {

  def main(args:Array[String]):Unit = {

    val env = new SparkEnvConfig()

    val config = env.sparkConfig()
    val sc = env.createsparkContext(config)
    val spark = env.createsparkSession(sc)

    import spark.implicits._

    // Create instance of SparkOperatorsDemo
    val demo = new Operators(spark)

    // Demonstrate DataFrame operators
    demo.demonstrateOperators()

    // Demonstrate SQL operators
    demo.demonstrateSQLOperators()

    // Stop SparkSession
    spark.stop()


  }


}
