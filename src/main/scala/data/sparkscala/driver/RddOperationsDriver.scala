package data.sparkscala.driver

import data.sparkscala.config.SparkEnvConfig
import data.sparkscala.sparkrdddfoperations.{Operators, RddOperations}
object RddOperationsDriver {

  def main(args: Array[String]): Unit = {

    val env = new SparkEnvConfig()

    val config = env.sparkConfig()
    val sc = env.createsparkContext(config)
    val spark = env.createsparkSession(sc)

    import spark.implicits._

    // Create instance of RDDOperationsDemo
    val demo = new RddOperations(sc)

    // Create RDDs
    demo.createRDDs()

    // Demonstrate RDD operations
    demo.demonstrateRDDOperations()

    // Stop SparkContext
    sc.stop()

  }
  }
