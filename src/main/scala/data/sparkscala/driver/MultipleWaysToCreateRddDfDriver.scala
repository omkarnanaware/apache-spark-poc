package data.sparkscala.driver

import data.sparkscala.config.SparkEnvConfig
import data.sparkscala.sparkrdddfoperations._




object MultipleWaysToCreateRddDfDriver {

  def main(args:Array[String]):Unit = {

    val env = new SparkEnvConfig()

    val config = env.sparkConfig()
    val sc = env.createsparkContext(config)
    val spark = env.createsparkSession(sc)

    import spark.implicits._

    val crDf = Seq.range(1, 11).map(x => (x, 11 - x)).toDF("asc", "desc")

    crDf.show()


    val obj = new MultipleWaysToCreatRddDf(spark)

    obj.creatingdf()


  }

}
