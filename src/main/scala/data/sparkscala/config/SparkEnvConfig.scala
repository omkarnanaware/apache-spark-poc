package data.sparkscala.config

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

class  SparkEnvConfig {

  def sparkConfig():org.apache.spark.SparkConf = {

    val conf = new org.apache.spark.SparkConf()

    conf.setAppName("POC")
    conf.setMaster("local[*]")
    conf.set("spark.testing.memory", "2147480000")
    conf.set("spark.driver.host", "localhost")

    conf
  }


  def createsparkContext(conf:SparkConf):SparkContext = {

    val sc = new SparkContext(conf)

    sc
  }

  def createsparkSession(sc:SparkContext):SparkSession = {

    val spark = SparkSession.builder.config(sc.getConf).getOrCreate()

    spark


  }


}






