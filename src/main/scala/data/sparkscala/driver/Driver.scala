package data.sparkscala.driver

import org.apache.arrow.vector.types.pojo.ArrowType.Struct
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext.getOrCreate
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, FloatType, IntegerType, MapType, StringType, StructField, StructType}

object Driver {

  def main(args:Array[String]):Unit = {

    System.setProperty("hadoop.home.dir","C:\\winutils")

    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Spark Pi").set("spark.driver.host", "localhost")

    conf.set("spark.testing.memory", "2147480000")

    val sc = new SparkContext(conf)

    val spark = SparkSession.builder()
      .getOrCreate()

    import spark.implicits._

    println("Driver Memory: " + spark.conf.get("spark.driver.memory", "not set"))
    println("Spark Master: " + spark.conf.get("spark.master"))

    // Simple Spark operation
    val data = spark.range(100)
    data.show()

    val data1 = Seq(("Omkar","Nanaware",1,1.4f)).toDF("name ","sr","id","id1")


    data1.show()

    data1.printSchema()

    val crDf = Seq.range(1,11).map(x => (x,11-x)).toDF("asc","desc")

    crDf.show()


    //Created from Rdd

    //Control over Schema

    val initRdd = sc.parallelize(Seq(Row(1,"Omkar","Nanaware",64000.5f),Row(2,"Omkar","Nanaware",96000.5f)))

    val schema = "`id` int, `Name` string, `Surname` string, `Value` float"

    val structSchema = StructType.fromDDL(schema)

    val manualSchema = StructType(Array(
      StructField("id",IntegerType,false),
      StructField("Name",StringType,false),
      StructField("Surname",StringType,false),
      StructField("Value",FloatType,false),

    ))

    val df = spark.createDataFrame(initRdd,manualSchema)

    df.show()

    df.printSchema()

    //Need to Import the spark.implicits._
    //No control over schema
    //For Testing purposes

    import spark.implicits._

    val fromSeq = Seq((1,"Omkar","Nanaware",64000.5f),(2,"Omkar","Nanaware",96000.5f)).toDF("id","Name","Surname","Value")

    fromSeq.show()

    fromSeq.printSchema()


    val arrayStructureData = Seq(
      Row(Row("James", "", "Smith"), List("Cricket", "Movies"), Map("hair" -> "black", "eye" -> "brown")),
      Row(Row("Michael", "Rose", ""), List("Tennis"), Map("hair" -> "brown", "eye" -> "black")),
      Row(Row("Robert", "", "Williams"), List("Cooking", "Football"), Map("hair" -> "red", "eye" -> "gray")),
      Row(Row("Maria", "Anne", "Jones"), null, Map("hair" -> "blond", "eye" -> "red")),
      Row(Row("Jen", "Mary", "Brown"), List("Blogging"), Map("white" -> "black", "eye" -> "black"))
    )

    val arrayStructureSchema = new StructType()
      .add("name", new StructType()
        .add("firstname", StringType)
        .add("middlename", StringType)
        .add("lastname", StringType))
      .add("hobbies", ArrayType(StringType))
      .add("properties", MapType(StringType, StringType))

    val df5 = spark.createDataFrame(
      spark.sparkContext.parallelize(arrayStructureData), arrayStructureSchema)
    df5.printSchema()
    df5.show()



    val newdf = df5.select(df5("hobbies"),df5("properties.value"))

    newdf.show()

    spark.stop()


  }

}
