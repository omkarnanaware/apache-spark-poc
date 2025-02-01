package data.sparkscala.parser

import data.sparkscala.utils._
import data.sparkscala.utils.FrameworkConstants._
import org.apache.kafka.clients.consumer.ConsumerRecord
import data.sparkscala.config.SparkEnvConfig
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, from_json, when}
import org.apache.spark.sql.types.{ArrayType, DataType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
class ParserService {

  val env = new SparkEnvConfig

  val scalaUtils = new ScalaUtils()



  def execute(pipelineId:String, rdd: org.apache.spark.rdd.RDD[ConsumerRecord[String,String]], parserMap: Map[String,String], spark: SparkSession): (org.apache.spark.sql.DataFrame, Long) ={

    var metaMap = parserMap

    println("ParserService - Kafka Streaming", pipelineId, "Entry")

    try{
      import spark.implicits._

      val kafkaDfschema = scalaUtils.createFlatSchema(FrameworkConstants.CONS_KAFKA_PARSER_BASE_SCHEMA)
      val flattenedRdd: RDD[Row] = rdd.flatMap(x => {
        try{
          scalaUtils.kafkaJsonFlattener(x)
        }catch {
          case ex:Exception => {
          ex.printStackTrace()
            Array[Row]()
          }
        }
      })

      val flattenedDf = spark.createDataFrame(flattenedRdd,kafkaDfschema).repartition(col("offset"))

      val ggDFschema = scalaUtils.createFlatSchema(FrameworkConstants.CONS_KAFKA_PARSER_GG_SCHEMA)
      val ggParserDF= flattenedDf.withColumn("gg_data",from_json(col("value"),ggDFschema))
      val ggFlattenedDF = flattenDataframe(ggParserDF).select(FrameworkConstants.PROP_PARSER_SELECT.split(",").map(col(_)):_*)
        .withColumn("gg_data",when(col("op_type").isin(FrameworkConstants.PROP_DATA_PARSE_OPERATION_LIST.split(",").toList:_*),col("After")).otherwise(col("before")))



      val schemaDDL = "col string"
      val structSchema = DataType.fromDDL(schemaDDL).asInstanceOf[StructType]
      val parseDf = ggFlattenedDF.withColumn("data",from_json(col("gg_data"),structSchema))


      val successDf = parseDf.where(col("data").isNotNull)
      val failureDf = parseDf.where(col("data").isNull)

      if(failureDf.count() > 0) {


      }

      (successDf,successDf.count())
    } catch {
      case ex:Exception => println("Exception Occured")
        throw ex
    }




  }

  def flattenDataframe(df:DataFrame): DataFrame = {

    val fields = df.schema.fields
    val fieldNames = fields.map(x => x.name)
    val length  = fields.length

    for(i <- 0 to fields.length - 1 ){
      val field = fields(i)
      val fieldtype = field.dataType
      val fieldName = field.name
      fieldtype match {
        case arrayType:ArrayType =>
          println("ParserService","FlattenDataframe- ArrayType Conversion")
          val fieldNamesExcludingArray = fieldNames.filter(_ != fieldName)
          val fieldNamesAndExplode = fieldNamesExcludingArray ++ Array(s"explode_outer($fieldName) as $fieldName")
          //val fieldNameToSelect = (fieldNamesExcludingArray ++ Array(s"$fieldName.*"))
          val explodeDf = df.selectExpr(fieldNamesAndExplode: _*)
          return flattenDataframe(explodeDf)
        case structType:StructType =>
          println("ParserService","FlattenDataframe- StructType Conversion")
          val childFieldnames = structType.fieldNames.map(childname => fieldName + "." + childname)
          val newfieldNames = fieldNames.filter(_ != fieldName) ++ childFieldnames
          val renamedcols = newfieldNames.map(x => (col(x.toString()).as(x.toString().substring(x.toString().indexOf(".")+1))))
          //val renamedcols = newfieldNames.map(x => (col(x.toString()).as(x.toString().replace(".","_"))))
          val explodedf = df.select(renamedcols: _*)
          return flattenDataframe(explodedf)

        case _ =>
      }
    }
    df
  }


}
