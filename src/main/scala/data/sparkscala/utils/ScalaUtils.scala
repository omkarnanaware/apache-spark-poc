package data.sparkscala.utils

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}
import org.codehaus.jettison.json.JSONArray

import scala.collection.mutable.ArrayBuffer

class ScalaUtils {


  def kafkaJsonFlattener(x: ConsumerRecord[String,String]):Array[Row] = {

    val flattened:Array[String] = jsonArrayFlattener(x.value())
    val list:ArrayBuffer[Row] = new ArrayBuffer[Row]();
    if(flattened.length > 0) {
      for(json <- flattened) {
        list.append(Row(x.topic(),x.partition(),x.offset(),x.key(),json,x.timestamp()))

      }
    }else{
        list.append(Row(x.topic(),x.partition(),x.offset(),x.key(),x.value()))
      }
    list.toArray
    }

  def jsonArrayFlattener(jsonString: String):Array[String] = {

    val list: ArrayBuffer[String] = new ArrayBuffer[String]();

    try{
      val jsonArray : JSONArray = new JSONArray(jsonString);

      if(jsonArray != null){
        val len = jsonArray.length();
        for(i <-0 to len-1){
          list.append(jsonArray.get(i).toString());
        }
      }
    } catch {
      case ex:Exception =>
    }
    list.toArray
  }



  def createFlatSchema(schema:String):StructType = {

    println("ScalaUtils - createFlatSchema: " + schema)

    val fields = schema.split(",").map(fieldName => StructField(fieldName.split(":")(0), getSparkTypeMapping(fieldName.split(":")(1))))

    StructType(fields)

  }

  def createSchema(schema:String):StructType = {

    val fields = schema.split(",").map(fieldName => {

      val dataType = fieldName.splitAt(fieldName.indexOf(":"))._2;

      dataType match {

        case "object" => {println("inside object:" + fieldName); null}
        case "array" => { println("inside Array" + fieldName); null }
        case _ => {
            println("datatype:" + dataType)
            StructField(
              fieldName.split(":")(0),
              if(dataType.startsWith(":object") || dataType.startsWith(":array")) createSchema("\\(.*\\)".r.findFirstIn(dataType).getOrElse("").replace("#",",")) else getSparkTypeMapping(dataType))
          }
        }
      } )
    StructType(fields)

  }


  def getSparkTypeMapping(scalaType: Any):org.apache.spark.sql.types.DataType = {

    scalaType match {
      case "byte" => DataTypes.ByteType
      case "short" => DataTypes.ShortType
      case "integer" => DataTypes.IntegerType
      case "long" => DataTypes.LongType
      case "float" => DataTypes.FloatType
      case "double" => DataTypes.DoubleType
      case "string" => DataTypes.StringType
      case "boolean" => DataTypes.BooleanType
      case "timestamp" => DataTypes.TimestampType
      case "date" => DataTypes.DateType
      case "null" => DataTypes.NullType


      case "java.lang.Byte" => DataTypes.ByteType
      case "java.lang.Short" => DataTypes.ShortType
      case "java.lang.Integer" => DataTypes.IntegerType
      case "java.lang.Long" => DataTypes.LongType
      case "java.lang.Float" => DataTypes.FloatType
      case "java.lang.Double" => DataTypes.DoubleType
      case "java.lang.String" => DataTypes.StringType


      case "java.sql.Boolean" => DataTypes.BooleanType
      case "java.sql.Timestamp" => DataTypes.TimestampType
      case "java.sql.Date" => DataTypes.DateType

      case "scala.Byte" => DataTypes.ByteType
      case "scala.Short" => DataTypes.ShortType
      case "scala.Integer" => DataTypes.IntegerType
      case "scala.Long" => DataTypes.LongType
      case "scala.Float" => DataTypes.FloatType
      case "scala.Double" => DataTypes.DoubleType
      case "scala.String" => DataTypes.StringType
      case "Array[Byte]" => DataTypes.BinaryType

      case _ => throw new Exception()



    }



  }



}
