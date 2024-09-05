package data.sparkscala.utils

import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}

class ScalaUtils {




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
