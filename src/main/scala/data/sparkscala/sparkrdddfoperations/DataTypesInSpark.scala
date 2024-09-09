package data.sparkscala.sparkrdddf

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


/**
 * Class to demonstrate Spark and Scala datatypes.
 * It creates a DataFrame that includes columns for all Spark datatypes,
 * along with Scala and Python datatypes for comparison.
 *
 * Detailed information about Spark and Scala datatypes:
 *
 * Spark DataTypes:
 * - IntegerType: Represents a 32-bit integer.
 * - LongType: Represents a 64-bit integer.
 * - DoubleType: Represents a double-precision floating point.
 * - FloatType: Represents a single-precision floating point.
 * - StringType: Represents a string.
 * - BooleanType: Represents a boolean value (true or false).
 * - DateType: Represents a date without time (YYYY-MM-DD).
 * - TimestampType: Represents a timestamp (YYYY-MM-DD HH:MM:SS).
 * - ArrayType: Represents an array of elements of a specified type.
 * - MapType: Represents a map of keys and values of specified types.
 * - StructType: Represents a complex type consisting of multiple fields.
 *
 * Scala DataTypes:
 * - Int: Represents a 32-bit integer.
 * - Long: Represents a 64-bit integer.
 * - Double: Represents a double-precision floating point.
 * - Float: Represents a single-precision floating point.
 * - Char: Represents a single 16-bit Unicode character.
 * - String: Represents a sequence of characters.
 * - Boolean: Represents a boolean value (true or false).
 * - Date: Represents a date.
 * - Timestamp: Represents a timestamp.
 * - Array: Represents a collection of elements.
 * - Map: Represents a collection of key-value pairs.
 * - Tuple: Represents a fixed-size collection of elements of different types.
 *
 * @param spark The SparkSession instance used to create the DataFrame.
 */


class DataTypesInSpark(spark:SparkSession) {


  import spark.implicits._

  // Define a schema that includes all Spark datatypes
  private val schema = StructType(Array(
    StructField("integer", IntegerType, nullable = true),
    StructField("long", LongType, nullable = true),
    StructField("double", DoubleType, nullable = true),
    StructField("float", FloatType, nullable = true),
    StructField("string", StringType, nullable = true),
    StructField("boolean", BooleanType, nullable = true),
    StructField("date", DateType, nullable = true),
    StructField("timestamp", TimestampType, nullable = true),
    StructField("array", ArrayType(IntegerType), nullable = true),
    StructField("map", MapType(StringType, IntegerType), nullable = true),
    StructField("struct", StructType(Array(
      StructField("field1", IntegerType, nullable = true),
      StructField("field2", StringType, nullable = true)
    )), nullable = true)
  ))

  // Create a DataFrame with sample data for each datatype
  private val data = Seq(
    Row
    (1,
      12345678901L,
      3.14,
      1.23f,
      "example",
      true,
      java.sql.Date.valueOf("2024-09-10"),
      java.sql.Timestamp.valueOf("2024-09-10 10:00:00"),
      Seq(1, 2, 3),
      Map("key1" -> 1),
      (1, "value")
    ))

  // Create the DataFrame
  private val dataFrame: DataFrame = spark.createDataFrame(spark.sparkContext.parallelize(data),schema)

  // Method to show the DataFrame
  def showDataTypes(): Unit = {
    dataFrame.show(false)
  }


}
