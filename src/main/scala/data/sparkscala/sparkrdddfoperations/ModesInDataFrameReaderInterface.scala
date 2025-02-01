package data.sparkscala.sparkrdddfoperations

import data.sparkscala.config.SparkEnvConfig
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object ModesInDataFrameReaderInterface extends App {

  val env = new SparkEnvConfig()
  val conf = env.sparkConfig()
  val sc = env.createsparkContext(conf)
  val spark = env.createsparkSession(sc)

  import spark.implicits._

  /*
  ColumnName1,ColumnName2
  valid1,123
  validnull,
  invalid1,none
  valid2,123
  validnull,
  invalid2,none
  valid3,123
  validnull,
  invalid3,none
   */


  val schema = new StructType()
    .add("column1", StringType, true)
    .add("column2", IntegerType, true)

  //1) PERMISSIVE

  //PERMISSIVE mode fills invalid or missing values with null but reads all rows.

  var df = spark.read
    .schema(schema)
    .option("mode", "PERMISSIVE")
    .option("header", "true") // Assume CSV has a header
    .csv("src/main/resources/CSVDatawithouterror.csv")

  df.show()

  //2) DROPMALFORMED

  //In DROPMALFORMED mode, Spark discards any rows that do not fit the schema. Only valid rows remain in the DataFrame.

    df = spark.read
    .schema(schema)
    .option("mode", "DROPMALFORMED")
    .option("header", "true") // Assume CSV has a header
    .csv("src/main/resources/CSVDatawithouterror.csv")

  df.show()

  //Using columnNameOfCorruptRecord Option with CSV
  //Here, we store any malformed rows in a special column for inspection, while keeping the valid records.

  val schemawithCorruptRec = new StructType()
    .add("column1", StringType, true)
    .add("column2", IntegerType, true)
    .add("corrupt_record", StringType, true)

  val df1 = spark.read
    .schema(schemawithCorruptRec)
    .option("header", "true")
    .option("mode", "PERMISSIVE")
    .option("columnNameOfCorruptRecord", "corrupt_record")
    .option("enforceSchema",true)
    .csv("src/main/resources/CSVDatawithouterror.csv")

  df1.show()

  df1.printSchema()

  df1.cache()

  // Filter out the corrupt records and drop the corrupt column
  val validDf = df1.filter(col("corrupt_record").isNull)

  validDf.show()


  val structureddf1 = spark.readStream
    .schema(schemawithCorruptRec)
    .option("header", "true")
    .option("mode", "PERMISSIVE")
    .option("columnNameOfCorruptRecord", "corrupt_record")
    .option("enforceSchema", true)
    .csv("src/main/resources/CSVDatawithouterror.csv")

  structureddf1.show()

  structureddf1.printSchema()

  structureddf1.cache()

  // Filter out the corrupt records and drop the corrupt column
  val validDf1 = structureddf1.filter(col("corrupt_record").isNull)

  validDf1.show()






}
