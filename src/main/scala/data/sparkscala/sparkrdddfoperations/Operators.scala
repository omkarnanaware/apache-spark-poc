package data.sparkscala.sparkrdddfoperations

import org.apache.spark.sql.{SparkSession, DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * Class to demonstrate various Spark operators.
 * This class covers:
 * - Transformations (e.g., select, filter, map, join, etc.)
 * - Actions (e.g., show, collect, count, etc.)
 * - SQL operations (e.g., SELECT, JOIN, etc.)
 *
 * @param spark The SparkSession instance used to create the DataFrame.
 */
class Operators(spark: SparkSession) {

  import spark.implicits._

  // Define schema for the DataFrame
  private val schema = StructType(Array(
    StructField("id", IntegerType, nullable = true),
    StructField("name", StringType, nullable = true),
    StructField("value", DoubleType, nullable = true),
    StructField("category", StringType, nullable = true)
  ))

  // Create sample data
  private val data: Seq[Row] = Seq(
    Row(1, "Alice", 10.5, "A"),
    Row(2, "Bob", 20.0, "B"),
    Row(3, "Charlie", 30.75, "A"),
    Row(4, "David", 25.0, "B")
  )

  // Create DataFrame
  private val dataFrame: DataFrame = spark.createDataFrame(
    spark.sparkContext.parallelize(data),
    schema
  )

  /**
   * Demonstrates various DataFrame operators.
   */
  def demonstrateOperators(): Unit = {
    println("Demonstrating DataFrame operators:")

    // 1. Select: Project specific columns
    println("1. Select:")
    dataFrame.select("id", "name").show()

    // 2. Filter: Filter rows based on a condition
    println("2. Filter:")
    dataFrame.filter(col("value") > 20).show()

    // 3. WithColumn: Add or modify a column
    println("3. WithColumn:")
    dataFrame.withColumn("value_plus_one", col("value") + 1).show()

    // 4. Drop: Remove a column
    println("4. Drop:")
    dataFrame.drop("category").show()

    // 5. Rename: Rename a column
    println("5. Rename:")
    dataFrame.withColumnRenamed("value", "amount").show()

    // 6. GroupBy and Aggregate: Group by a column and perform aggregations
    println("6. GroupBy and Aggregate:")
    dataFrame.groupBy("category").agg(
      avg("value").as("avg_value"),
      max("value").as("max_value")
    ).show()

    // 7. Join: Join with another DataFrame
    println("7. Join:")
    val otherData = Seq(
      Row(1, "Alice"),
      Row(2, "Bob"),
      Row(3, "Charlie"),
      Row(5, "Eve")
    )
    val otherSchema = StructType(Array(
      StructField("id", IntegerType, nullable = true),
      StructField("full_name", StringType, nullable = true)
    ))
    val otherDF = spark.createDataFrame(
      spark.sparkContext.parallelize(otherData),
      otherSchema
    )
    dataFrame.join(otherDF, "id").show()

    // 8. Union: Combine rows from two DataFrames
    println("8. Union:")
    val unionDF = dataFrame.union(
      spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(5, "Eve", 40.0, "C"))),
        schema
      )
    )
    unionDF.show()

    // 9. Distinct: Remove duplicate rows
    println("9. Distinct:")
    dataFrame.union(unionDF).distinct().show()

    // 10. OrderBy: Sort rows based on column values
    println("10. OrderBy:")
    dataFrame.orderBy(col("value").desc).show()

    // 11. Limit: Limit the number of rows
    println("11. Limit:")
    dataFrame.limit(2).show()

    // 12. Sample: Sample rows from the DataFrame
    println("12. Sample:")
    dataFrame.sample(withReplacement = false, fraction = 0.5).show()

    // 13. DropDuplicates: Drop duplicate rows based on specific columns
    println("13. DropDuplicates:")
    dataFrame.dropDuplicates(Seq("category")).show()

    // 14. Explode: Explode an array column into multiple rows
    println("14. Explode:")
    val arrayDF = Seq(
      (1, Seq("A", "B")),
      (2, Seq("C", "D"))
    ).toDF("id", "letters")
    arrayDF.withColumn("letter", explode(col("letters"))).show()

    // 15. Cast: Cast a column to a different type
    println("15. Cast:")
    dataFrame.withColumn("value_as_string", col("value").cast(StringType)).show()

    // 16. Fill: Fill missing values
    println("16. Fill:")
    val dfWithNulls = dataFrame.withColumn("value", when(col("id") === 2, null).otherwise(col("value")))
    dfWithNulls.na.fill(Map("value" -> 0.0)).show()

    // 17. Replace: Replace specific values with new values
    println("17. Replace:")
    dataFrame.na.replace("category", Map("A" -> "Category A", "B" -> "Category B")).show()

    // 18. Pivot: Pivot data from long to wide format
    println("18. Pivot:")
    dataFrame.groupBy("id").pivot("category").agg(sum("value")).show()

    // 19. Coalesce: Reduce the number of partitions
    println("19. Coalesce:")
    dataFrame.coalesce(1).count()

    // 20. Repartition: Increase or decrease the number of partitions
    println("20. Repartition:")
    dataFrame.repartition(3).count()

    // 21. Cache: Cache the DataFrame for faster access
    println("21. Cache:")
    dataFrame.cache().count() // Trigger caching

    // 22. Uncache: Remove the DataFrame from cache
    println("22. Uncache:")
    dataFrame.unpersist() // Remove from cache
  }

  /**
   * Demonstrates SQL operators.
   */
  def demonstrateSQLOperators(): Unit = {
    println("Demonstrating SQL operators:")

    // Register DataFrame as a temporary view
    dataFrame.createOrReplaceTempView("data_table")

    // 1. SQL SELECT
    println("1. SQL SELECT:")
    val sqlDF = spark.sql("SELECT id, name FROM data_table WHERE value > 20")
    sqlDF.show()

    // 2. SQL JOIN
    println("2. SQL JOIN:")
    val otherDF = spark.sql(
      """
        |SELECT id, full_name
        |FROM (SELECT 1 AS id, 'Alice' AS full_name UNION ALL
        |      SELECT 2 AS id, 'Bob' UNION ALL
        |      SELECT 3 AS id, 'Charlie' UNION ALL
        |      SELECT 5 AS id, 'Eve')
        """.stripMargin
    )
    otherDF.createOrReplaceTempView("other_table")
    val sqlJoinDF = spark.sql(
      "SELECT a.id, a.name, b.full_name FROM data_table a JOIN other_table b ON a.id = b.id"
    )
    sqlJoinDF.show()

    // 3. SQL UNION
    println("3. SQL UNION:")
    val sqlUnionDF = spark.sql(
      """
        |SELECT id, name FROM data_table
        |UNION
        |SELECT id, full_name AS name FROM other_table
        """.stripMargin
    )
    sqlUnionDF.show()

    // 4. SQL ORDER BY
    println("4. SQL ORDER BY:")
    val sqlOrderDF = spark.sql("SELECT * FROM data_table ORDER BY value DESC")
    sqlOrderDF.show()

    // 5. SQL LIMIT
    println("5. SQL LIMIT:")
    val sqlLimitDF = spark.sql("SELECT * FROM data_table LIMIT 2")
    sqlLimitDF.show()
  }

}
