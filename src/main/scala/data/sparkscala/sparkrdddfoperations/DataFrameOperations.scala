package data.sparkscala.sparkrdddf

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class DataFrameOperations(spark: SparkSession) {

  // 1. Create DataFrame from a Seq of tuples and column names
  def createDataFrame(data: Seq[(Int, String, Int)], schema: Seq[String]): DataFrame = {
    // Convert sequence to DataFrame using the provided schema.
    // The `toDF` method is used to assign the schema (column names) to the DataFrame.
    import spark.implicits._  // Needed for converting Scala collections to DataFrame.
    spark.createDataFrame(data).toDF(schema: _*)  // `schema: _*` expands the list into varargs for the `toDF` method.
  }

  // 2. Filter DataFrame based on a condition
  def filterDataFrame(df: DataFrame, condition: String): DataFrame = {
    // The `filter` method filters rows based on a condition provided in SQL-like syntax.
    df.filter(condition)
  }

  // 3. Select specific columns from a DataFrame
  def selectColumns(df: DataFrame, columns: String*): DataFrame = {
    // The `select` method selects the columns specified by name (varargs *).
    df.select(columns.map(col): _*)
  }

  // 4. Group and aggregate data
  def groupAndAggregate(df: DataFrame, groupByColumn: String, aggColumn: String, aggFunc: String): DataFrame = {
    // The `groupBy` method groups the DataFrame by the specified column.
    // The `agg` method performs the aggregation using an aggregation function (e.g., sum, avg).
    df.groupBy(groupByColumn).agg(expr(s"$aggFunc($aggColumn)"))
  }

  // 5. Sort DataFrame by a column (ascending or descending)
  def sortDataFrame(df: DataFrame, sortColumn: String, ascending: Boolean = true): DataFrame = {
    // The `orderBy` method sorts the DataFrame. If `ascending` is true, it sorts in ascending order.
    if (ascending) df.orderBy(col(sortColumn).asc)
    else df.orderBy(col(sortColumn).desc)
  }

  // 6. Join two DataFrames on a specific column
  def joinDataFrames(df1: DataFrame, df2: DataFrame, joinColumn: String, joinType: String = "inner"): DataFrame = {
    // The `join` method joins two DataFrames on a common column.
    // The `joinType` parameter specifies the type of join (e.g., inner, left, right, outer).
    df1.join(df2, col(joinColumn), joinType)
  }

  // 7. Add a new column based on an expression
  def addColumn(df: DataFrame, newColName: String, expression: String): DataFrame = {
    // The `withColumn` method adds a new column to the DataFrame based on a given expression.
    df.withColumn(newColName, expr(expression))
  }

  // 8. Remove duplicates based on specific columns
  def removeDuplicates(df: DataFrame, subset: Seq[String]): DataFrame = {
    // The `dropDuplicates` method removes duplicate rows based on a subset of columns.
    df.dropDuplicates(subset)
  }

  // 9. Show the first few rows of the DataFrame
  def showDataFrame(df: DataFrame, numRows: Int = 5): Unit = {
    // The `show` method displays the first `numRows` rows of the DataFrame in a tabular format.
    df.show(numRows)
  }
}
