package data.sparkscala.driver

import data.sparkscala.config.SparkEnvConfig
import data.sparkscala.sparkrdddfoperations.DataFrameOperations

object DataFrameOperationsDriver {

  def main(args: Array[String]): Unit = {

    val env = new SparkEnvConfig()

    val config = env.sparkConfig()
    val sc = env.createsparkContext(config)
    val spark = env.createsparkSession(sc)

  val data = Seq(
    (1, "Alice", 34),
    (2, "Bob", 45),
    (3, "Cathy", 29),
    (4, "David", 40),
    (5, "Eva", 34)
  )

  // Schema for the DataFrame
  val schema = Seq("ID", "Name", "Age")

  // Initialize the DataFrameOperations class
  val dfOps = new DataFrameOperations(spark)

  // 1. Create DataFrame
  val df = dfOps.createDataFrame(data, schema)
  dfOps.showDataFrame(df)

  // 2. Filter DataFrame (filter rows where Age > 35)
  val filteredDf = dfOps.filterDataFrame(df, "Age > 35")
  dfOps.showDataFrame(filteredDf)

  // 3. Select specific columns
  val selectedDf = dfOps.selectColumns(df, "Name", "Age")
  dfOps.showDataFrame(selectedDf)

  // 4. Group and aggregate (count number of people by Age)
  val aggregatedDf = dfOps.groupAndAggregate(df, "Age", "*", "count")
  dfOps.showDataFrame(aggregatedDf)

  // 5. Sort DataFrame by Age in descending order
  val sortedDf = dfOps.sortDataFrame(df, "Age", ascending = false)
  dfOps.showDataFrame(sortedDf)

  // 6. Add a new column (double the age)
  val newColDf = dfOps.addColumn(df, "Age_Doubled", "Age * 2")
  dfOps.showDataFrame(newColDf)

  // 7. Remove duplicates (based on Age)
  val dedupDf = dfOps.removeDuplicates(df, Seq("Age"))
  dfOps.showDataFrame(dedupDf)

  // 8. Join two DataFrames (self-join for demonstration)
  val joinedDf = dfOps.joinDataFrames(df.as("firstdf"), df.as("seconddf"), "firstdf.ID")
  dfOps.showDataFrame(joinedDf)


}

}
