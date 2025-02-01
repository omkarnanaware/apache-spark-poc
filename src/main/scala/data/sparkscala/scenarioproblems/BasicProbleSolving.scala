package data.sparkscala.scenarioproblems

import org.apache.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, current_date}

/*
give me 10 sceanario based question in spark and give input data for those qustions
Sure! Here are 10 scenario-based questions in Apache Spark along with the input data for each question:

Scenario 1
Question: How would you load a CSV file into a Spark DataFrame and show its schema? Input Data:

plaintext
Name, Age, City
John, 25, New York
Emma, 30, London
Liam, 22, Tokyo
Scenario 2
Question: How would you filter the DataFrame to select rows where the age is greater than 25? Input Data:

plaintext
Name, Age, City
John, 25, New York
Emma, 30, London
Liam, 22, Tokyo


Scenario 3
Question: How would you perform a group by operation on the City column and find the average age? Input Data:

plaintext
Name, Age, City
John, 25, New York
Emma, 30, London
Liam, 22, Tokyo
Sophie, 28, Tokyo


Scenario 4
Question: How would you join two DataFrames on a common column (e.g., Name)? Input Data: DataFrame 1:

plaintext
Name, Age
John, 25
Emma, 30
Liam, 22
DataFrame 2:

plaintext
Name, City
John, New York
Emma, London
Liam, Tokyo


Scenario 5
Question: How would you add a new column to a DataFrame that calculates the age in days? Input Data:

plaintext
Name, Age
John, 25
Emma, 30
Liam, 22


Scenario 6
Question: How would you remove duplicates from a DataFrame based on a specific column (e.g., City)? Input Data:

plaintext
Name, Age, City
John, 25, New York
Emma, 30, London
Liam, 22, Tokyo
Sophie, 28, Tokyo
Scenario 7
Question: How would you handle null or missing values in a DataFrame by filling them with a default value? Input Data:

plaintext
Name, Age, City
John, 25, New York
Emma, , London
Liam, 22,
Sophie, 28, Tokyo
Scenario 8
Question: How would you sort a DataFrame by the Age column in descending order? Input Data:

plaintext
Name, Age, City
John, 25, New York
Emma, 30, London
Liam, 22, Tokyo
Sophie, 28, Tokyo
Scenario 9
Question: How would you write the DataFrame to a Parquet file? Input Data:

plaintext
Name, Age, City
John, 25, New York
Emma, 30, London
Liam, 22, Tokyo
Sophie, 28, Tokyo
Scenario 10
Question: How would you read a JSON file into a Spark DataFrame and display the first 5 rows? Input Data:

json
[
    {"Name": "John", "Age": 25, "City": "New York"},
    {"Name": "Emma", "Age": 30, "City": "London"},
    {"Name": "Liam", "Age": 22, "City": "Tokyo"}
]
 */



object BasicProbleSolving extends App {

  //first Scenario


  val spark = SparkSession.builder().master("local").getOrCreate()

  val firstFilePath = "/home/datamaking/Documents/data/inputdata.txt"

  val firstDf = spark.read.
     option ("header", "true").
     option ("inferSchema", "true").
     option ("ignoreLeadingWhiteSpace", "true").
     option ("ignoreTrailingWhiteSpace", "true").
     csv (firstFilePath)

  //firstDf.show()

  //second Scenari

  val peopleWithAgeLessThan25 = firstDf.where(col("Age") > 25)

  //peopleWithAgeLessThan25.show()

  //Third Scenario

  val groupByCityDfAvgAge = firstDf.groupBy("City").avg("Age")

  //groupByCityDfAvgAge.show()

  //Fourth Scenario

  val secondFilePath = "/home/datamaking/Documents/data/secondinputdata.txt"

  val secondDf = spark.read.
    option("header", "true").
    option("inferSchema", "true").
    option("ignoreLeadingWhiteSpace", "true").
    option("ignoreTrailingWhiteSpace", "true").
    csv(secondFilePath)

  //secondDf.show

  //Join on the Name

  val joinedDf = firstDf.join(secondDf,firstDf("Name")===secondDf("Name"),"inner")

  //joinedDf.printSchema()


  //Fifth sceario

  val currnetDateDf = firstDf.withColumn("current_date",current_date())

  //currnetDateDf.show()

  val totalDateLived = currnetDateDf.withColumn("totalDaysLived",col("Age") * 365)

  totalDateLived.show()
}
