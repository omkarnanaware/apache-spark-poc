package data.sparkscala.sparkrdddfoperations

import data.sparkscala.config.SparkEnvConfig
object JoinsInSpark extends App {

  val env = new SparkEnvConfig()

  val conf = env.sparkConfig()

  val sc = env.createsparkContext(conf)

  val spark = env.createsparkSession(sc)

  import spark.implicits._

  val df1 = Seq((1,"Omkar"),(2,"Raja"))

  case class student(Id:Int,Name:String)
  case class subject(SubjectId:Int,Subject:String)

  case class marks(SubId:Int,StudId:Int,Marks:Int)

  val studentDf = Seq(
    student(1,"Omkar"),
    student(2,"Rohan"),
    student(3,"Raja"),
    student(4,"Babu")
  ).toDF()


  //studentDf.show()


  val subjectDf = Seq(
    subject(100, "Maths"),
    subject(101, "Physics"),
    subject(102, "Chem")
  ).toDF()


  val marksDf = Seq(
    marks(100,1,81),
    marks(100,2,81),
    marks(100,3,81),
    marks(101,1,81),
    marks(101,2,81),
    marks(101,3,81),
    marks(102,1,81),
    marks(102,2,81),
    marks(102,3,81),
  ).toDF()


 //Inner join  :- Join student marks and subject dfs
  val df = marksDf.join(subjectDf,marksDf("SubId")===subjectDf("SubjectId"),"inner")
    .join(studentDf,marksDf("StudId")===studentDf("Id"))
  df.show()
  df.explain()


  val colLukUp = "Name"

  val list = df.select(colLukUp).collect().map(f => f.getAs[String](colLukUp))

  list.foreach(println)

  println(list)


}
