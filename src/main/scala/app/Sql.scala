package app

import org.apache.spark.sql.SparkSession


object Sql {

  case class Person(name: String, age: Long)
def main(args:Array[String]): Unit ={


  val spark = SparkSession
    .builder()
    .appName("sql test")
    .master("local")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()

  import spark.implicits._

  //val df = spark.read.option("header","true").csv("src/main/resources/test.csv")
  //val caseClassDS = Seq(Person("Andy", 32)).toDS()

  //caseClassDS.show()
  //val df = spark.read.json("src/main/resources/test.json")
  //df.show()
  // Create DataSet representing the stream of input lines from kafka
  val lines = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "")
    .option("", "")
    .load()
    .selectExpr("CAST(value AS STRING)")
    .as[String]


  spark.stop()
}

}
