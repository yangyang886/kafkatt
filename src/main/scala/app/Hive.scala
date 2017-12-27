package app

import org.apache.spark.sql.SparkSession

object Hive {
def main(args:Array[String]): Unit ={


  val spark = SparkSession
    .builder()
    .appName("Spark Hive Example")
    .enableHiveSupport()
    .getOrCreate()

  import spark.implicits._
  import spark.sql

  sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive")
  sql("LOAD DATA LOCAL INPATH 'src/main/resources/kv1.txt' INTO TABLE src")

  // Queries are expressed in HiveQL
  sql("SELECT * FROM src").show()
}

}
