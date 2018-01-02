
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


 /* val caseClassDS = Seq(Person("Andy", 32)).toDS()

  caseClassDS.show()
*/
  // The inferred schema can be visualized using the printSchema() method
  val path = "src/main/resources/person.json"

  val peopleDF = spark.read.json(path)

  // The inferred schema can be visualized using the printSchema() method
  peopleDF.printSchema()

  peopleDF.select("b.1").show()
}

}

