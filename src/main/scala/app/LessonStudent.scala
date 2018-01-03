package app

import org.apache.spark.sql.SparkSession
import redis.clients.jedis.Jedis

object LessonStudent {

  def main(args:Array[String]): Unit ={
    val spark = SparkSession
      .builder()
      .appName("sql test")
      .master("local")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    runJdbcDatasetExample(spark)

    spark.stop()

  }

  private def runJdbcDatasetExample(spark: SparkSession): Unit = {
    // $example on:jdbc_dataset$
    // Note: JDBC loading and saving can be achieved via either the load/save or jdbc methods
    // Loading data from a JDBC source
    val df = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://192.168.6.77:5432/cloudconfigdb")
      .option("dbtable", "sync_assemble_mem_info")
      .option("user", "dbuser")
      .option("password", "Sigma5t123")
      .load()
     .createOrReplaceTempView("wc")

    val redisHost = "192.168.6.76"
    val redisPort = 6379
    val redisClient = new Jedis(redisHost, redisPort)
    import spark.implicits._
    // df.filter($"user_code"==="11501145").createOrReplaceTempView("wc")

     //spark.sql("select assemble_code, assemble_name,json_tuple(props,'type') as type from wc")
      // .filter($"type"==="6001").groupBy("assemble_code").count().distinct().collect().foreach(
       //line =>
       //  redisClient.hset("my",line(0).toString,line(1).toString)
    // )


   /* spark.sql("select assemble_code,count  from wc ").collect().foreach(
      line =>
        redisClient.hset("my",line(0).toString,line(1).toString)
    )*/


  }


}
