package app

import org.apache.spark.{SparkConf, SparkContext}

object test {

  def main(args:Array[String]): Unit ={

    /**
      * 1.运行配置信息setMaster  url local
      */
    val conf = new SparkConf();//set sparkConfig
    conf.setAppName("MyFristSpark App!") // set app name
    conf.setMaster("local") // local not spark

    /**
      * 2.sparkContext  start DAGScheduler,TaskScheduler  master
      */
    val sc = new SparkContext(conf) //

    /**
      * list
      */
    val rdd01 = sc.makeRDD(List(1,2,3,4,5,6))
    val r01 = rdd01.map{x => x*x}
   // println(r01.collect().mkString(","))

    /**
      * Array
      */
    val rdd02 = sc.makeRDD(Array(1,2,3,4,5,6))
    val  r02 = rdd02.filter{x => x<5}
   // println(r02.collect().mkString(","))

    val rdd03 = sc.parallelize(List(1,2,3,4,5,6),1)
    val  r03 = rdd03.map(x => x+1)
   // println(r03.collect().mkString(","))

    val inputJsonFile = "C://Users//yang//test.json"

    val input5 = sc.textFile(inputJsonFile)
    input5.foreach(println)
  }

}
