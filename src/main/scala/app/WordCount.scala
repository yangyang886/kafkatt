package app

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
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
       * 3. accoding data HDFS HBase Local FS DB S3
       *  RDD 1.HDFS  2.Scala 3.other RDD
       *
       */
     val lines = sc.textFile("C://Users//23916//IdeaProjects//spark//README.md",1)

     /**
       * 4. data RDD Transformation map filter
       *
       */
   val words = lines.flatMap{line =>line.split(" ")} //words  flat
     /**
       * 4.2
       *
       */
     val  pairs = words.map{word => (word,1)}

     /**
       *4.3 total
       */
     val wordCounts = pairs.reduceByKey(_+_)

     wordCounts.foreach(wordNumberPair => println(wordNumberPair._1 +":"+wordNumberPair._2))

     sc.stop()
   }

}
