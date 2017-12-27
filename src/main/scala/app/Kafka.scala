package app




import com.alibaba.fastjson.JSON
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}








object Kafka {

       def main(args: Array[String]): Unit = {
              val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
              val ssc = new StreamingContext(conf, Seconds(1))

              val kafkaParams = Map[String, String](
                //post 9092
                     "metadata.broker.list" -> "",
                     "group.id" -> "45",
                     "auto.offset.reset" -> "smallest"
              )
              val topics = Set("trace")



           val   kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)



              kafkaStream.foreachRDD(rdd=>{
                     rdd.foreach(line => {
                           // println(line._2)
                           val json = JSON.parseObject(line._2)
                            val traceType = json.getString("traceInfos")
                            println(traceType)
                     })
              })




              ssc.start()
              ssc.awaitTermination()



       }




}
