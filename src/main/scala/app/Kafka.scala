/*

package app




import com.alibaba.fastjson.JSON
import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
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
                       val conf = HBaseConfiguration.create()
                       // //设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
                       conf.set("hbase.zookeeper.quorum","192.168.6.70:2181,192.168.6.71:2181,192.168.6.72:2181")
                       //设置zookeeper连接端口，默认2181
                       conf.set("hbase.zookeeper.property.clientPort","2181")

                       val tablename ="location"

                       conf.set(TableInputFormat.INPUT_TABLE, tablename)
                     })
              })




              ssc.start()
              ssc.awaitTermination()



       }




}

*/
