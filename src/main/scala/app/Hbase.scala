package app

import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, TableName}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

object Hbase{
    def main(args:Array[String]): Unit ={

    val logger = LoggerFactory.getLogger(this.getClass())

      val sparkConf = new SparkConf().setAppName("spark").setMaster("local")

      val sc =new SparkContext(sparkConf)


      val conf = HBaseConfiguration.create()
      // //设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
      conf.set("hbase.zookeeper.quorum","")
      //设置zookeeper连接端口，默认2181
      conf.set("hbase.zookeeper.property.clientPort","2181")

      val tablename ="leave"

      conf.set(TableInputFormat.INPUT_TABLE, tablename)

      // 如果表不存在则创建表
      val admin = new HBaseAdmin(conf)
      if (!admin.isTableAvailable(tablename)) {
        val tableDesc = new HTableDescriptor(TableName.valueOf(tablename))
        admin.createTable(tableDesc)
      }

      //读取数据并转化成rdd
      val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
        classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
        classOf[org.apache.hadoop.hbase.client.Result])




      hBaseRDD.foreach{case (_,result) =>{
        //获取行键
        val key = Bytes.toString(result.getRow)
        //通过列族和列名获取列
        val name = Bytes.toString(result.getValue("confDataList".getBytes,"9".getBytes))
        val age = Bytes.toString(result.getValue("confDataList".getBytes,"10".getBytes))
        println("Row key:"+key+" Name:"+name+" Age:"+age)
       // logger.info("Row key:"+key+" Name:"+name+" Age:"+age)
      }}



      sc.stop()
      admin.close()

    }

}
