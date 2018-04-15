package main.scala.streamingworkouts
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import StreamingContext._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.storage.StorageLevel
import scala.collection.mutable.ListBuffer
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{ Put }
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.{ LongWritable, Writable, IntWritable, Text }
import org.apache.hadoop.mapred.{ TextOutputFormat, JobConf }
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import StreamingContext._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SQLContext._
import org.elasticsearch.spark.streaming._
import org.apache.spark.sql.Row;
import org.elasticsearch.spark._
import java.util.Date
import org.apache.spark.sql.hive.orc._
import org.apache.spark.sql._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.JdbcRDD
import java.sql.{Connection, DriverManager, ResultSet}
import java.util.Properties
import java.util.{Date, Properties}
 import org.apache.kafka.clients.producer._
 import org.apache.spark._

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.util.Random
import scala.tools.ant.sabbus.Break
import java.util.ArrayList

object lappractise3 {  
  
  case class schema(id:Int,name:String,age:String,tab:String,amt:Int)

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("kafkahbase").setMaster("local[*]")
    val sparkcontext = new SparkContext(sparkConf)
    sparkcontext.setLogLevel("ERROR")
    val ssc = new StreamingContext(sparkcontext, Seconds(2))
    ssc.checkpoint("checkpointdir")
    val spark = SparkSession
      .builder()
      .master("local")
      
      //.config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()
      
      import spark.implicits._
       import spark.sql
    
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",                                                                                                                      
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "kafka1",
      "auto.offset.reset" -> "latest")
      
          val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("client.id", "ScalaProducerExample")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  

      
      
      
      
      
      val prop = new Properties() 
       prop.put("user", "root")
       prop.put("password", "root")
       
       
    val topics = Array("tk1")
    val stream = KafkaUtils.createDirectStream[String, String](  
      ssc,
      PreferConsistent,                                                                                                 
         
      Subscribe[String, String](topics, kafkaParams))

    val kafkastream = stream.map(record => (record.key, record.value))
          
      val inputStream1 = kafkastream.map(rec => rec._2).print()

      val inputStream = kafkastream.map(rec => rec._2)
      
    
    
     inputStream.foreachRDD(rdd=>
      {
        if(!rdd.isEmpty){
          
          
          val df1 = rdd.map(line => line.split(",")).map(x=>schema(x(0).toInt,x(1),x(2),x(3),x(4).toInt)).toDF()
     //     df1.show()
          
          println("creating table")
          df1.createOrReplaceTempView("data");
          
          spark.sql("select * from data").show()
          
       //  val count= spark.sql("select count(*) from data where id='' or name = '' or age='' or tab =''  or amt=''").rdd.map(x=>x.mkString(","))
           
    val count=spark.sql("select count(*) from data where age='_null_' or id =''")  //count came with 2 but other fields     
          
        val finalcount = count.first().getLong(0)
        
        println(finalcount)
        
        if(finalcount>0)
        {
          
        }
        
        
       
       
        
        
   
        
          
        /*  val row = df1.rdd.first();
          val counter = 0
          val isValid = true
          val list = new 
          while(counter != row.length){
            if(row.get(counter) == null || row.get(counter) == "") {
              lis
            }            
            counter.+(1)
          }
          
          if(true){
            println("Go to Error Queue")
          }else {
            println("Go to Success Queue")
          }*/
         
          //val data = df1.rdd.map(x=>x.getAs(0))
          
          
          
          
          /* df1.select("id").show()
         df1.
         df1.select("name").show()
         df1.show()*/
         
         
         
         
         
        /* val producer = new KafkaProducer[String,String](kafkaParams1)
                    val message=new ProducerRecord[String, String]("output",null,x)
                    producer.send(message)*/
         
         /*val producer = new KafkaProducer[String, String](props)
         val message=new ProducerRecord[String, String]("df1",null)
         producer.send(message)*/
         
         
                      
        }
      }
      )  
      
      
    ssc.start()
    ssc.awaitTermination()

  }
}
