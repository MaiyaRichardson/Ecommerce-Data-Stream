import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka
import org.apache.spark.streaming.StreamingContext._
import scala.collection.mutable.ArrayBuffer
import java.io.{BufferedWriter, OutputStreamWriter}
import scala.collection.JavaConversions._
import scala.util.Random
import java.io.{File, FileWriter}
import au.com.bytecode.opencsv.CSVWriter
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
object Kafka {
var a=true
    def main(args: Array[String]): Unit = {
      val spark=SparkSession
      .builder
      .appName("Consumer")
      .config("spark.master", "local")
      .getOrCreate()
    
   // while (a==true)
   //Thread.sleep(20000)
    spark.sparkContext.setLogLevel("WARN")
         var lines=spark
         .readStream
         .format("kafka")
         .option("kafka.bootstrap.servers", "localhost:9092")
         .option("subscribe", "mytest")
         .option("failOnDataLoss", "false")
         .load()
         var r=lines.selectExpr("Cast(key AS STRING)", "CAST(value AS STRING)")
       
        
         
         r.writeStream
         .format("csv")
         .option("path", "/mnt/c/Users/user/Desktop/project3A")
         .option("failOnDataLoss", "false")
         .option("checkpointLocation",  "/mnt/c/Users/user/Desktop/project3A/src")
         .start()
       //  .awaitTermination(5000)
         //.stop()
       
            var r2=r
        /*  spark.sparkContext.setLogLevel("WARN")
            lines=spark
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "mytest")
            .load() */
            for (i<-0 to 5)
                {
                r=lines.selectExpr("Cast(key AS STRING)", "CAST(value AS STRING)")
                r2.union(r)
                Thread.sleep(5000)
                }
        
            r2.writeStream
            .format("csv")
            .option("path", "/mnt/c/Users/user/Desktop/project3A")
            .option("failOnDataLoss", "false")
            //.option("checkPointLocation",  "/c/Users/leifr/Desktop/Kafka")
            .option("checkpointLocation",  "/mnt/c/Users/user/Desktop/project3A")
            .start()
            .awaitTermination(5000)
  }
}