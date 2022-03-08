import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.ivy.plugins.trigger.Trigger
import org.apache.spark.sql.streaming.Trigger
import java.util
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.util.Properties
import scala.collection.JavaConverters._
import org.apache.spark.sql.execution.streaming.StreamExecution



object Consumer {
    
    def main(args: Array[String]): Unit = {
        consumeFromKafka("quick-start")
      val spark=SparkSession
        .builder
        .appName("Consumer")
        .config("spark.master", "local")
        .getOrCreate()
    
   while(true){
   //Thread.sleep(20000)
    spark.sparkContext.setLogLevel("WARN")
    var lines=spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "mytopic")
        .load()
        var r = lines.selectExpr("Cast(key AS STRING)", "CAST(value AS STRING)")
    //var r2=r
    
        r.writeStream
        .format("csv")
        .option("path", "/c/Users/user/Desktop/ProjectPath")
    //.option("checkPointLocation",  "/c/Users/leifr/Desktop/Kafka")
        .option("checkpointLocation",  "/c/Users/user/Desktop/Kafka")
        .start()
        .awaitTermination()
    }
    

    }

    def consumeFromKafka(topic: String) = {
        val props = new Properties()
        props.put("bootstrap.servers", "localhost:9092")
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        props.put("auto.offset.reset", "latest")
        props.put("group.id", "consumer-group")
        val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)
        consumer.subscribe(util.Arrays.asList(topic))

        
        while (true) {
            val record = consumer.poll(1000).asScala
            for (data <- record.iterator)
                println(data.value())
        }
    }
    

}

