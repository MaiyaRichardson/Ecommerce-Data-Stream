// import org.apache.log4j.{Level, Logger}
// import org.apache.spark.SparkConf
// import org.apache.spark.streaming.kafka.KafkaUtils
// import org.apache.spark.streaming.{Seconds, StreamingContext}
// import org.apache.spark.streaming._
// import org.apache.spark.streaming.kafka
// import org.apache.spark.streaming.StreamingContext._
// import scala.collection.mutable.ArrayBuffer
// import java.io.{BufferedWriter, OutputStreamWriter}
// import scala.collection.JavaConversions._
// import scala.util.Random
// import java.io.{File, FileWriter}
// import au.com.bytecode.opencsv.CSVWriter
// import scala.collection.mutable.ListBuffer
// import org.apache.spark.sql.DataFrame
// import org.apache.spark.sql.SparkSession

// object Consumer {
//     def main(args: Array[String]): Unit = {
//         consumeFromKafka("quick-start")
//     }

//     def consumeFromKafka(topic: String) = {
//         val props = new Properties()
//         props.put("bootstrap.servers", "localhost:9094")
//         props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
//         props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
//         props.put("auto.offset.reset", "latest")
//         props.put("group.id", "consumer-group")
//         val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)
//         consumer.subscribe(util.Arrays.asList(topic))
//         while (true) {
//         val record = consumer.poll(1000).asScala
//         for (data <- record.iterator)
//             println(data.value())
//         }
//     }

    
// }
