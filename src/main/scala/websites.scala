import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkConf
import scala.util.Random.nextInt
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.P
import scala.util.Random.nextInt

object websites {
    def main(args:Array[String]): Unit = {
        /*val spark = SparkSession
            .builder
            .appName("KafkaSparkIntegration")
            .master("local")
            .getOrCreate()

        spark.sparkContext.setLogLevel("WARN")

        import spark.implicits._

        val rdd = spark.sparkContext.parallelize(Array(("USA", "Boston"), ("India", "Mumbai"), ("trisha", 28), ("bob", 52), ("fred", 23)))
        val df = rdd.toDF("Customer_Name", "age")

        df.selectExpr("CAST(Customer_Name AS String) AS key", "CAST(Customer_ID AS String) AS value")
            .write
            .format("kafka")
            .option("topic", "newtopic")
            .option("kafka.bootstrap.servers","localhost:9092")
            .option("checkpointLocation", "/home/gabrielklein/week11/KafkaSparkIntegration")
            .save()

        spark.close()

        def randomStringGen(length: Int) = scala.util.Random.alphanumeric.take(length).mkString
        var name = "dsd"
        var Id = "sdsd"
        //var website
        //val df = sparkContext.parallelize(Seq.fill(4000){(randomStringGen(4), randomStringGen(4), randomStringGen(6))}, 10).toDF("Order_ID", "Customer_Name", "Customer_ID")})
        df.write.csv("s3://my-bucket/dummy-data/")
        */
        randomGenerator()
    }

    def randomGenerator(): Unit = {
    
    var emails = List("www.jacobblack.com", "www.jekh@gmail.com", "www.weloveapples.com", "www.amazon.com", "www.amazon.com",
    "www.happyy.com", "www.eatvegtables.com", "www.thetable.com", "www.isbroken.com" , "wwww.crystals.com", "www.nomatter.com", "www.intersteller.com", "www.whynot.com", "www.blah.com")

    var ran = new scala.util.Random

    println(emails(ran.nextInt(emails.size)))
}

    

    
}