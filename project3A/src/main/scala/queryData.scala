import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import java.sql.DriverManager
import java.sql.Connection
import java.util.Scanner
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{min, max}
import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.joins.BroadcastHashJoinExec
import org.apache.spark.sql.expressions.scalalang.typed
import org.apache.spark.sql.functions.{avg, broadcast, col, max}
import org.apache.spark.sql.types
import org.apache.hadoop.fs.HardLink
import org.apache.spark.sql.functions.{to_date, to_timestamp}
import org.apache.spark.sql.types.{IntegerType,StringType,StructType,StructField}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.IntegerType



object queryData {
    val conf = new SparkConf().setMaster("local").setAppName("Covid")
    private val sc = new SparkContext(conf)
    private val hiveCtx = new HiveContext(sc)

    def main(args: Array[String]): Unit = {

    val spark = SparkSession
            .builder
            .appName("ProjMain")
            .master("local")
            .getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        import spark.implicits._

    
    spar(spark)
        
    }

    
    def spar(spark:SparkSession): Unit={
        val df = spark.read.csv("input/team2_data.csv").toDF("order_id","customer_id","customer_name","product_id","product_name","product_category","payment_type","qty","price","datetime","country","city","ecommerce_website_name","payment_txn_id","payment_txn_success","failure_reason")

        // //val f= df.where(col("payment_txn_succcess" = "Y"))
        // val f = df.where(df("payment_txn_success") === "N")
        // f.show()
        // f.write.csv("input/Hardik's")
        // //var co =f.count()
        // //println("total: "+co)


        var newdf = df.withColumn("qty",col("qty").cast(IntegerType))
        newdf.createOrReplaceTempView("df2")
        df.createOrReplaceTempView("df1")
        

        var query = "Select * From df1"

        val f= spark.sql(query)
        f.show(100,100)

        var maxPurchase = "Select country,max(qty) from df2 group by country"
        //var maxPurchase = "select qty,product_name from df2 where country = \"Germany\" "
        val g = spark.sql(maxPurchase)
        g.show(10)
        g.repartition(1).write.format("com.databricks.spark.csv").mode("overwrite").csv("input/groupedCountry")
        //g.write.csv("input/germanycountryPurchase")

    }



    //     val output = hiveCtx.read
    //         .format("csv")
    //         .option("inferschema", "true")
    //         .option("header", "true")
    //         .option("delimiter", ",")
    //         .load("input/team2_data.csv")

    //         //output.printSchema()
    //     output.limit(15).show() // Prints out the first 15 lines of the dataframe

    //     // output.registerTempTable("data2") // This will create a temporary table from your dataframe reader that can be used for queries. 

    //     output.createOrReplaceTempView("temp_data4")
    //     //hiveCtx.sql("DROP TABLE IF EXISTS covid1")    
    //     hiveCtx.sql("CREATE TABLE IF NOT EXISTS Kafka (order_id STRING,customer_id STRING,customer_name STRING,product_id STRING,product_name STRING,product_category STRING,payment_type STRING,qty STRING,price STRING,datetime STRING,country STRING,city STRING,ecommerce_website_name STRING,payment_txn_id STRING,payment_txn_success STRING,failure_reason STRING) ROW FORMAT DELIMITED BY ',' FIELDS TERMINATED BY ','")
    //     hiveCtx.sql("INSERT INTO Kafka SELECT * FROM temp_data4")
    //     val summary = hiveCtx.sql("SELECT * FROM Kafka LIMIT 10")
        
    //     summary.show()
    //     summary.write.mode("overwrite").csv("results/view10")       
    // }
    // def maxPurchase(): Unit = {
    //     val result = hiveCtx.sql("Select MAX(price), order_id, product_name, qty, datetime, customer_name from Kafka limit 1")
    //     result.show()
    //     result.write.csv("input/maxPurchase")
    // }
    // def minPurchase(): Unit = {
    //     val result =
    //     hiveCtx.sql("Select MIN(price), order_id, product_name, qty, datetime, customer_name from Kafka limit 1")
    //     result.show()
    //     result.write.csv("input/minPurchase")
    // }

    // def orderCount(): Unit = {
    //     val result =
    //     hiveCtx.sql("Select Count(payment_txn_success) from Kafka")
    //     result.show()
    //     result.write.csv("input/orderCount")
    // }
    //}
}
