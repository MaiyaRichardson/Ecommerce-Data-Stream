import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkConf
import scala.util.Random.nextInt
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.P
import java.math.BigInteger
import scala.util.matching.Regex
<<<<<<< HEAD

import java.util.Properties
import java.util.UUID.randomUUID

=======
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit.{DAYS,MINUTES}
import java.time.LocalDate
import scala.io._
import java.io.File
import java.util.UUID.randomUUID
import java.io.{BufferedWriter, FileWriter}
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.util.Random
import au.com.bytecode.opencsv.CSVWriter
import au.com.bytecode.opencsv.CSVReader
import java.io.FileReader
import _root_.javax.xml.crypto.Data
>>>>>>> testing
object EcommerceProj {
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
        
        while(true){
            randomGenerator() 
            Thread.sleep(2000)
        }
        
        //val csvFields1 = List(id, qty, price)
    }

    def randomGenerator(): Unit = {
<<<<<<< HEAD
        var res2 = 0
        while(true){
=======
        val rdm = new scala.util.Random
        var res2 = 0
        var rdmTxn = 1
        var rdmPID = 2
        var randOrderId = 0
        var aa = 0
        
        
        //var randomnm = randomTime()
        
        
        
        //println(a)
        //var hh = new ja(qty,price,Date.toString)

        /*var fw = new FileWriter("ecommerceData1.csv",true);
        val outputFile = new BufferedWriter(fw) //replace the path with the desired path and filename with the desired filename

        var rw = new FileReader("ecommerceData1.csv");

        //val csvWriter = new CSVWriter(fw)
        val reader = new CSVReader(rw)
        var csvLines = reader.readAll()
        var fd = csvLines.get(csvLines.size-1)
        println(fd(0))
        var id = (fd(0).toInt+1)
        //val csvFields = Array("id", "qty", "prices", "DateTime")
        //val csvFields1 = List(id, qty, price)
        // val nameList = List(“Deepak”, “Sangeeta”, “Geetika”, “Anubhav”, “Sahil”, “Akshay”)
        // val ageList = (24 to 26).toList
        // val cityList = List(“Delhi”, “Kolkata”, “Chennai”, “Mumbai”)
        // val random = new Random()
        var listOfRecords = new ListBuffer[Array[String]]()
        var listOfRecords1 = new ListBuffer[Array[String]]()
        //listOfRecords += csvFields
        //listOfRecords1 += csvFields1
        //println(anyName())
        
        //csvWriter.writeAll(listOfRecords.toList)
        //csvWriter.writeAll(List(Array(randomTime, res2, id.toString, qty.toString, price.toString,Date)))
        outputFile.close()
        */

        /*while(true){
            var prnt = print()
>>>>>>> testing
            nameGenerator()
            
            Thread.sleep(2000) // wait for 2 seconds
        }
        //nameGenerator()
        //countryCityGenerator()
        //productNameCategoryGenerator()
        */
        
            //println(Date)

            //var exp = randomProductID()
            // println("before ja class")
            // println(qty,price,date.toString)
            // println("after")

            // println(qty + price + date)
        def nameGen(): String = {
            val firstNames = List("Steve", "Tony", "Peter", "Miles", "Cameron", "Kyle", "Brandon", "Summer", "Sunshine", "Autumn", "Sharyar", "Keisha", "Hardik", "Daulton", "Abubacarr", "Hardik", "Giancarlos", "Alvin", "Mai")
            val lastNames = List("Parker", "Stark", "Rodgers", "moon", "rain","wind", "sea", "morning", "snow", "lake", "sunset", "pine", "shadow", "leaf","sequoia", "cedar", "wrath", "blessing", "spirit", "nova", "storm", "burst","giant", "elemental", "throne", "game", "weed", "stone", "apogee", "bang")
            
            var randFirstNames = firstNames(nextInt(firstNames.length))
            var randlastNames = ""
            var randName = ""
            
            
                randFirstNames match {
                    case "Steve" =>
                        randlastNames = lastNames(nextInt(4))
                    case "Tony" =>
                        randlastNames = lastNames(nextInt(4)+4)
                    case "Peter" =>
                        randlastNames = lastNames(nextInt(4)+8)
                    case "Miles" =>
                        randlastNames = lastNames(nextInt(4)+12)
                    case "Cameron" =>
                        randlastNames = lastNames(nextInt(4)+16)
                    case "Kyle" =>
                        randlastNames = lastNames(nextInt(4)+18)
                    case "Brandon" =>
                        randlastNames = lastNames(nextInt(4)+22)
                    case "Summer" =>
                        randlastNames = lastNames(nextInt(4)+24)
                    case "Sunshine" =>
                        randlastNames = lastNames(nextInt(4)+4)
                    case "Autumn" =>
                        randlastNames = lastNames(nextInt(4)+8)
                    case "Sharyar" =>
                        randlastNames = lastNames(nextInt(4))
                    case "Keisha" =>
                        randlastNames = lastNames(nextInt(4)+4)
                    case "Hardik" =>
                        randlastNames = lastNames(nextInt(4)+8)
                    case "Daulton" =>
                        randlastNames = lastNames(nextInt(4)+12)
                    case "Abubacarr" =>
                        randlastNames = lastNames(nextInt(4)+16)
                    case "Giancarlos" =>
                        randlastNames = lastNames(nextInt(4)+18)
                    case "Alvin" =>
                        randlastNames = lastNames(nextInt(4)+20)
                    case "Mai" =>
                        randlastNames = lastNames(nextInt(4)+22)
                    case _ =>
                        println("default")
                    
                }
            randName = randFirstNames + " " + randlastNames
            return randName
        }

        def countryCityGen(): String = {
            val countries = List("USA", "India","UK","Canada","Japan","Korea","Brazil","Colombia")
            val cities = List("New York City", "Boston", "Los Angeles","Miami", "Mumbai", "Delhi", "Bangalore", "Hyderabad", "Birmingham", "London", "Liverpool","Manchester","Vancouver","Toronto","Ontario","British Columbia","Tokyo","Kyoto","Osaka","Yokohama","Seoul","Busan","Daegu","Gwangju", "Sao Paulo","Rio de Janeiro","Brasilia","Salvador","Bogota","Leticia","Barranquilla","Medellin")

            var randCountry = countries(nextInt(countries.length))
            var randCity = ""
            var randCountryCity = ""
            
                randCountry match {
                    case "USA" =>
                        randCity = cities(nextInt(4))
                    case "India" =>
                        randCity = cities(nextInt(4)+4)
                    case "UK" =>
                        randCity = cities(nextInt(4)+8)
                    case "Canada" =>
                        randCity = cities(nextInt(4)+12)
                    case "Japan" =>
                        randCity = cities(nextInt(4)+16)
                    case "Korea" =>
                        randCity = cities(nextInt(4)+20)
                    case "Brazil" =>
                        randCity = cities(nextInt(4)+24)
                    case "Colombia" =>
                        randCity = cities(nextInt(4)+28)
                    case _ =>
                        println("default")
                }
            randCountryCity = randCountry + "," + randCity
            return randCountryCity
            
        }


        def productNameCategoryGen(): String = {
            val categories = List("Stationery", "Electronics","Books","Clothing","Music")
            val names = List("Pen","Pencil","Notepad","Markers","iPad","CellPhone", "TV","laptop","Harry Potter","A Game of Thrones", "The Da Vinci Code", "New Moon", "Yeezy", "Gucci", "Yves Saint Laurent", "Hermes", "ANTI", "YEEZUS", "Certified Lover Boy", "Happier Than Ever")

            var randProdCat = categories(nextInt(categories.length))
            var randProdName = ""
            var randProdCatName = ""
            
                randProdCat match {
                    case "Stationery" =>
                        randProdName = names(nextInt(4))
                    case "Electronics" =>
                        randProdName = names(nextInt(4)+4)
                    case "Books" =>
                        randProdName = names(nextInt(4)+8)
                    case "Clothing" =>
                        randProdName = names(nextInt(4)+12)
                    case "Music" =>
                        randProdName = names(nextInt(4)+16)
                    case _ =>
                        println("default")
                }
            randProdCatName = randProdCat + "," + randProdName
            return randProdCatName
        }
        
        
        def order_id(): BigInt = {
        // val res2 = randomUUID().toString
            randOrderId += 1

            return randOrderId
        }
        
        def customer_id(): String = {
            val randCustomerId = randomUUID().toString

            return randCustomerId
        }

        /*while(true){
            // order id
            println(order_id + ", " + customer_id)
            // customer id
            // println(customer_id)
            Thread.sleep(2000)
        }
        */
        
            //println(emailg)
        
        def emailg(): String = {
            var emails = List("www.jacobblack.com", "www.jekh@gmail.com", "www.weloveapples.com", "www.amazon.com", "www.amazon.com",
            "www.happyy.com", "www.eatvegtables.com", "www.thetable.com", "www.isbroken.com" , "wwww.crystals.com", "www.nomatter.com", "www.intersteller.com", "www.whynot.com", "www.blah.com","www.hesjks.com","www.goodbye.com","www.welcome.comrun")

            var ran = new scala.util.Random

            var randEmail = emails(ran.nextInt(emails.size))
            return randEmail;
        }  

        
        def payment_txn_id(): BigInt ={
            rdmTxn += 1

            return rdmTxn
            //var i = 0
            //var randomID=nextInt(9999)
            /*while(true){ 
                
                randomID=nextInt(9999)
                if(randomID < 1000){
                    randomID+=1000 
                    println("added 0ne thousand")  
                }
            //println(s"^\\d{4}" + randomID)
            print(randomID +",")
            Thread.sleep(2000) // wait for 2000 millisecond
            }  
            */

            //return randomID
        }

        def randomProductID(): BigInt = {
            rdmPID += 1
            return rdmPID
            //var scanner = new Scanner(System.in)

            //val r = new scala.util.Random
            //var productID = nextInt(100000)
            
            /*while (true){           
                productID = r.nextInt(1000000)
                
                
                //println("Product ID:" + productID)  
                    //for(i<-0 to 100 by 1){
                    //  println("ProductID:" + r.nextInt(1000000000))
                    // return productID
            }
            */
            //return productID
        }
            
        def random(from1: LocalDate, to1: LocalDate): LocalDate= {
                val diff = DAYS.between(from1, to1)
            
                // val di = diff.split("T")
                val random = new Random(System.nanoTime) // You may want a different seed
                
                from1.plusDays(random.nextInt(diff.toInt))
        }
        
        def random1(from: LocalDateTime, to: LocalDateTime): LocalDateTime = {
                val diff = DAYS.between(from, to)
                //println(diff)
                // val di = diff.split("T")
                val random = new Random(System.nanoTime) // You may want a different seed
                
                from.plusMinutes(random.nextInt(diff.toInt))
        }

        def randomQty(): Int = {
            val rqty = rdm
            var qty = rqty.nextInt(50)

            return qty
        }

        def randomPrice(): Int = {
            val rprc = rdm
            var price = rprc.nextInt(10000)

            return price
        }

        
        def randomTime(): String = {
            val from = LocalDateTime.of(2000, 1, 1,12,45,34)

            val to = LocalDateTime.of(2015, 1, 1,12,55,55)

            val from1 = LocalDate.of(2000,1,1)
            val to1 = LocalDate.of(2022,2,1)
                
            // connect to the database named "mysql" on port 8889 of localhost
            aa +=1
            
            var f = random1(from, to)
            var g = f.toString
            var fi = g.split("T")
            var Date = random(from1, to1)+" "+ fi(1)
            //println(Date)

            //var exp = randomProductID()
            //var hh = new ja(qty,price,Date.toString)

            //var idGen = Array(Date.toString)
            return Date
            

        }
        
        var allData = nameGen() + "," + countryCityGen() + "," + productNameCategoryGen() + "," + order_id() + "," + emailg() + "," + payment_txn_id()+ "," + randomProductID() + "," + randomQty() + "," + randomPrice() + "," + randomTime()
                
        println(allData)
        
<<<<<<< HEAD
        def order_id: BigInt = {
        // val res2 = randomUUID().toString
            res2 += 1
            return res2
        }

        def customer_id: String = {
            val res2 = randomUUID().toString
            return res2
        }


        while(true){
// order id
        println(order_id + ", " + customer_id)

// customer id
// println(customer_id)
        Thread.sleep(2000)
}


        

=======
        
                // println(randomQty() + " qty " + randomPrice() + " price " + randomTime() + " time ")
>>>>>>> testing

        
    }

    
}