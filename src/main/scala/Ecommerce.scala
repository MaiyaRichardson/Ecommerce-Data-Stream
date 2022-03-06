import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkConf
import scala.util.Random.nextInt
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.P
import java.math.BigInteger
import scala.util.matching.Regex
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit.{DAYS,MINUTES}
import java.util.Random
import java.time.LocalDate
import scala.io._
import java.io.File
import java.util.UUID.randomUUID
import java.util.Properties
import org.apache.kafka.clients.producer._



object EcommerceProj {

    def empt1(): String = {
            var emp1 = ",,,,,,,,,,,,,,,"

            return emp1
    }

    def countMet(): Any = {
            var randCount = 1    
            while(true){
                    // order_id()
                if (randCount % 20 == 0) {
                    println(empt1() + randomGenerator())
                    
                }
                else {
                    print(randomGenerator())
                }        
                Thread.sleep(500)
                randCount += 1
            }
        }
    def main(args:Array[String]): Unit = {
        writeToKafka("Quick-start")
        val spark = SparkSession
            .builder
            .appName("KafkaSparkIntegration")
            .master("local")
            .getOrCreate()

        spark.sparkContext.setLogLevel("WARN")

        import spark.implicits._
        /*val rdd = spark.sparkContext.parallelize(Array(("USA", "Boston"), ("India", "Mumbai"), ("trisha", 28), ("bob", 52), ("fred", 23)))
        val df = rdd.toDF("Customer_Name", "age")

        df.selectExpr("CAST(Customer_Name AS String) AS key", "CAST(Customer_ID AS String) AS value")
            .write
            .format("kafka")
            .option("topic", "p3ecom")
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
        // var randOrderId = 0
        // while(true){
        // println(randomGenerator())
        // order +=1
        // }
        // println(randTxnSF)
        //val csvFields1 = List(id, qty, price)        
        
        
    }


    def writeToKafka(topic: String): Unit = {
                
        val props = new Properties()
        props.put("bootstrap.servers", "localhost:9094")
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        val producer = new KafkaProducer[Any, String](props)
        val record = new ProducerRecord[Any, String](topic, countMet(), randomGenerator())
                
                
                /*var randCount = 1    
                    while(true){
                            // order_id()
                        if (randCount % 20 == 0) {
                            producer.send(record)
                        }
                        else {
                            producer.send(record)
                        }        
                        Thread.sleep(500)
                        randCount += 1
                        }
                
                */
                
            
        producer.send(record)
        producer.close()
    }

        println(empt1)

    

    def randomGenerator(): String = {
       

        var aa=0
        var randOrderId = 0
        // randomTime()
        // nameGenerator()
        // countryCityGenerator()
        // productNameCategoryGenerator()
        // order_id()
        // customer_id()
        // emailg()
        // payment_txn_id()
        // randomProductID()
        // while (true){
        //     println(paymentType())
        //     Thread.sleep(2000)
        // }
        // println(nameGen() + "," + countryCityGen() + "," + productNameCategoryGen() + "," + order_id() + "," + emailg() + "," + payment_txn_id()+ "," + randomProductID() )
        //  + "," + randCountryCity + "," + randProdCatName + "," + randOrderId + "," + randCustomerId + "," + randEmail + "," + randTxnId + "," + randProdId)

        while(true){
            var prnt = print()
            nameGenerator()
            
            Thread.sleep(2000) // wait for 2 seconds
        }
        //nameGenerator()
        //countryCityGenerator()
        //productNameCategoryGenerator()


        def nameGenerator(): Unit = {
            val firstNames = List("Steve", "Tony", "Peter", "Miles", "Cameron", "Kyle", "Brandon", "Summer", "Sunshine", "Autumn", "Sharyar", "Keisha", "Hardik", "Daulton", "Abubacarr", "Hardik", "Giancarlos", "Alvin", "Mai")
            val lastNames = List("Parker", "Stark", "Rodgers", "moon", "rain","wind", "sea", "morning", "snow", "lake", "sunset", "pine", "shadow", "leaf","sequoia", "cedar", "wrath", "blessing", "spirit", "nova", "storm", "burst","giant", "elemental", "throne", "game", "weed", "stone", "apogee", "bang")
            
            var randFirstNames = firstNames(nextInt(firstNames.length))
            var randlastNames = ""
            
            
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
                    case "Aububacar" =>
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
            println(randFirstNames + "," + randlastNames)
            
        }

        def countryCityGenerator(): Unit = {
            val countries = List("USA", "India","UK","Canada","Japan","Korea","Brazil","Colombia")
            val cities = List("New York City", "Boston", "Los Angeles","Miami", "Mumbai", "Delhi", "Bangalore", "Hyderabad", "Birmingham", "London", "Liverpool","Manchester","Vancouver","Toronto","Ontario","British Columbia","Tokyo","Kyoto","Osaka","Yokohama","Seoul","Busan","Daegu","Gwangju", "Sao Paulo","Rio de Janeiro","Brasilia","Salvador","Bogota","Leticia","Barranquilla","Medellin")

            var randCountry = countries(nextInt(countries.length))
            var randCity = ""
            
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
            println(randCountry + "," + randCity)
            
        }


        def productNameCategoryGenerator(): Unit = {
            val categories = List("Stationery", "Electronics","Books","Clothing","Music")
            val names = List("Pen","Pencil","Notepad","Markers","iPad","CellPhone", "TV","laptop","Harry Potter","A Game of Thrones", "The Da Vinci Code", "New Moon", "Yeezy", "Gucci", "Yves Saint Laurent", "Hermes", "ANTI", "YEEZUS", "Certified Lover Boy", "Happier Than Ever")

            var randProdCat = categories(nextInt(categories.length))
            var randProdName = ""
            
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
            println(randProdCat + "," + randProdName)
           
        }
        
        println()
        def RandNum(): Unit = {
            var res = BigInt("0")
            val a = BigInt("26525285981219105863630848482795")
            //val x = (1 to a).map { e => (e + 1) }
            //val x = List.tabulate(9999)(_ + 1)
            
            //println(x)

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
        def from(start: Int): Stream[Int] = Stream.cons(start, from(start + 1))
            val nn = from(0) 
            //println(nn.take(99).mkString(","))
            Thread.sleep(2000) // wait for 2 seconds
        
        def order_id: BigInt = {
        // val res2 = randomUUID().toString
            randOrderId += 1

            return randOrderId
        }
        
        def customer_id(): String = {
            val randCustomerId = randomUUID().toString
            return randCustomerId
        }
        
        def websiteName(): String = {
            var emails = List("www.jacobblack.com", "www.jekh@gmail.com", "www.weloveapples.com", "www.amazon.com", "www.amazon.com",
            "www.happyy.com", "www.eatvegtables.com", "www.thetable.com", "www.isbroken.com" , "wwww.crystals.com", "www.nomatter.com", "www.intersteller.com", "www.whynot.com", "www.blah.com","www.hesjks.com","www.goodbye.com","www.welcome.comrun")



        
        def payment_txn_id(): Int ={

            var i = 0
            var randTxnId=nextInt(9999)

            return randTxnId
        }

        def randomProductID(): Int = {
    
            val r = new scala.util.Random
            var randProdId = nextInt(100000)
            
            return randProdId
        }
            
        def randTxnSF(): String = {
            //payment txn success
            //failure reason
            // val txnSuccess = List("Y","N")
            val failReasons = List ("Invalid CVV","Insufficient Balance","Incorrect Billing Address","Invalid Name")
            var randTxn = nextInt(4)
            var txnSuccess = ""
            var txnSF = ""
            randTxn match {
                case 0 => txnSuccess = "N"
                case 1 | 2 | 3 => txnSuccess = "Y"
            }
            if (txnSuccess == "Y"){
                txnSF = txnSuccess + ","
            } else {
                txnSF = txnSuccess + "," + failReasons(nextInt(4))
            }
            return txnSF
                
        }

        def paymentType(): String = {
            var payments = List("Card", "Internet Banking", "UPI", "Wallet")

            // var ran = rdm

            var randPayment = payments(nextInt(payments.size))
            return randPayment;
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
            // val rqty = rdm
            var qty = nextInt(50)

            return qty
        }

        def randomPrice(): Int = {
            var price = nextInt(10000)

            return price
        }

        
        def randomTime(): String = {
            val from = LocalDateTime.of(2000, 1, 1,12,45,34)

            val to = LocalDateTime.of(2015, 1, 1,12,55,55)

            val from1 = LocalDate.of(2000,1,1)
            val to1 = LocalDate.of(2022,2,1)
                
            aa +=1
            
            var f = random1(from, to)
            var g = f.toString
            var fi = g.split("T")
            var Date = random(from1, to1)+" "+ fi(1)
            
            return Date
            

        }

                
            var randData = ""
            var allData = customer_id() + "," + nameGen()  + "," + randomProductID() + "," + productNameCategoryGen() + "," + paymentType() + "," + randomQty() + "," + randomPrice() + "," + randomTime() + "," + countryCityGen() + "," +  websiteName() + "," + payment_txn_id() + "," + randTxnSF


            // println(allData)
            return allData


                
                // println(allData)
//currently have 13
//payment type



        

        

        
    }

     

    
    

    
}
}