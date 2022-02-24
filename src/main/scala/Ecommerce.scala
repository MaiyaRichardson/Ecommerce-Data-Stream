import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkConf
import scala.util.Random.nextInt
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.P
import java.math.BigInteger
import scala.util.matching.Regex


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
        
        randomGenerator()
    }

    def randomGenerator(): Unit = {
<<<<<<< HEAD

        val spark = SparkSession
            .builder
            .appName("KafkaSparkIntegration")
            .master("local")
            .getOrCreate()

        spark.sparkContext.setLogLevel("WARN")

        import spark.implicits._
        
            val firstNames = List("Steve", "Tony", "Peter", "Miles", "Cameron", "Kyle", "Brandon", "Summer", "Sunshine", "Autumn", "Sharyar", "Keisha", "Hardik", "Daulton", "Abubacarr", "Hardik", "Giancarlos", "Alvin", "Mai")
            val nouns = List("Parker", "Stark", "Rodgers", "moon", "rain","wind", "sea", "morning", "snow", "lake", "sunset", "pine", "shadow", "leaf","sequoia", "cedar", "wrath", "blessing", "spirit", "nova", "storm", "burst","giant", "elemental", "throne", "game", "weed", "stone", "apogee", "bang")
    
        
        def getRandElt[A](xs: List[A]): A = xs.apply(nextInt(xs.size))
    
        def getRandNumber(ra: Range): String = {
            (ra.head + nextInt(ra.end - ra.head)).toString
        }
            
        def haiku: String = {
            val xs = getRandNumber(1000 to 9999) :: List(nouns, firstNames).map(getRandElt)
            xs.reverse.mkString(",")
=======
>>>>>>> fec1b51212415922f9a7dc20ca7de2916ef10e4a

        nameGenerator()
        countryCityGenerator()
        productNameCategoryGenerator()




        def nameGenerator(): Unit = {
            val firstNames = List("Steve", "Tony", "Peter", "Miles", "Cameron", "Kyle", "Brandon", "Summer", "Sunshine", "Autumn", "Sharyar", "Keisha", "Hardik", "Daulton", "Abubacarr", "Hardik", "Giancarlos", "Alvin", "Mai")
            val lastNames = List("Parker", "Stark", "Rodgers", "moon", "rain","wind", "sea", "morning", "snow", "lake", "sunset", "pine", "shadow", "leaf","sequoia", "cedar", "wrath", "blessing", "spirit", "nova", "storm", "burst","giant", "elemental", "throne", "game", "weed", "stone", "apogee", "bang")
            
            def getRandElt[A](xs: List[A]): A = xs.apply(nextInt(xs.size))
    
            def getRandNumber(ra: Range): String = {
                (ra.head + nextInt(ra.end - ra.head)).toString
                }
            
            def haiku: String = {
                val xs = getRandNumber(1000 to 9999) :: List(lastNames, firstNames).map(getRandElt)
                xs.reverse.mkString(",")
                }
            // println(haiku)
        
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
            println(randCountry)
            println(randCity)
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
            println(randProdCat)
            println(randProdName)
        }
<<<<<<< HEAD
        println(haiku)
        println()
        def RandNum(): Unit = {
            var res = BigInt("0")
            val a = BigInt("26525285981219105863630848482795")
            //val x = (1 to a).map { e => (e + 1) }
            //val x = List.tabulate(9999)(_ + 1)
            
            //println(x)

        }
        
        def from(start: Int): Stream[Int] = Stream.cons(start, from(start + 1))
            val nn = from(0) 
            println(nn.take(99).mkString(","))
            Thread.sleep(1000) // wait for 100 millisecond
        
        
=======
>>>>>>> fec1b51212415922f9a7dc20ca7de2916ef10e4a
        


        
    }

     

    
    

    
}