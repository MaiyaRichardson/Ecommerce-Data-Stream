import java.io.{BufferedWriter, FileWriter}
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.util.Random
import au.com.bytecode.opencsv.CSVWriter
import au.com.bytecode.opencsv.CSVReader
import java.io.FileReader

class ja(qty:Int,price:Int,Date:String) {
    var fw = new FileWriter("ecommerceData1.csv",true);
    val outputFile = new BufferedWriter(fw) //replace the path with the desired path and filename with the desired filename

    var rw = new FileReader("ecommerceData1.csv");

    val csvWriter = new CSVWriter(fw)
    val reader = new CSVReader(rw)
    var csvLines = reader.readAll()
    var fd = csvLines.get(csvLines.size-1)
    println(fd(0))
    var id = (fd(0).toInt+1)
    //val csvFields = Array("id", "qty", "prices", "DateTime")
    val csvFields1 = List(id, qty, price)
    // val nameList = List(“Deepak”, “Sangeeta”, “Geetika”, “Anubhav”, “Sahil”, “Akshay”)
    // val ageList = (24 to 26).toList
    // val cityList = List(“Delhi”, “Kolkata”, “Chennai”, “Mumbai”)
    // val random = new Random()
    var listOfRecords = new ListBuffer[Array[String]]()
    var listOfRecords1 = new ListBuffer[Array[String]]()
    //listOfRecords += csvFields
    //listOfRecords1 += csvFields1

    //csvWriter.writeAll(listOfRecords.toList)
    csvWriter.writeAll(List(Array(id.toString, qty.toString, price.toString,Date)))
    outputFile.close()
}
