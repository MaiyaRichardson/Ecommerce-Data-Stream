
import java.util.Scanner;
import java.time.LocalDate
import java.time.temporal.ChronoUnit.{DAYS,MINUTES}
import java.util.Random
import java.time.LocalDateTime

import scala.io._
import java.io.File
object Hello_World {
    var sc = new Scanner(System.in)
    var aa=0
    def main(args: Array[String]):Unit= { 
 
val from = LocalDateTime.of(2000, 1, 1,12,45,34)

 val to = LocalDateTime.of(2015, 1, 1,12,55,55)

 val from1 = LocalDate.of(2000,1,1)
 val to1 = LocalDate.of(2022,2,1)
                
        // connect to the database named "mysql" on port 8889 of localhost
      aa +=1
        val r = new scala.util.Random
        println()
  
        
        println()
        var f = random1(from, to)
            var g=f.toString
           var fi = g.split("T")
          
    var qty= r.nextInt(50)
       var price=r.nextInt(10000)
        var Date =random(from1, to1)+"  "+ fi(1)
        //println(Date)


var hh =new ja(qty,price,Date.toString)

 

    }   

    def random(from1: LocalDate, to1: LocalDate): LocalDate= {
    val diff =DAYS.between(from1, to1)
  
      // val di = diff.split("T")
    val random = new Random(System.nanoTime) // You may want a different seed
    
    from1.plusDays(random.nextInt(diff.toInt))
}


def random1(from: LocalDateTime, to: LocalDateTime): LocalDateTime = {
    val diff =DAYS.between(from, to)
   // println(diff)
      // val di = diff.split("T")
    val random = new Random(System.nanoTime) // You may want a different seed
    
    from.plusMinutes(random.nextInt(diff.toInt))
}











}