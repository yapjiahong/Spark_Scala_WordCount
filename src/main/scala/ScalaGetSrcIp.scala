//mport scala.io.Source
import java.io.InputStream

import org.apache.spark._
import org.apache.spark.SparkContext

import org.apache.spark.SparkConf

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

object GetSrcIp
{
    def main(args: Array[String])
    {
      //println(args.mkString(" "))
      println("args lenth: "+args.size)
      if((args.length-1)!= 1)
      {
	println("<input> <output> error")
      }
      println("Test args.foreach for args.")
      args.foreach(arg => println(arg))   
      val inputFile = args(0)
      println("input path: "+args(0))
      println("output path: "+args(1))
      val outputFile = args(1)
      val conf = new SparkConf().setAppName("GetScrIp")
      // Create a Scala Spark Context.
      val sc = new SparkContext(conf)
      // Load our input data.
      val input =  sc.textFile(inputFile)
      // Split up into words.
      //val words = input.flatMap(line => line.split(" "))
      // Transform into word and count.
      
      val srcIp = input.map(line => line.split("\t")(2).split(">")(0).split(":")(0))
     
      val count = srcIp.distinct()

      //val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}
      // Save the word count back out to a text file, causing evaluation.
      count.saveAsTextFile(outputFile)
    }
}
